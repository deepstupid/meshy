/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.meshy;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.addthis.meshy.ChannelState.MESHY_BYTE_OVERHEAD;
import static com.google.common.base.Preconditions.checkArgument;


public abstract class SourceHandler implements SessionHandler {

    private static final int DEFAULT_COMPLETE_TIMEOUT = Parameter.intValue("meshy.complete.timeout", 120);
    private static final int DEFAULT_RESPONSE_TIMEOUT = Parameter.intValue("meshy.source.timeout", 0);
    private static final boolean SLOW_SLOW_CHANNELS = Parameter.boolValue("meshy.source.closeSlow", false);
    private static final boolean DISABLE_CREATION_FRAMES = Parameter.boolValue("meshy.source.noCreationFrames", false);
    // only used by response watcher
    private static final Set<SourceHandler> activeSources = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Logger log = LoggerFactory.getLogger(SourceHandler.class);
    // TODO: use scheduled thread pool
    static final Thread responseWatcher = new Thread("Source Response Watcher") {
        {
            setDaemon(true);
            start();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    sleep(500);
                } catch (Exception ignored) {
                    break;
                }
                probeActiveSources();
            }
            log.info("{} exiting", this);
        }

        private void probeActiveSources() {
            activeSources.forEach(SourceHandler::handleChannelTimeouts);
        }
    };

    private final Class<? extends TargetHandler> targetClass;
    private final String className = getClass().getName();
    private final String shortName = className.substring(className.lastIndexOf('.') + 1);
    private final AtomicBoolean sent = new AtomicBoolean(false);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicBoolean waited = new AtomicBoolean(false);
    private final Semaphore gate = new Semaphore(0);
    private final ChannelMaster master;
    protected final Set<Channel> channels = Sets.newConcurrentHashSet();
    private int session;
    private int targetHandler;
    private long readTime;
    private long readTimeout;
    private long completeTimeout;

    /**
     * This constructor does not perform initialization during construction and therefore does not escape "this".
     * The boolean parameter is expected to always be true and is only included to differentiate the signature from
     * the otherwise identical constructor that does escape "this".
     */
    protected SourceHandler(ChannelMaster master,
                            Class<? extends TargetHandler> targetClass,
                            /* should always be true */ boolean skipInit) {
        checkArgument(skipInit, "skipInit must always be true");
        this.master = master;
        this.targetClass = targetClass;
    }

    protected SourceHandler(ChannelMaster master, Class<? extends TargetHandler> targetClass) {
        this(master, targetClass, MeshyConstants.LINK_ALL);
    }

    protected SourceHandler(ChannelMaster master, Class<? extends TargetHandler> targetClass, String targetUuid) {
        this(master, targetClass, true);
        start(targetUuid);
    }

    private static ByteBuf allocateSendBuffer(ByteBufAllocator alloc, int type, int session, byte[] data) {
        ByteBuf sendBuffer = alloc.buffer(MESHY_BYTE_OVERHEAD + data.length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(data.length);
        sendBuffer.writeBytes(data);
        return sendBuffer;
    }

    protected final void start() {
        start(MeshyConstants.LINK_ALL);
    }

    protected void start(String targetUuid) {
        this.readTime = JitterClock.globalTime();
        Collection<ChannelState> matches = master.getChannels(targetUuid);
        if (matches.isEmpty()) {
            throw new ChannelException("no matching mesh peers");
        }
        this.session = master.newSession();

        for (ChannelState state : matches) {
            channels.add(state.getChannel());
        }

        this.targetHandler = master.targetHandlerId(targetClass);
        setReadTimeout(DEFAULT_RESPONSE_TIMEOUT);
        setCompleteTimeout(DEFAULT_COMPLETE_TIMEOUT);
        activeSources.add(this);
        for (ChannelState state : matches) {
            /* add channel callback path to source */
            state.addSourceHandler(session, this);
            if (!state.getChannel().isOpen()) {
                channels.remove(state.getChannel()); // may or may not be needed
            }
        }
        log.debug("{} start target={} uuid={} group={} sessionID={}",
                this, targetHandler, targetUuid, channels, session);
    }

    @Override
    public String toString() {
        return master + "[Source:" + shortName + ":s=" + session + ",h=" + targetHandler +
                ",c=" + (channels != null ? channels.size() : "null") + ']';
    }

    private void handleChannelTimeouts() {
        if ((readTimeout > 0) && ((JitterClock.globalTime() - readTime) > readTimeout)) {
            log.info("{} response timeout on channel: {}", this, channelsToList());
            if (SLOW_SLOW_CHANNELS) {
                log.warn("closing {} channel(s)", channels.size());
                 {
                     channels.forEach(Channel::close);
                }
            }
            channels.clear();
            try {
                receiveComplete(session);
            } catch (Exception ex) {
                log.error("Swallowing exception while handling channel timeout", ex);
            }
        }
    }

    protected ChannelMaster getChannelMaster() {
        return master;
    }

    private void setReadTimeout(int seconds) {
        readTimeout = (long) (seconds * 1000);
    }

    private void setCompleteTimeout(int seconds) {
        completeTimeout = (long) (seconds * 1000);
    }

    public int getPeerCount() {
        return channels.size();
    }

    public String getPeerString() {
        StringBuilder sb = new StringBuilder(10 * channels.size());
         {
            for (Channel channel : channels) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(channel.remoteAddress());
            }
        }
        return sb.toString();
    }

    @Override
    public boolean sendComplete() {
        return send(MeshyConstants.EMPTY_BYTES, null);
    }

    public boolean send(byte[] data) {
        return send(data, null);
    }

    @Override
    public final boolean send(byte[] data, final SendWatcher watcher) {
         {
            if (channels.isEmpty()) {
                return false;
            }

            int sendType = MeshyConstants.KEY_EXISTING;
            if (sent.compareAndSet(false, true) || DISABLE_CREATION_FRAMES) {
                sendType = targetHandler;
            }

            final ByteBufAllocator alloc = channels.iterator().next().alloc();
            final ByteBuf buffer = allocateSendBuffer(alloc, sendType, session, data);
            final int reportBytes = data.length;
            final int peerCount = channels.size();

            log.trace("{} send {} to {}", this, buffer.capacity(), peerCount);
            List<ChannelFuture> futures = new ArrayList<>(peerCount);
            for (Channel c : channels) {
                futures.add(c.writeAndFlush(buffer.duplicate().retain()));
            }
            AggregateChannelFuture aggregateFuture = new AggregateChannelFuture(futures);
            aggregateFuture.addListener(ignored -> {
                master.sentBytes(reportBytes * peerCount);
                if (watcher != null) {
                    watcher.sendFinished(reportBytes);
                }
            });
            buffer.release();
            return true;
        }
    }

    protected boolean sendToSingleTarget(Channel channel, byte[] data) {
        int sendType = MeshyConstants.KEY_EXISTING;
        final ByteBufAllocator alloc = channel.alloc();
        final ByteBuf buffer = allocateSendBuffer(alloc, sendType, session, data);

        final int reportBytes = data.length;
        log.trace("{} send {} to {}", this, buffer.capacity(), channel);
        channel.writeAndFlush(buffer.duplicate().retain()).addListener(ignored -> master.sentBytes(reportBytes));
        buffer.release();
        return true;
    }

    @Override
    public void receive(ChannelState state, int receivingSession, int length, ByteBuf buffer) throws Exception {
        this.readTime = JitterClock.globalTime();
        log.debug("{} receive [{}] l={}", this, session, length);
        receive(state, length, buffer);
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        log.debug("{} receiveComplete.1 [{}]", this, completedSession);
        Channel channel = state.getChannel();
        if (channel != null) {
            channels.remove(channel);
            if (!channel.isOpen()) {
                channelClosed(state);
            }
        }
        if (channels.isEmpty()) {
            receiveComplete(completedSession);
        }
    }

    private void receiveComplete(int completedSession) throws Exception {
        log.debug("{} receiveComplete.2 [{}]", this, completedSession);
        // ensure this is only called once
        if (complete.compareAndSet(false, true)) {
            try {
                receiveComplete();
            } finally {
                activeSources.remove(this);
                if (sent.get()) {
                    gate.release();
                }
            }
        }
    }

    private String channelsToList() {
        StringBuilder stringBuilder = new StringBuilder(10 * channels.size());
         {
            for (Channel channel : channels) {
                stringBuilder.append(channel.remoteAddress().toString());
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public void waitComplete() {
        // this is technically incorrect, but prevents lockups
        if (waited.compareAndSet(false, true) && sent.get()) {
            try {
                if (!gate.tryAcquire(completeTimeout, TimeUnit.MILLISECONDS)) {
                    log.warn("{} failed to waitComplete() normally from channels: {}", this, channelsToList());
                    activeSources.remove(this);
                }
            } catch (Exception ex) {
                log.error("Swallowing mystery exception", ex);
            }
        }
    }

    protected abstract void channelClosed(ChannelState state);

    protected abstract void receive(ChannelState state, int length, ByteBuf buffer) throws Exception;

    protected abstract void receiveComplete() throws Exception;
}
