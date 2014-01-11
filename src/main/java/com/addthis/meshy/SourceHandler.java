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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelGroupFutureListener;
import io.netty.util.concurrent.DefaultThreadFactory;


public abstract class SourceHandler implements SessionHandler {

    private static final Logger log = LoggerFactory.getLogger(SourceHandler.class);
    static final int DEFAULT_COMPLETE_TIMEOUT = Parameter.intValue("meshy.complete.timeout", 120);
    static final int DEFAULT_RESPONSE_TIMEOUT = Parameter.intValue("meshy.source.timeout", 0);
    static final boolean closeSlowChannels = Parameter.boolValue("meshy.source.closeSlow", false);
    static final Set<SourceHandler> activeSources = Collections.newSetFromMap(new ConcurrentHashMap<SourceHandler, Boolean>());
    static final ScheduledThreadPoolExecutor responseWatcher =
            new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory("Meshy Source Response Watcher"));


    static {
        responseWatcher.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                probeActiveSources();
            }

            private void probeActiveSources() {
                for (SourceHandler handler : activeSources) {
                    handler.handleChannelTimeouts();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private final String className = getClass().getName();
    private final String shortName = className.substring(className.lastIndexOf(".") + 1);
    private final AtomicBoolean sent = new AtomicBoolean(false);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicBoolean waited = new AtomicBoolean(false);
    private final Semaphore gate = new Semaphore(1);
    private final ChannelMaster master;

    private int session;
    private int targetHandler;
    private long readTime;
    private long readTimeout;
    private long completeTimeout;
    private ChannelGroup channels;
    private ChannelState state;

    public SourceHandler(ChannelMaster master, Class<? extends TargetHandler> targetClass) {
        this(master, targetClass, MeshyConstants.LINK_ALL);
    }

    // TODO: more sane construction
    public SourceHandler(ChannelMaster master, Class<? extends TargetHandler> targetClass, String targetUuid) {
        this.master = master;
        master.createSession(this, targetClass, targetUuid);
    }

    @Override
    public String toString() {
        return master + "[Source:" + shortName + ":s=" + session + ",h=" + targetHandler + ",c=" + (channels != null ? channels.size() : "null") + "]";
    }

    private void handleChannelTimeouts() {
        if ((readTimeout > 0) && ((JitterClock.globalTime() - readTime) > readTimeout)) {
            log.info(this + " response timeout on channel: " + channelsToList());
            if (closeSlowChannels) {
                log.info("closing " + channels.size() + " channel(s)");
                synchronized (channels) {
                    for (Channel channel : channels) {
                        channel.close();
                    }
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

    public ChannelMaster getChannelMaster() {
        return master;
    }

    public ChannelState getChannelState() {
        return state;
    }

    public void init(int session, int targetHandler, ChannelGroup group) {
        this.readTime = JitterClock.globalTime();
        this.session = session;
        this.channels = group;
        this.targetHandler = targetHandler;
        setReadTimeout(DEFAULT_RESPONSE_TIMEOUT);
        setCompleteTimeout(DEFAULT_COMPLETE_TIMEOUT);
        activeSources.add(this);
    }

    public void setReadTimeout(int seconds) {
        readTimeout = (long) (seconds * 1000);
    }

    public void setCompleteTimeout(int seconds) {
        completeTimeout = (long) (seconds * 1000);
    }

    public int getPeerCount() {
        return channels.size();
    }

    /**
     * returned set must be synchronized on for iteration, and probably
     * should not modify the contents.
     *
     * @return peers
     */
    public Set<Channel> getPeers() {
        return channels;
    }

    public String getPeerString() {
        StringBuilder sb = new StringBuilder(10 * channels.size());
        synchronized (channels) {
            for (Channel channel : channels) {
                if (sb.length() > 0) {
                    sb.append(",");
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
    public boolean send(byte[] data, SendWatcher watcher) {
        return send(ChannelState.allocateSendBuffer(targetHandler, session, data), watcher, data.length);
    }

    private boolean send(final ByteBuf buffer, final SendWatcher watcher, final int reportBytes) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send " + buffer.capacity() + " to " + channels.size());
        }
        if (!channels.isEmpty()) {
            final int peerCount = channels.size();
            if (sent.compareAndSet(false, true)) {
                try {
                    gate.acquire();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            ChannelGroupFuture future = channels.writeAndFlush(buffer);
            future.addListener(new ChannelGroupFutureListener() {
                @Override
                public void operationComplete(ChannelGroupFuture future) throws Exception {
                    master.sentBytes(reportBytes * peerCount);
                    buffer.release();
                    if (watcher != null) {
                        watcher.sendFinished(reportBytes);
                    }
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public void receive(ChannelState state, int receivingSession, int length, ByteBuf buffer) throws Exception {
        this.state = state;
        this.readTime = JitterClock.globalTime();
        if (log.isDebugEnabled()) {
            log.debug(this + " receive [" + receivingSession + "] l=" + length);
        }
        receive(length, buffer);
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        this.state = state;
        if (log.isDebugEnabled()) {
            log.debug(this + " receiveComplete.1 [" + completedSession + "]");
        }
        Channel channel = state.getChannel();
        if (channel != null) {
            channels.remove(channel);
            if (!channel.isOpen()) {
                channelClosed();
            }
        }
        if (channels.isEmpty()) {
            receiveComplete(completedSession);
        }
    }

    @Override
    public void receiveComplete(int completedSession) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug(this + " receiveComplete.2 [" + completedSession + "]");
        }
        // ensure this is only called once
        if (complete.compareAndSet(false, true)) {
            if (sent.get()) {
                gate.release();
            }
            receiveComplete();
            activeSources.remove(this);
        }
    }

    public void channelClosed() {
        // override in subclasses
    }

    private String channelsToList() {
        StringBuilder stringBuilder = new StringBuilder(10 * channels.size());
        synchronized (channels) {
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
                    log.warn(this + " failed to waitComplete() normally from channels: " + channelsToList());
                    activeSources.remove(this);
                }
            } catch (Exception ex) {
                log.error("Swallowing mystery exception", ex);
            }
        }
    }

    public abstract void receive(int length, ByteBuf buffer) throws Exception;

    public abstract void receiveComplete() throws Exception;
}
