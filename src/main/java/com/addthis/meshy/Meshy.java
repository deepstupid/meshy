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
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.file.FileTarget;
import com.addthis.meshy.service.host.HostSource;
import com.addthis.meshy.service.host.HostTarget;
import com.addthis.meshy.service.message.MessageSource;
import com.addthis.meshy.service.message.MessageTarget;
import com.addthis.meshy.service.peer.PeerSource;
import com.addthis.meshy.service.peer.PeerTarget;
import com.addthis.meshy.service.stream.StreamSource;
import com.addthis.meshy.service.stream.StreamTarget;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.VirtualMachineMetrics;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * full mesh nodes are both clients and servers. so the client logic is not exclusive to client-only nodes.
 * try not to let this confuse you when reading the code. this is a partial explanation as to why there are
 * client and server codes co-mingled.
 */
public abstract class Meshy implements ChannelMaster, Closeable {
    // system property constants
    static final boolean THROTTLE_LOG = Parameter.boolValue("meshy.throttleLog", true);
    static final int STATS_INTERVAL = Parameter.intValue("meshy.stats.time", 1) * 1000;
    // default channel watermarks to 10 MB and 5 MB
    static final int HIGH_WATERMARK = Parameter.intValue("meshy.channel.highWatermark", 10 * 1024) * 1024;
    static final int LOW_WATERMARK = Parameter.intValue("meshy.channel.lowWatermark", 5 * 1024) * 1024;
    private static final Map<Integer, Class<? extends SessionHandler>> idHandlerMap = new HashMap<>();
    private static final Map<Class<? extends SessionHandler>, Integer> handlerIdMap = new HashMap<>();
    private static final AtomicInteger nextHandlerID = new AtomicInteger(1);
    static final DecimalFormat numbers = new DecimalFormat("#,###");
    static final VirtualMachineMetrics vmMetrics = VirtualMachineMetrics.getInstance();
    static final AtomicInteger nextSession = new AtomicInteger(0);
    private static final Logger log = LoggerFactory.getLogger(Meshy.class);
    // metrics and logging
    private static final Meter bytesInMeter = Metrics.newMeter(Meshy.class, "bytesIn", "bytesIn", TimeUnit.SECONDS);
    private static final Meter bytesOutMeter = Metrics.newMeter(Meshy.class, "bytesOut", "bytesOut", TimeUnit.SECONDS);

    private static final String HOSTNAME = getShortHostName();

    // static init block
    static {
        registerHandlerClass(HostSource.class);
        registerHandlerClass(HostTarget.class);
        registerHandlerClass(FileSource.class);
        registerHandlerClass(FileTarget.class);
        registerHandlerClass(PeerSource.class);
        registerHandlerClass(PeerTarget.class);
        registerHandlerClass(StreamSource.class);
        registerHandlerClass(StreamTarget.class);
        registerHandlerClass(MessageSource.class);
        registerHandlerClass(MessageTarget.class);
        /* create enum for "smart" auto-mesh */
        try {
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    final Set<ChannelState> connectedChannels = Sets.newConcurrentHashSet();
    /* nodes actively being peered */
    final Set<String> inPeering = new HashSet<>();
    final EventLoopGroup workerGroup;
    private final Collection<ChannelCloseListener> channelCloseListeners = new ArrayList<>();
    private final Bootstrap clientBootstrap;
    private final String uuid;

    private final AtomicLong lastEvent = new AtomicLong(0);
    private final AtomicInteger bytesIn = new AtomicInteger(0);
    private final AtomicInteger bytesOut = new AtomicInteger(0);

    Meshy() {
        if (HOSTNAME != null) {
            uuid = HOSTNAME + '-' + Long.toHexString(System.currentTimeMillis() & 0xffffff);
        } else {
            uuid = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
        }
        workerGroup = new NioEventLoopGroup();
        clientBootstrap = new Bootstrap()
                .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATERMARK)
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATERMARK)
                .channel(NioSocketChannel.class)
                .group(workerGroup)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(final NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ChannelState(Meshy.this, ch));
                    }
                });
        updateLastEventTime();
    }

    private static void registerHandlerClass(Class<? extends SessionHandler> clazz) {
        if (!handlerIdMap.containsKey(clazz)) {
            int id = nextHandlerID.getAndIncrement();
            idHandlerMap.put(id, clazz);
            handlerIdMap.put(clazz, id);
        }
    }

    public static InputStream getInput(int length, ByteBuf buffer) {
        return new ByteArrayInputStream(getBytes(length, buffer));
    }

    /**
     * utility
     */
    public static byte[] getBytes(int length, ByteBuf buffer) {
        byte[] request = new byte[length];
        buffer.readBytes(request);
        return request;
    }

    @Nullable
    private static String getShortHostName() {
        try {
            String hostName = Splitter.on('.')
                    .split(InetAddress.getLocalHost().getHostName())
                    .iterator().next();
            log.debug("Local host name resolved to {}", hostName);
            return hostName;
        } catch (Exception ex) {
            log.warn("Unable to resolve local host name");
            log.debug("Local host name resolution stack trace", ex);
            return null;
        }
    }

    void updateLastEventTime() {
        lastEvent.set(JitterClock.globalTime());
    }

    @Override
    public void close() {
        closeAsync().syncUninterruptibly();
    }

    public Future<?> closeAsync() {
         {
            for (ChannelState state : connectedChannels) {
                state.debugSessions();
            }
        }
        return workerGroup.shutdownGracefully();
    }

    ChannelFuture connect(InetSocketAddress addr) {
        return clientBootstrap.connect(addr);
    }

    public int getChannelCount() {
         {
            return connectedChannels.size();
        }
    }

    int getAndClearSent() {
        return bytesOut.getAndSet(0);
    }

    int getAndClearRecv() {
        return bytesIn.getAndSet(0);
    }

    @Override
    public String getUUID() {
        return uuid;
    }

    @Override
    public TargetHandler createHandler(int type) {
        try {
            return (TargetHandler) idHandlerMap.get(type).getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int newSession() {
        return nextSession.incrementAndGet();
    }

    @Override
    public int targetHandlerId(Class<? extends TargetHandler> targetHandler) {
        return handlerIdMap.get(targetHandler);
    }

    /**
     * @param nameFilter null = all channels, empty = named channels, non-empty = exact match
     */
    @Override
    public Collection<ChannelState> getChannels(final String nameFilter) {
        Collection<ChannelState> group;
        HashSet<String> uuids = new HashSet<>();
        final boolean breakOnMatch = (nameFilter != MeshyConstants.LINK_ALL) &&
                (nameFilter != MeshyConstants.LINK_NAMED);
         {
            group = new ArrayList<>(breakOnMatch ? 1 : connectedChannels.size());
            for (ChannelState state : connectedChannels) {
                if ((nameFilter == MeshyConstants.LINK_ALL) ||
                        ((nameFilter == MeshyConstants.LINK_NAMED) && (state.getRemoteAddress() != null)) ||
                        ((state.getName() != null) && nameFilter.equals(state.getName()))) {
                    /* prevent dups if >1 connection to the same host */
                    if ((state.getName() != null) && !uuids.add(state.getName())) {
                        continue;
                    }
                    group.add(state);
                    if (breakOnMatch) {
                        break;
                    }
                }
            }
        }
        return group;
    }

    @Override
    public void sentBytes(int size) {
        bytesOutMeter.mark(size);
        bytesOut.addAndGet(size);
    }

    @Override
    public void recvBytes(int size) {
        bytesInMeter.mark(size);
        bytesIn.addAndGet(size);
    }

    @Override
    public long lastEventTime() {
        return lastEvent.get();
    }

    void channelConnected(Channel channel, ChannelState channelState) {
         {
            connectedChannels.add(channelState);
        }
        log.debug("{} channelConnected @ {}", this, channel.remoteAddress());
    }

    void channelClosed(Channel channel, ChannelState channelState) {
         {
            connectedChannels.remove(channelState);
            inPeering.remove(channelState.getName());
        }
        synchronized (channelCloseListeners) {
            for (ChannelCloseListener channelCloseListener : channelCloseListeners) {
                channelCloseListener.channelClosed(channel);
            }
        }
        log.debug("{} channelClosed @ {}", this, channel.remoteAddress());
    }

    public boolean addChannelCloseListener(ChannelCloseListener channelCloseListener) {
        synchronized (this.channelCloseListeners) {
            return this.channelCloseListeners.add(channelCloseListener);
        }
    }

    public boolean removeChannelCloseListener(ChannelCloseListener channelCloseListener) {
        synchronized (this.channelCloseListeners) {
            return this.channelCloseListeners.remove(channelCloseListener);
        }
    }
}
