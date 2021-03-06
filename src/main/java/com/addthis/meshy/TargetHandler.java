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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.addthis.meshy.MeshyConstants.KEY_RESPONSE;


public abstract class TargetHandler implements SessionHandler {

    protected static final Logger log = LoggerFactory.getLogger(TargetHandler.class);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicBoolean waited = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(1);

    private MeshyServer master;
    private ChannelState channelState;
    private int session;

    protected TargetHandler() {
    }

    public void setContext(MeshyServer master, ChannelState state, int session) {
        this.master = master;
        this.channelState = state;
        this.session = session;
    }

    protected Objects.ToStringHelper toStringHelper() {
        return Objects.toStringHelper(this)
                .add("channelState", channelState.getName())
                .add("session", session)
                .add("complete", complete)
                .add("waited", waited);
    }

    @Override
    public String toString() {
        return toStringHelper().toString();
    }

    protected ChannelState getChannelState() {
        return channelState;
    }

    public MeshyServer getChannelMaster() {
        return master;
    }

    public int getSessionId() {
        return session;
    }

    public boolean send(byte[] data) {
        return send(data, null);
    }

    public void send(ByteBuf from, int length) {
        log.trace("{} send.buf [{}] {}", this, length, from);
        channelState.send(channelState.allocateSendBuffer(KEY_RESPONSE, session, from, length), null, length);
    }

    @Override
    public boolean send(byte[] data, SendWatcher watcher) {
        log.trace("{} send {}", this, data.length);
        return channelState.send(channelState.allocateSendBuffer(KEY_RESPONSE, session, data),
                watcher, data.length);
    }

    public void send(byte[] data, int off, int len, SendWatcher watcher) {
        log.trace("{} send {} o={} l={}", this, data.length, off, len);
        channelState.send(channelState.allocateSendBuffer(KEY_RESPONSE, session, data, off, len),
                watcher, len);
    }

    protected ByteBuf getSendBuffer(int length) {
        return channelState.allocateSendBuffer(KEY_RESPONSE, session, length);
    }

    protected int send(ByteBuf buffer, SendWatcher watcher) {
        if (log.isTraceEnabled()) {
            log.trace("{} send b={} l={}", this, buffer, buffer.readableBytes());
        }
        int length = buffer.readableBytes();
        channelState.send(buffer, watcher, length);
        return length;
    }

    @Override
    public boolean sendComplete() {
        return send(MeshyConstants.EMPTY_BYTES, null);
    }

    @Override
    public void receive(ChannelState state, int receivingSession, int length, ByteBuf buffer) throws Exception {
        assert this.channelState == state;
        assert this.session == session;
        log.debug("{} receive [{}] l={}", this, session, length);
        receive(length, buffer);
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        assert this.channelState == state;
        assert this.session == completedSession;
        log.debug("{} receiveComplete.1 [{}]", this, completedSession);
        if (!state.getChannel().isOpen()) {
            channelClosed();
        }
        receiveComplete(completedSession);
    }

    private void receiveComplete(int completedSession) throws Exception {
        assert this.session == completedSession;
        log.debug("{} receiveComplete.2 [{}]", this, completedSession);
        // ensure this is only called once
        if (complete.compareAndSet(false, true)) {
            receiveComplete();
            latch.countDown();
        }
    }

    protected void autoReceiveComplete() {
        channelState.sessionComplete(this, MeshyConstants.KEY_EXISTING, session);
    }

    @Override
    public void waitComplete() {
        // this is technically incorrect, but prevents lockups
        if (waited.compareAndSet(false, true)) {
            try {
                latch.await();
            } catch (Exception ex) {
                log.error("Swallowing exception while waitComplete() on targetHandler", ex);
            }
        }
    }

    protected abstract void channelClosed();

    protected abstract void receive(int length, ByteBuf buffer) throws Exception;

    protected abstract void receiveComplete() throws Exception;
}
