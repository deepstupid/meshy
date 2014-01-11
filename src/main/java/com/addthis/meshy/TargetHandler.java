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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;


public abstract class TargetHandler implements SessionHandler {

    protected static final Logger log = LoggerFactory.getLogger(TargetHandler.class);
    private final String className = getClass().getName();
    private final String shortName = className.substring(className.lastIndexOf(".") + 1);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicBoolean waited = new AtomicBoolean(false);
    private final Semaphore gate = new Semaphore(1);

    private MeshyServer master;
    private ChannelState channelState;
    private int session;

    // non-static initializer
    {
        try {
            gate.acquire();
        } catch (Exception ex) {
            log.error("Swallowing exception during non-static initialization of TargetHandler", ex);
        }
    }

    public void setContext(MeshyServer master, ChannelState state, int session) {
        this.master = master;
        this.channelState = state;
        this.session = session;
    }

    protected Objects.ToStringHelper toStringHelper() {
        return Objects.toStringHelper(this)
                .add("shortName", shortName)
                .add("channelState", channelState.getName())
                .add("session", session)
                .add("complete", complete)
                .add("waited", waited)
                .add("gate-permits", gate.availablePermits());
    }

    @Override
    public String toString() {
        return toStringHelper().toString();
    }

    public ChannelState getChannelState() {
        return channelState;
    }

    public MeshyServer getChannelMaster() {
        return master;
    }

    public int getSessionId() {
        return session;
    }

    public void send(ByteBuf from, int length) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send.buf [" + length + "] " + from);
        }
        channelState.send(ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, from, length), null, length);
    }

    public boolean send(byte data[]) {
        return send(data, null);
    }

    @Override
    public boolean send(byte data[], SendWatcher watcher) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send " + data.length);
        }
        return channelState.send(ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, data), watcher, data.length);
    }

    public void send(byte data[], int off, int len, SendWatcher watcher) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send " + data.length + " o=" + off + " l=" + len);
        }
        channelState.send(ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, data, off, len), watcher, len);
    }

    public ByteBuf getSendBuffer(int length) {
        return ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, length);
    }

    public int send(ByteBuf buffer, SendWatcher watcher) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send b=" + buffer + " l=" + buffer.readableBytes());
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
        assert this.session == receivingSession;
        if (log.isDebugEnabled()) {
            log.debug(this + " receive [" + receivingSession + "] l=" + length);
        }
        receive(length, buffer);
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        assert this.channelState == state;
        assert this.session == completedSession;
        if (log.isDebugEnabled()) {
            log.debug(this + " receiveComplete.1 [" + completedSession + "]");
        }
        if (!state.getChannel().isOpen()) {
            channelClosed();
        }
        receiveComplete(completedSession);
    }

    @Override
    public void receiveComplete(int completedSession) throws Exception {
        assert this.session == completedSession;
        if (log.isDebugEnabled()) {
            log.debug(this + " receiveComplete.2 [" + completedSession + "]");
        }
        // ensure this is only called once
        if (complete.compareAndSet(false, true)) {
            receiveComplete();
            gate.release();
        }
    }

    public void channelClosed() {
        // override in subclasses
    }

    @Override
    public void waitComplete() {
        // this is technically incorrect, but prevents lockups
        if (waited.compareAndSet(false, true)) {
            try {
                gate.acquire();
            } catch (Exception ex) {
                log.error("Swallowing exception while waitComplete() on targetHandler", ex);
            }
        }
    }

    public abstract void receive(int length, ByteBuf buffer) throws Exception;

    public abstract void receiveComplete() throws Exception;
}
