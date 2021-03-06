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
package com.addthis.meshy.service.peer;

import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.SourceHandler;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import static com.addthis.meshy.service.peer.PeerService.decodeExtraPeers;
import static com.addthis.meshy.service.peer.PeerService.decodePrimaryPeer;

public class PeerSource extends SourceHandler {
    private static final Logger log = PeerService.log;

    private boolean receivedStateUuid = false;

    public PeerSource(MeshyServer master, String tempUuid) {
        super(master, PeerTarget.class, tempUuid);
        send(PeerService.encodeSelf(master));
    }

    @Override
    public void channelClosed(ChannelState state) {
    }

    @Override
    public void receive(ChannelState state, int length, ByteBuf buffer) throws Exception {
        log.debug("{} decode from {}", this, state);
        if (!receivedStateUuid) {
            if (decodePrimaryPeer((MeshyServer) getChannelMaster(), state, Meshy.getInput(length, buffer))) {
                send(PeerService.encodeExtraPeers((MeshyServer) getChannelMaster()));
                sendComplete();
            } else {
                sendComplete();
                state.getChannel().close();
            }
            receivedStateUuid = true;
        } else {
            decodeExtraPeers((MeshyServer) getChannelMaster(), Meshy.getInput(length, buffer));
        }
    }

    @Override
    public void receiveComplete() throws Exception {
    }
}
