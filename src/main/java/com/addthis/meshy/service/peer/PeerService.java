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

import com.addthis.basis.util.LessBytes;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.MeshyServer;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * connect to a peer, receive from peer it's uuid and connection map
 */
public final class PeerService {

    static final Logger log = LoggerFactory.getLogger(PeerService.class);
    private static final LinkedBlockingQueue<PeerTuple> peerQueue = new LinkedBlockingQueue<>();

    static final Thread peeringThread = new Thread() {
        {
            setName("peering finderQueue");
            setDaemon(true);
            start();
        }

        public void run() {
            while (true) {
                try {
                    peerQueue.take().connect();
                } catch (Exception e) {
                    log.warn("peer finderQueue exiting on error", e);
                    return;
                }
            }
        }
    };

    private PeerService() {
    }

    private static boolean shouldEncode(InetSocketAddress sockAddr) {
        InetAddress addr = sockAddr.getAddress();
        return !(addr.isLoopbackAddress() || addr.isAnyLocalAddress());
    }

    public static void encodeAddress(InetSocketAddress addr, OutputStream out) throws IOException {
        LessBytes.writeBytes(addr.getAddress().getAddress(), out);
        LessBytes.writeInt(addr.getPort(), out);
    }

    public static InetSocketAddress decodeAddress(InputStream in) throws IOException {
        return new InetSocketAddress(InetAddress.getByAddress(LessBytes.readBytes(in)), LessBytes.readInt(in));
    }

    /**
     * send local peer uuid:port and list of peers to remote
     */
    public static byte[] encodeExtraPeers(MeshyServer master) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (ChannelState channelState : master.getChannels(MeshyConstants.LINK_NAMED)) {
                LessBytes.writeString(channelState.getName(), out);
                encodeAddress(channelState.getRemoteAddress(), out);
                log.debug("{} encoded {} @ {}", master, channelState.getName(), channelState.getChannelRemoteAddress());
            }
            for (MeshyServer member : master.getMembers()) {
                if (member != master && shouldEncode(member.getLocalAddress())) {
                    log.trace("encode MEMBER: {} / {}", member.getUUID(), member.getLocalAddress());
                    LessBytes.writeString(member.getUUID(), out);
                    encodeAddress(member.getLocalAddress(), out);
                }
            }
            LessBytes.writeString("", out);
            return out.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static byte[] encodeSelf(MeshyServer master) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            LessBytes.writeString(master.getUUID(), out);
            encodeAddress(master.getLocalAddress(), out);
            return out.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * receive peer's uuid:port and peer list then connect to ones that are new to us
     */
    public static boolean decodePrimaryPeer(MeshyServer master, ChannelState peerState, InputStream in) {
        try {
            String newName = LessBytes.readString(in);
            if (Strings.isNullOrEmpty(newName)) {
                log.debug("would-be peer is refusing peerage: sent {} from {} for {}", newName, peerState, master);
                return false;
            }
            boolean shouldBeConnector = master.shouldBeConnector(newName);
            boolean isConnector = peerState.getChannel().parent() == null;
            boolean promoteToPeer = shouldBeConnector == isConnector;

            InetSocketAddress newAddr = decodeAddress(in);
            InetAddress newInetAddr = newAddr.getAddress();
            /* if remote reports loopback or any-net, use peer ip addr + port */
            if (newInetAddr.isAnyLocalAddress() || newInetAddr.isLoopbackAddress()) {
                newAddr = new InetSocketAddress(peerState.getChannelRemoteAddress().getAddress(), newAddr.getPort());
                newInetAddr = newAddr.getAddress();
            }
            if ((newInetAddr.isAnyLocalAddress() || newInetAddr.isLoopbackAddress()) && isConnector) {
                newAddr = new InetSocketAddress(peerState.getChannel().localAddress().getAddress(), newAddr.getPort());
            }

            if (promoteToPeer) {
                return master.promoteToNamedServerPeer(peerState, newName, newAddr);
            } else if (shouldBeConnector) {
                log.info("dropping (and reconnecting as client) backwards connection from: {} @ {} for: {}",
                        newName, newAddr, master);
                master.connectToPeer(newName, newAddr);
            }
            return promoteToPeer;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * receive peer's uuid:port and peer list then connect to ones that are new to us
     */
    public static void decodeExtraPeers(MeshyServer master, InputStream in) {
        try {
            while (true) {
                String peerUuid = LessBytes.readString(in);
                if (peerUuid.isEmpty()) {
                    break;
                }
                InetSocketAddress peerAddress = decodeAddress(in);
                log.debug("{} decoded {} @ {}", master, peerUuid, peerAddress);
                peerQueue.put(new PeerTuple(master, peerUuid, peerAddress));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
