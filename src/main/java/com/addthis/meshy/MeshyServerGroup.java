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
import com.addthis.meshy.service.file.FileStats;
import com.addthis.meshy.service.stream.StreamStats;
import com.addthis.muxy.ReadMuxFileDirectoryCache;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.VirtualMachineMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;


class MeshyServerGroup {

    private static final Logger log = LoggerFactory.getLogger(MeshyServerGroup.class);

    private static final GCMetrics gcMetrics = new GCMetrics();
    private static final boolean MERGE_METRICS = Parameter.boolValue("meshy.metrics.merge", true);

    private final Set<String> byUuid = Sets.newConcurrentHashSet();
    private final Set<MeshyServer> byServer = Sets.newConcurrentHashSet();
    private final String uuid = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
    private final LinkedList<String> lastStats = new LinkedList<>();
    private final Thread statsThread;
    private volatile int openStreams;
    private int statsCountdown = 2;

    // TODO replace with scheduled thread pool
    public MeshyServerGroup() {
        statsThread = new Thread() {
            public void run() {
                setName("MeshyStats");
                if (Meshy.STATS_INTERVAL <= 0) {
                    log.debug("stats thread disabled");
                    return;
                }
                while (true) {
                    emitStats();
                    try {
                        Thread.sleep(Meshy.STATS_INTERVAL);
                    } catch (Exception ignored) {
                        return;
                    }
                }
            }
        };
        statsThread.setDaemon(true);
        statsThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> statsThread.interrupt()));
    }

    public String[] getLastStats() {
        synchronized (lastStats) {
            return lastStats.toArray(new String[lastStats.size()]);
        }
    }

    public Map<String, Integer> getLastStatsMap() {
        HashMap<String, Integer> stats = new HashMap<>();
        stats.put("sO", openStreams);
        return stats;
    }

    private void emitStats() {
        GCSummary gc = gcMetrics.update(Meshy.vmMetrics);
        StreamStats ss = new StreamStats();
        FileStats fs = new FileStats();

        StringBuilder rep = new StringBuilder();
        rep.append("seqReads=");
        rep.append(ss.seqRead); // number of sequential nextBytes from the same target
        rep.append(" totalReads=");
        rep.append(ss.totalRead); // number of total reads across all targets
        rep.append(" bytesRead=");
        rep.append(ss.readBytes); // number of total bytes read across all targets (does not include rerouting)
        rep.append(" sN=");
        rep.append(ss.newOpenCount); // newly open streams since last logline
        rep.append(" sC=");
        rep.append(ss.closedStreams); // closed streams since last logline
        rep.append(" sO=");
        openStreams = ss.openCount;
        rep.append(openStreams); // open streams
        rep.append(" sQ=");
        rep.append(ss.qSize);  // "more" finderQueue size
        rep.append(" sR=");
        rep.append(ss.readWaitTime);  // time spent reading from disk
        rep.append(" sW=");
        rep.append(Meshy.numbers.format(ss.sendWaiting));  // send buffers bytes waiting to return
        rep.append(" sZ=");
        rep.append(ss.sleeps); // sleeps b/c over sendWait limit
        rep.append(" cZ=");
        rep.append(ChannelState.writeSleeps.getAndSet(0)); // sleeps b/c over channel watermark
        rep.append(" fQ=");
        rep.append(fs.finderQueue); // number of finds waiting in queue
        rep.append(" fR=");
        rep.append(fs.findsRunning); // calls to find in-progress
        rep.append(" fF=");
        rep.append(fs.finds); // calls to find command
        rep.append(" fO=");
        rep.append(fs.found); // number of files returned
        rep.append(" fT=");
        rep.append(fs.findTime); // time spend in find command
        rep.append(" fTL=");
        rep.append(fs.findTimeLocal); // time spend in find command locally
        rep.append(" iSR=");
        rep.append(InputStreamWrapper.getShortReadCount()); // input stream wrapper short reads (bad for perf)
        rep.append(" gcR=");
        rep.append(gc.runs); // # of gc invocations
        rep.append(" gcT=");
        rep.append(gc.timeSpent); // ms spent in gc
        if (LocalFileHandlerMux.muxEnabled) {
            rep.append(" mD=");
            rep.append(ReadMuxFileDirectoryCache.getCacheDirSize()); // muxy cached dirs
            rep.append(" mF=");
            rep.append(ReadMuxFileDirectoryCache.getCacheFileSize()); // muxy cached files
        }

        int bin = 0;
        int bout = 0;
        if (MERGE_METRICS) {
            int channelCount = 0;
            int peerCount = 0;
            for (MeshyServer server : byServer) {
                ServerStats stats = server.getStats();
                bin += stats.bin;
                bout += stats.bout;
                channelCount += stats.channelCount;
                peerCount += stats.peerCount;
            }
            rep.append(" mC=").append(channelCount); // total channel count
            rep.append(" mS=").append(peerCount); // fully connected channels
            rep.append(" mBI=").append(bin); // total bytes in
            rep.append(" mBO=").append(bout); // total bytes out
        } else {
            int index = 0;
            for (MeshyServer server : byServer) {
                ServerStats stats = server.getStats();
                bin += stats.bin;
                bout += stats.bout;
                String pre = byServer.size() > 1 ? (" " + index) : " ";
                rep.append(pre).append("p=").append(server.getLocalPort()).append('-').append(server.getNetIf());
                rep.append(pre).append("mS=").append(stats.peerCount); // fully connected channels
                rep.append(pre).append("mBI=").append(stats.bin); // total bytes in
                rep.append(pre).append("mBO=").append(stats.bout); // total bytes out
                index++;
            }
        }

        final boolean statsSkip = (bin | bout) == 0;
        if (Meshy.THROTTLE_LOG && statsSkip && statsCountdown-- <= 0) {
            return;
        }

        String report = rep.toString();
        MeshyServer.log.info(report);
        synchronized (lastStats) {
            lastStats.addLast("t=" + JitterClock.globalTime() + ' ' + report);
            if (lastStats.size() > 10) {
                lastStats.removeFirst();
            }
        }
        if (!statsSkip) {
            statsCountdown = 2;
        }
        if (gc.timeSpent > Meshy.STATS_INTERVAL) {
            for (MeshyServer server : byServer) {
                synchronized (server.connectedChannels) {
                    for (ChannelState channelState : server.connectedChannels) {
                        channelState.debugSessions();
                    }
                }
            }
        }
    }

    public void join(MeshyServer server) {
        byUuid.add(server.getUUID());
         {
            byServer.add(server);
        }
    }

    public boolean hasUuid(String testUuid) {
         {
            return byUuid.contains(testUuid);
        }
    }

    public boolean hasServer(MeshyServer server) {
         {
            return byServer.contains(server);
        }
    }

    public MeshyServer[] getMembers() {
         {
            return byServer.toArray(new MeshyServer[byServer.size()]);
        }
    }

    public String getGroupUuid() {
        return uuid;
    }

    private static class GCMetrics {

        private long lastTime;
        private long lastRuns;

        GCSummary update(VirtualMachineMetrics vmMetrics) {
            long totalTime = 0;
            long totalRuns = 0;
            for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> e : vmMetrics.garbageCollectors().entrySet()) {
                VirtualMachineMetrics.GarbageCollectorStats stats = e.getValue();
                totalTime += Math.max(0, stats.getTime(TimeUnit.MILLISECONDS));
                totalRuns += Math.max(0, stats.getRuns());
            }
            long newTime = totalTime - lastTime;
            long newRuns = totalRuns - lastRuns;
            lastTime = totalTime;
            lastRuns = totalRuns;
            return new GCSummary(newTime, newRuns);
        }
    }

    private static class GCSummary {

        public final long timeSpent;
        public final long runs;

        private GCSummary(long timeSpent, long runs) {
            this.runs = runs;
            this.timeSpent = timeSpent;
        }
    }
}
