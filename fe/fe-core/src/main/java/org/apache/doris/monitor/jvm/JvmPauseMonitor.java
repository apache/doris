// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.monitor.jvm;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.log4j.Logger;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a short interval of time. If the sleep takes significantly longer
 * than its target time, it implies that the JVM or host machine has
 * paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message.
 */
public class JvmPauseMonitor {
    private static final Logger LOG = Logger.getLogger(JvmPauseMonitor.class);

    // The target sleep time.
    private static final long SLEEP_INTERVAL_MS = 500;

    // Check for Java deadlocks at this interval. Set by init(). 0 or negative means that
    // the deadlock checks are disabled.
    private long deadlockCheckIntervalS_ = 0;

    // log WARN if we detect a pause longer than this threshold.
    private long warnThresholdMs_;
    private static final long WARN_THRESHOLD_MS = 10000;

    // log INFO if we detect a pause longer than this threshold.
    private long infoThresholdMs_;
    private static final long INFO_THRESHOLD_MS = 1000;

    // Overall metrics
    // Volatile to allow populating metrics concurrently with the values
    // being updated without staleness (but with no other synchronization
    // guarantees).
    private volatile long numGcWarnThresholdExceeded = 0;
    private volatile long numGcInfoThresholdExceeded = 0;
    private volatile long totalGcExtraSleepTime = 0;

    // Daemon thread running the pause monitor loop.
    private Thread monitorThread_;
    private volatile boolean shouldRun = true;

    // Singleton instance of this pause monitor.
    public static JvmPauseMonitor INSTANCE = new JvmPauseMonitor();

    // Initializes the pause monitor. No-op if called multiple times.
    public static void initPauseMonitor(long deadlockCheckIntervalS) {
        if (INSTANCE.isStarted()) return;
        INSTANCE.init(deadlockCheckIntervalS);
    }

    private JvmPauseMonitor() {
        this(INFO_THRESHOLD_MS, WARN_THRESHOLD_MS);
    }

    private JvmPauseMonitor(long infoThresholdMs, long warnThresholdMs) {
        this.infoThresholdMs_ = infoThresholdMs;
        this.warnThresholdMs_ = warnThresholdMs;
    }

    protected void init(long deadlockCheckIntervalS) {
        deadlockCheckIntervalS_ = deadlockCheckIntervalS;
        monitorThread_ = new Thread(new Monitor(), "JVM pause monitor");
        monitorThread_.setDaemon(true);
        monitorThread_.start();
    }

    public boolean isStarted() {
        return monitorThread_ != null;
    }

    public long getNumGcWarnThresholdExceeded() {
        return numGcWarnThresholdExceeded;
    }

    public long getNumGcInfoThresholdExceeded() {
        return numGcInfoThresholdExceeded;
    }

    public long getTotalGcExtraSleepTime() {
        return totalGcExtraSleepTime;
    }

    /**
     * Helper method that formats the message to be logged, along with
     * the GC metrics.
     */
    private String formatMessage(long extraSleepTime,
                                 Map<String, GcTimes> gcTimesAfterSleep,
                                 Map<String, GcTimes> gcTimesBeforeSleep) {

        Set<String> gcBeanNames = Sets.intersection(
                gcTimesAfterSleep.keySet(),
                gcTimesBeforeSleep.keySet());
        List<String> gcDiffs = Lists.newArrayList();
        for (String name : gcBeanNames) {
            GcTimes diff = gcTimesAfterSleep.get(name).subtract(
                    gcTimesBeforeSleep.get(name));
            if (diff.gcCount != 0) {
                gcDiffs.add("GC pool '" + name + "' had collection(s): " +
                        diff.toString());
            }
        }

        String ret = "Detected pause in JVM or host machine (eg GC): " +
                "pause of approximately " + extraSleepTime + "ms\n";
        if (gcDiffs.isEmpty()) {
            ret += "No GCs detected";
        } else {
            ret += Joiner.on("\n").join(gcDiffs);
        }
        return ret;
    }

    private Map<String, GcTimes> getGcTimes() {
        Map<String, GcTimes> map = Maps.newHashMap();
        List<GarbageCollectorMXBean> gcBeans =
                ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            map.put(gcBean.getName(), new GcTimes(gcBean));
        }
        return map;
    }

    private static class GcTimes {
        private GcTimes(GarbageCollectorMXBean gcBean) {
            gcCount = gcBean.getCollectionCount();
            gcTimeMillis = gcBean.getCollectionTime();
        }

        private GcTimes(long count, long time) {
            this.gcCount = count;
            this.gcTimeMillis = time;
        }

        private GcTimes subtract(GcTimes other) {
            return new GcTimes(this.gcCount - other.gcCount,
                    this.gcTimeMillis - other.gcTimeMillis);
        }

        @Override
        public String toString() {
            return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
        }

        private long gcCount;
        private long gcTimeMillis;
    }

    /**
     * Runnable instance of the pause monitor loop. Launched from serviceStart().
     */
    private class Monitor implements Runnable {
        @Override
        public void run() {
            Stopwatch sw = Stopwatch.createUnstarted();
            Stopwatch timeSinceDeadlockCheck = Stopwatch.createStarted();
            Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
            LOG.info("Starting JVM pause monitor");
            while (shouldRun) {
                sw.reset().start();
                try {
                    Thread.sleep(SLEEP_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    LOG.error("JVM pause monitor interrupted", ie);
                    return;
                }
                sw.stop();
                long extraSleepTime = sw.elapsed(TimeUnit.MILLISECONDS) - SLEEP_INTERVAL_MS;
                Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

                if (extraSleepTime > warnThresholdMs_) {
                    ++numGcWarnThresholdExceeded;
                    LOG.warn(formatMessage(
                            extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
                } else if (extraSleepTime > infoThresholdMs_) {
                    ++numGcInfoThresholdExceeded;
                    LOG.info(formatMessage(
                            extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
                }
                totalGcExtraSleepTime += extraSleepTime;
                gcTimesBeforeSleep = gcTimesAfterSleep;

                if (deadlockCheckIntervalS_ > 0 &&
                        timeSinceDeadlockCheck.elapsed(TimeUnit.SECONDS) >= deadlockCheckIntervalS_) {
                    checkForDeadlocks();
                    timeSinceDeadlockCheck.reset().start();
                }
            }
        }

        /**
         * Check for deadlocks between Java threads using the JVM's deadlock detector.
         * If a deadlock is found, log info about the deadlocked threads and exit the
         * process.
         * <p>
         * We choose to exit the process this situation because the deadlock will likely
         * cause hangs and other forms of service unavailability and there is no way to
         * recover from the deadlock except by restarting the process.
         */
        private void checkForDeadlocks() {
            ThreadMXBean threadMx = ManagementFactory.getThreadMXBean();
            long deadlockedTids[] = threadMx.findDeadlockedThreads();
            if (deadlockedTids != null) {
                ThreadInfo deadlockedThreads[] =
                        threadMx.getThreadInfo(deadlockedTids, true, true);
                // Log diagnostics with error before aborting the process with a FATAL log.
                LOG.error("Found " + deadlockedThreads.length + " threads in deadlock: ");
                for (ThreadInfo thread : deadlockedThreads) {
                    // Defensively check for null in case the thread somehow disappeared between
                    // findDeadlockedThreads() and getThreadInfo().
                    if (thread != null) LOG.error(thread.toString());
                }
                LOG.warn("All threads:");
                for (ThreadInfo thread : threadMx.dumpAllThreads(true, true)) {
                    LOG.error(thread.toString());
                }
                // In the context of an Doris service, LOG.fatal calls glog's fatal, which
                // aborts the process, which will produce a coredump if coredumps are enabled.
                LOG.fatal("Aborting because of deadlocked threads in JVM.");
                System.exit(1);
            }
        }
    }

    /**
     * Helper for manual testing that causes a deadlock between java threads.
     */
    private static void causeDeadlock() {
        final Object obj1 = new Object();
        final Object obj2 = new Object();

        new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    synchronized (obj2) {
                        synchronized (obj1) {
                            System.err.println("Thread 1 got locks");
                        }
                    }
                }
            }
        }).start();

        while (true) {
            synchronized (obj1) {
                synchronized (obj2) {
                    System.err.println("Thread 2 got locks");
                }
            }
        }
    }

    /**
     * This function just leaks memory into a list. Running this function
     * with a 1GB heap will very quickly go into "GC hell" and result in
     * log messages about the GC pauses.
     */
    private static void allocateMemory() {
        List<String> list = Lists.newArrayList();
        int i = 0;
        while (true) {
            list.add(String.valueOf(i++));
        }
    }

    /**
     * Simple 'main' to facilitate manual testing of the pause monitor.
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        JvmPauseMonitor monitor = new JvmPauseMonitor();
        monitor.init(60);
        if (args[0].equals("gc")) {
            allocateMemory();
        } else if (args[0].equals("deadlock")) {
            causeDeadlock();
        } else {
            System.err.println("Unknown mode");
        }
    }

}


