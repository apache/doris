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

import org.apache.doris.monitor.unit.ByteSizeValue;
import org.apache.doris.monitor.unit.TimeValue;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JvmStats {

    private static final RuntimeMXBean runtimeMXBean;
    private static final MemoryMXBean memoryMXBean;
    private static final ThreadMXBean threadMXBean;
    private static final ClassLoadingMXBean classLoadingMXBean;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
    }

    public static JvmStats jvmStats() {
        MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
        long heapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        long heapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        long heapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();
        memUsage = memoryMXBean.getNonHeapMemoryUsage();
        long nonHeapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        long nonHeapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        List<MemoryPool> pools = new ArrayList<>();
        for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans) {
            try {
                MemoryUsage usage = memoryPoolMXBean.getUsage();
                MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
                String name = GcNames.getByMemoryPoolName(memoryPoolMXBean.getName(), null);
                if (name == null) { // if we can't resolve it, its not interesting.... (Per Gen, Code Cache)
                    continue;
                }
                pools.add(new MemoryPool(name,
                        usage.getUsed() < 0 ? 0 : usage.getUsed(),
                        usage.getMax() < 0 ? heapMax : usage.getMax(),
                        peakUsage.getUsed() < 0 ? 0 : peakUsage.getUsed(),
                        peakUsage.getMax() < 0 ? 0 : peakUsage.getMax()
                ));
            } catch (Exception ex) {
                /* ignore some JVMs might barf here with:
                 * java.lang.InternalError: Memory Pool not found
                 * we just omit the pool in that case!*/
            }
        }
        Mem mem = new Mem(heapCommitted, heapUsed, heapMax, nonHeapCommitted, nonHeapUsed,
                Collections.unmodifiableList(pools));

        int threadsNew = 0;
        int threadsRunnable = 0;
        int threadsBlocked = 0;
        int threadsWaiting = 0;
        int threadsTimedWaiting = 0;
        int threadsTerminated = 0;
        long[] threadIds = threadMXBean.getAllThreadIds();
        for (ThreadInfo threadInfo : threadMXBean.getThreadInfo(threadIds, 0)) {
            if (threadInfo == null) {
                continue; // race protection
            }
            switch (threadInfo.getThreadState()) {
                case NEW:
                    threadsNew++;
                    break;
                case RUNNABLE:
                    threadsRunnable++;
                    break;
                case BLOCKED:
                    threadsBlocked++;
                    break;
                case WAITING:
                    threadsWaiting++;
                    break;
                case TIMED_WAITING:
                    threadsTimedWaiting++;
                    break;
                case TERMINATED:
                    threadsTerminated++;
                    break;
                default:
                    break;
            }
        }
        Threads threads = new Threads(threadMXBean.getThreadCount(), threadMXBean.getPeakThreadCount(), threadsNew,
                threadsRunnable, threadsBlocked, threadsWaiting, threadsTimedWaiting, threadsTerminated);

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        GarbageCollector[] collectors = new GarbageCollector[gcMxBeans.size()];
        for (int i = 0; i < collectors.length; i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            collectors[i] = new GarbageCollector(GcNames.getByGcName(gcMxBean.getName(), gcMxBean.getName()),
                    gcMxBean.getCollectionCount(), gcMxBean.getCollectionTime());
        }
        GarbageCollectors garbageCollectors = new GarbageCollectors(collectors);
        List<BufferPool> bufferPoolsList = Collections.emptyList();
        try {
            List<BufferPoolMXBean> bufferPools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            bufferPoolsList = new ArrayList<>(bufferPools.size());
            for (BufferPoolMXBean bufferPool : bufferPools) {
                bufferPoolsList.add(new BufferPool(bufferPool.getName(), bufferPool.getCount(),
                        bufferPool.getTotalCapacity(), bufferPool.getMemoryUsed()));
            }
        } catch (Exception e) {
            // buffer pools are not available
        }

        Classes classes = new Classes(classLoadingMXBean.getLoadedClassCount(),
                classLoadingMXBean.getTotalLoadedClassCount(),
                classLoadingMXBean.getUnloadedClassCount());

        return new JvmStats(System.currentTimeMillis(), runtimeMXBean.getUptime(), mem, threads,
                garbageCollectors, bufferPoolsList, classes);
    }

    private final long timestamp;
    private final long uptime;
    private final Mem mem;
    private final Threads threads;
    private final GarbageCollectors gc;
    private final List<BufferPool> bufferPools;
    private final Classes classes;

    public JvmStats(long timestamp, long uptime, Mem mem, Threads threads, GarbageCollectors gc,
                    List<BufferPool> bufferPools, Classes classes) {
        this.timestamp = timestamp;
        this.uptime = uptime;
        this.mem = mem;
        this.threads = threads;
        this.gc = gc;
        this.bufferPools = bufferPools;
        this.classes = classes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TimeValue getUptime() {
        return new TimeValue(uptime);
    }

    public Mem getMem() {
        return this.mem;
    }

    public Threads getThreads() {
        return threads;
    }

    public GarbageCollectors getGc() {
        return gc;
    }

    public List<BufferPool> getBufferPools() {
        return bufferPools;
    }

    public Classes getClasses() {
        return classes;
    }

    static final class Fields {
        static final String JVM = "jvm";
        static final String TIMESTAMP = "timestamp";
        static final String UPTIME = "uptime";
        static final String UPTIME_IN_MILLIS = "uptime_in_millis";

        static final String MEM = "mem";
        static final String HEAP_USED = "heap_used";
        static final String HEAP_USED_IN_BYTES = "heap_used_in_bytes";
        static final String HEAP_USED_PERCENT = "heap_used_percent";
        static final String HEAP_MAX = "heap_max";
        static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
        static final String HEAP_COMMITTED = "heap_committed";
        static final String HEAP_COMMITTED_IN_BYTES = "heap_committed_in_bytes";

        static final String NON_HEAP_USED = "non_heap_used";
        static final String NON_HEAP_USED_IN_BYTES = "non_heap_used_in_bytes";
        static final String NON_HEAP_COMMITTED = "non_heap_committed";
        static final String NON_HEAP_COMMITTED_IN_BYTES = "non_heap_committed_in_bytes";

        static final String POOLS = "pools";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String MAX = "max";
        static final String MAX_IN_BYTES = "max_in_bytes";
        static final String PEAK_USED = "peak_used";
        static final String PEAK_USED_IN_BYTES = "peak_used_in_bytes";
        static final String PEAK_MAX = "peak_max";
        static final String PEAK_MAX_IN_BYTES = "peak_max_in_bytes";

        static final String THREADS = "threads";
        static final String COUNT = "count";
        static final String PEAK_COUNT = "peak_count";
        static final String NEW_COUNT = "new_count";
        static final String RUNNING_COUNT = "runnable_count";
        static final String BLOCKED_COUNT = "blocked_count";
        static final String WAITING_COUNT = "waiting_count";
        static final String TIMED_WAITING_COUNT = "timed_waiting_count";
        static final String TERMINATED_COUNT = "terminated_count";

        static final String GC = "gc";
        static final String COLLECTORS = "collectors";
        static final String COLLECTION_COUNT = "collection_count";
        static final String COLLECTION_TIME = "collection_time";
        static final String COLLECTION_TIME_IN_MILLIS = "collection_time_in_millis";

        static final String BUFFER_POOLS = "buffer_pools";
        static final String TOTAL_CAPACITY = "total_capacity";
        static final String TOTAL_CAPACITY_IN_BYTES = "total_capacity_in_bytes";

        static final String CLASSES = "classes";
        static final String CURRENT_LOADED_COUNT = "current_loaded_count";
        static final String TOTAL_LOADED_COUNT = "total_loaded_count";
        static final String TOTAL_UNLOADED_COUNT = "total_unloaded_count";
    }

    public static class GarbageCollectors implements Iterable<GarbageCollector> {

        private final GarbageCollector[] collectors;

        public GarbageCollectors(GarbageCollector[] collectors) {
            this.collectors = collectors;
        }

        public GarbageCollector[] getCollectors() {
            return this.collectors;
        }

        @Override
        public Iterator<GarbageCollector> iterator() {
            return Arrays.stream(collectors).iterator();
        }
    }

    public static class GarbageCollector {

        private final String name;
        private final long collectionCount;
        private final long collectionTime;

        public GarbageCollector(String name, long collectionCount, long collectionTime) {
            this.name = name;
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        public String getName() {
            return this.name;
        }

        public long getCollectionCount() {
            return this.collectionCount;
        }

        public TimeValue getCollectionTime() {
            return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(name);
            sb.append(": collection count: ").append(collectionCount);
            sb.append(", collection time: ").append(getCollectionTime().getStringRep());
            return sb.toString();
        }
    }

    public static class Threads {

        private final int count;
        private final int peakCount;
        private final int threadsNewCount;
        private final int threadsRunnableCount;
        private final int threadsBlockedCount;
        private final int threadsWaitingCount;
        private final int threadsTimedWaitingCount;
        private final int threadsTerminatedCount;

        public Threads(int count, int peakCount, int threadsNewCount, int threadsRunnableCount, int threadsBlockedCount,
                       int threadsWaitingCount, int threadsTimedWaitingCount, int threadsTerminatedCount) {
            this.count = count;
            this.peakCount = peakCount;
            this.threadsNewCount = threadsNewCount;
            this.threadsRunnableCount = threadsRunnableCount;
            this.threadsBlockedCount = threadsBlockedCount;
            this.threadsWaitingCount = threadsWaitingCount;
            this.threadsTimedWaitingCount = threadsTimedWaitingCount;
            this.threadsTerminatedCount = threadsTerminatedCount;
        }

        public int getCount() {
            return count;
        }

        public int getPeakCount() {
            return peakCount;
        }

        public int getThreadsNewCount() {
            return threadsNewCount;
        }

        public int getThreadsRunnableCount() {
            return threadsRunnableCount;
        }

        public int getThreadsBlockedCount() {
            return threadsBlockedCount;
        }

        public int getThreadsWaitingCount() {
            return threadsWaitingCount;
        }

        public int getThreadsTimedWaitingCount() {
            return threadsTimedWaitingCount;
        }

        public int getThreadsTerminatedCount() {
            return threadsTerminatedCount;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("count: ").append(count).append(", peak count: ").append(peakCount);
            return sb.toString();
        }
    }

    public static class MemoryPool {

        private final String name;
        private final long used;
        private final long max;
        private final long peakUsed;
        private final long peakMax;

        public MemoryPool(String name, long used, long max, long peakUsed, long peakMax) {
            this.name = name;
            this.used = used;
            this.max = max;
            this.peakUsed = peakUsed;
            this.peakMax = peakMax;
        }

        public String getName() {
            return this.name;
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getMax() {
            return new ByteSizeValue(max);
        }

        public ByteSizeValue getPeakUsed() {
            return new ByteSizeValue(peakUsed);
        }

        public ByteSizeValue getPeakMax() {
            return new ByteSizeValue(peakMax);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("name: ").append(name).append(", used: ").append(getUsed().toString());
            sb.append(", max: ").append(getMax().toString()).append(", peak used: ").append(getPeakUsed().toString());
            sb.append(", peak max: ").append(getPeakMax().toString());
            return sb.toString();
        }
    }

    public static class Mem implements Iterable<MemoryPool> {

        private final long heapCommitted;
        private final long heapUsed;
        private final long heapMax;
        private final long nonHeapCommitted;
        private final long nonHeapUsed;
        private final List<MemoryPool> pools;

        public Mem(long heapCommitted, long heapUsed, long heapMax,
                   long nonHeapCommitted, long nonHeapUsed, List<MemoryPool> pools) {
            this.heapCommitted = heapCommitted;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.nonHeapCommitted = nonHeapCommitted;
            this.nonHeapUsed = nonHeapUsed;
            this.pools = pools;
        }

        @Override
        public Iterator<MemoryPool> iterator() {
            return pools.iterator();
        }

        public ByteSizeValue getHeapCommitted() {
            return new ByteSizeValue(heapCommitted);
        }

        public ByteSizeValue getHeapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        /**
         * returns the maximum heap size. 0 bytes signals unknown.
         */
        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        /**
         * returns the heap usage in percent. -1 signals unknown.
         */
        public short getHeapUsedPercent() {
            if (heapMax == 0) {
                return -1;
            }
            return (short) (heapUsed * 100 / heapMax);
        }

        public ByteSizeValue getNonHeapCommitted() {
            return new ByteSizeValue(nonHeapCommitted);
        }

        public ByteSizeValue getNonHeapUsed() {
            return new ByteSizeValue(nonHeapUsed);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("heap committed: ").append(getHeapCommitted().toString());
            sb.append(", heap used: ").append(getHeapUsed().toString());
            sb.append(", heap max: ").append(getHeapMax().toString());
            sb.append(", non heap committed: ").append(getNonHeapCommitted().toString());
            sb.append(", non heap used: ").append(getNonHeapUsed().toString());
            sb.append("\nMem pools: ");
            for (MemoryPool memoryPool : pools) {
                sb.append(memoryPool.toString()).append("\n");
            }
            return sb.toString();
        }
    }

    public static class BufferPool {

        private final String name;
        private final long count;
        private final long totalCapacity;
        private final long used;

        public BufferPool(String name, long count, long totalCapacity, long used) {
            this.name = name;
            this.count = count;
            this.totalCapacity = totalCapacity;
            this.used = used;
        }

        public String getName() {
            return this.name;
        }

        public long getCount() {
            return this.count;
        }

        public ByteSizeValue getTotalCapacity() {
            return new ByteSizeValue(totalCapacity);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("name: ").append(name).append(", count: ").append(count);
            sb.append("total capacity: ").append(totalCapacity).append(", used: ").append(used);
            return sb.toString();
        }
    }

    public static class Classes {

        private final long loadedClassCount;
        private final long totalLoadedClassCount;
        private final long unloadedClassCount;

        public Classes(long loadedClassCount, long totalLoadedClassCount, long unloadedClassCount) {
            this.loadedClassCount = loadedClassCount;
            this.totalLoadedClassCount = totalLoadedClassCount;
            this.unloadedClassCount = unloadedClassCount;
        }

        public long getLoadedClassCount() {
            return loadedClassCount;
        }

        public long getTotalLoadedClassCount() {
            return totalLoadedClassCount;
        }

        public long getUnloadedClassCount() {
            return unloadedClassCount;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Classes: ").append("loaded: ").append(loadedClassCount);
            sb.append(", total loaded: ").append(totalLoadedClassCount);
            sb.append(", unloaded: ").append(unloadedClassCount);
            return sb.toString();
        }
    }
}
