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

package org.apache.doris.common;

import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ThreadPoolManager is a helper class for construct daemon thread pool with limit thread and memory resource.
 * thread names in thread pool are formatted as poolName-ID, where ID is a unique, sequentially assigned integer.
 * it provide four functions to construct thread pool now.
 *
 * 1. newDaemonCacheThreadPool
 *    Wrapper over newCachedThreadPool with additional maxNumThread limit.
 * 2. newDaemonFixedThreadPool
 *    Wrapper over newCachedThreadPool with additional blocking queue capacity limit.
 * 3. newDaemonThreadPool
 *    Wrapper over ThreadPoolExecutor, user can use it to construct thread pool more flexibly.
 * 4. newDaemonScheduledThreadPool
 *    Wrapper over ScheduledThreadPoolExecutor, but without delay task num limit and thread num limit now(NOTICE).
 *
 *  All thread pool constructed by ThreadPoolManager will be added to the nameToThreadPoolMap,
 *  so the thread pool name in fe must be unique.
 *  when all thread pools are constructed, ThreadPoolManager will register some metrics of all thread pool to MetricRepo,
 *  so we can know the runtime state for all thread pool by prometheus metrics
 */

public class ThreadPoolManager {

    private static Map<String, ThreadPoolExecutor> nameToThreadPoolMap = Maps.newConcurrentMap();

    private static String[] poolMetricTypes = {"pool_size", "active_thread_num", "task_in_queue"};

    private final static long KEEP_ALIVE_TIME = 60L;

    public static void registerAllThreadPoolMetric() {
        for (Map.Entry<String, ThreadPoolExecutor> entry : nameToThreadPoolMap.entrySet()) {
            registerThreadPoolMetric(entry.getKey(), entry.getValue());
        }
        nameToThreadPoolMap.clear();
    }

    public static void registerThreadPoolMetric(String poolName, ThreadPoolExecutor threadPool) {
        for (String poolMetricType : poolMetricTypes) {
            GaugeMetric<Integer> gauge = new GaugeMetric<Integer>("thread_pool", MetricUnit.NOUNIT, "thread_pool statistics") {
                @Override
                public Integer getValue() {
                    String metricType = this.getLabels().get(1).getValue();
                    switch (metricType) {
                        case "pool_size":
                            return threadPool.getPoolSize();
                        case "active_thread_num":
                            return threadPool.getActiveCount();
                        case "task_in_queue":
                            return threadPool.getQueue().size();
                        default:
                            return 0;
                    }
                }
            };
            gauge.addLabel(new MetricLabel("name", poolName))
                    .addLabel(new MetricLabel("type", poolMetricType));
            MetricRepo.PALO_METRIC_REGISTER.addPaloMetrics(gauge);
        }
    }

    public static ThreadPoolExecutor newDaemonCacheThreadPool(int maxNumThread, String poolName, boolean needRegisterMetric) {
        return newDaemonThreadPool(0, maxNumThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue(),
                new LogDiscardPolicy(poolName), poolName, needRegisterMetric);
    }

    public static ThreadPoolExecutor newDaemonFixedThreadPool(int numThread, int queueSize, String poolName, boolean needRegisterMetric) {
        return newDaemonThreadPool(numThread, numThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueSize),
                new BlockedPolicy(poolName, 60), poolName, needRegisterMetric);
    }

    public static ThreadPoolExecutor newDaemonProfileThreadPool(int numThread, int queueSize, String poolName,
                                                                boolean needRegisterMetric) {
        return newDaemonThreadPool(numThread, numThread, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize), new LogDiscardOldestPolicy(poolName), poolName,
                needRegisterMetric);
    }

    public static ThreadPoolExecutor newDaemonThreadPool(int corePoolSize,
                                                         int maximumPoolSize,
                                                         long keepAliveTime,
                                                         TimeUnit unit,
                                                         BlockingQueue<Runnable> workQueue,
                                                         RejectedExecutionHandler handler,
                                                         String poolName,
                                                         boolean needRegisterMetric) {
        ThreadFactory threadFactory = namedThreadFactory(poolName);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        if (needRegisterMetric) {
            nameToThreadPoolMap.put(poolName, threadPool);
        }
        return threadPool;
    }

    // Now, we have no delay task num limit and thread num limit in ScheduledThreadPoolExecutor,
    // so it may cause oom when there are too many delay tasks or threads in ScheduledThreadPoolExecutor
    // Please use this api only for scheduling short task at fix rate.
    public static ScheduledThreadPoolExecutor newDaemonScheduledThreadPool(int corePoolSize, String poolName, boolean needRegisterMetric) {
        ThreadFactory threadFactory = namedThreadFactory(poolName);
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
        if (needRegisterMetric) {
            nameToThreadPoolMap.put(poolName, scheduledThreadPoolExecutor);
        }
        return scheduledThreadPoolExecutor;
    }

    /**
     * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
     */
    private static ThreadFactory namedThreadFactory(String poolName) {
        return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(poolName + "-%d").build();
    }

    /**
     * A handler for rejected task that discards and log it, used for cached thread pool
     */
    static class LogDiscardPolicy implements RejectedExecutionHandler {

        private static final Logger LOG = LogManager.getLogger(LogDiscardPolicy.class);

        private String threadPoolName;

        public LogDiscardPolicy(String threadPoolName) {
            this.threadPoolName = threadPoolName;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            LOG.warn("Task " + r.toString() + " rejected from " + threadPoolName + " " + executor.toString());
        }
    }

    /**
     * A handler for rejected task that try to be blocked until the pool enqueue task succeed or timeout, used for fixed thread pool
     */
    static class BlockedPolicy implements RejectedExecutionHandler {

        private static final Logger LOG = LogManager.getLogger(BlockedPolicy.class);

        private String threadPoolName;

        private int timeoutSeconds;

        public BlockedPolicy(String threadPoolName, int timeoutSeconds) {
            this.threadPoolName = threadPoolName;
            this.timeoutSeconds = timeoutSeconds;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                boolean ret = executor.getQueue().offer(r, timeoutSeconds, TimeUnit.SECONDS);
                if (!ret) {
                    throw new RejectedExecutionException("submit task failed, queue size is full: " + this.threadPoolName);
                }
            } catch (InterruptedException e) {
                String errMsg = String.format("Task %s wait to enqueue in %s %s failed",
                        r.toString(), threadPoolName, executor.toString());
                LOG.warn(errMsg);
                throw new RejectedExecutionException(errMsg);
            }
        }
    }

    static class LogDiscardOldestPolicy implements RejectedExecutionHandler {

        private static final Logger LOG = LogManager.getLogger(LogDiscardOldestPolicy.class);

        private String threadPoolName;

        public LogDiscardOldestPolicy(String threadPoolName) {
            this.threadPoolName = threadPoolName;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                Runnable discardTask = executor.getQueue().poll();
                LOG.warn("Task: {} submit to {}, and discard the oldest task:{}", task, threadPoolName, discardTask);
                executor.execute(task);
            }
        }
    }
}

