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


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ThreadPoolManager {

    private static Map<String, ThreadPoolExecutor> nameToThreadPoolMap = new HashMap<>();


    public static Map<String, ThreadPoolExecutor> getNameToThreadPoolMap() {
        return nameToThreadPoolMap;
    }

    public static void registerThreadPoolMetric(String poolName, ThreadPoolExecutor threadPool) {
        nameToThreadPoolMap.put(poolName, threadPool);
    }

    public static ThreadPoolExecutor newDaemonCacheThreadPool(int maxNumThread, String poolName) {
        return newDaemonThreadPool(0, maxNumThread, 60L, TimeUnit.SECONDS, new SynchronousQueue(), new LogDiscardPolicy(poolName), poolName);
    }

    public static ThreadPoolExecutor newDaemonFixedThreadPool(int numThread, int queueSize, String poolName) {
       return newDaemonThreadPool(numThread, numThread, 60L ,TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueSize),
               new BlockedPolicy(poolName, 60), poolName);
    }

    public static ThreadPoolExecutor newDaemonThreadPool(int corePoolSize,
                                               int maximumPoolSize,
                                               long keepAliveTime,
                                               TimeUnit unit,
                                               BlockingQueue<Runnable> workQueue,
                                               RejectedExecutionHandler handler,
                                               String poolName) {
        ThreadFactory threadFactory = namedThreadFactory(poolName);
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
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

        private static final Logger LOG = LogManager.getLogger(LogDiscardPolicy.class);

        private String threadPoolName;

        private int timeoutSeconds;

        public BlockedPolicy(String threadPoolName, int timeoutSeconds) {
            this.threadPoolName = threadPoolName;
            this.timeoutSeconds = timeoutSeconds;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                executor.getQueue().offer(r, timeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("Task " + r.toString() + " wait to enqueue in " + threadPoolName + " " + executor.toString() + " failed");
            }
        }
    }
}

