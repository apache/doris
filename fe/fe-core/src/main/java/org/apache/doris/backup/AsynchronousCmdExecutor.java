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

package org.apache.doris.backup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsynchronousCmdExecutor<V> {
    private static final Logger LOG = LogManager.getLogger(AsynchronousCmdExecutor.class);
    private final BlockingQueue<Runnable> waitingQueue;
    private final ThreadPoolExecutor pool;

    public AsynchronousCmdExecutor() {
        waitingQueue = new LinkedBlockingQueue<Runnable>();
        // same as Executors.newSingleThreadExecutor()
        // use this to help monitoring queue size
        pool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, waitingQueue);
    }

    public Future<V> submit(Callable<V> task) {
        Future<V> future = pool.submit(task);
        LOG.info("submit task. queue size: {}", waitingQueue.size());
        return future;
    }
}
