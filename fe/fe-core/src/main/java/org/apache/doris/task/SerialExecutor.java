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

package org.apache.doris.task;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SerialExecutor extends AbstractExecutorService {

    private final ExecutorService taskPool;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition terminating = lock.newCondition();

    private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
    private Runnable active;

    private boolean shutdown;

    public SerialExecutor(final ExecutorService executor) {
        Preconditions.checkNotNull(executor);
        this.taskPool = executor;
    }

    public void execute(final Runnable r) {
        lock.lock();
        try {
            checkPoolIsRunning();
            tasks.add(new Runnable() {
                public void run() {
                    try {
                        r.run();
                    } finally {
                        scheduleNext();
                    }
                }
            });
            if (active == null) {
                scheduleNext();
            }
        } finally {
            lock.unlock();
        }
    }

    private void checkPoolIsRunning() {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        if (shutdown) {
            throw new RejectedExecutionException("SerialExecutor is already shutdown");
        }
    }

    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
        } finally {
            lock.unlock();
        }
    }

    public List<Runnable> shutdownNow() {
        lock.lock();
        try {
            shutdown = true;
            List<Runnable> result = new ArrayList<>();
            tasks.drainTo(result);
            return result;
        } finally {
            lock.unlock();
        }
    }

    public boolean isShutdown() {
        lock.lock();
        try {
            return shutdown;
        } finally {
            lock.unlock();
        }
    }

    public boolean isTerminated() {
        lock.lock();
        try {
            return shutdown && active == null;
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            long waitUntil = System.nanoTime() + unit.toNanos(timeout);
            long remainingTime;
            while ((remainingTime = waitUntil - System.nanoTime()) > 0) {
                if (shutdown && active == null) {
                    break;
                }
                terminating.awaitNanos(remainingTime);
            }
            return remainingTime > 0;
        } finally {
            lock.unlock();
        }
    }

    private void scheduleNext() {
        lock.lock();
        try {
            if ((active = tasks.poll()) != null) {
                taskPool.execute(active);
            } else if (shutdown) {
                terminating.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }
}
