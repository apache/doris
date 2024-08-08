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

package org.apache.doris.common.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A monitored version of ReentrantLock that provides additional monitoring capabilities
 * for lock acquisition and release.
 */
public class MonitoredReentrantLock extends ReentrantLock {
    private static final long serialVersionUID = 1L;

    // Monitor for tracking lock acquisition and release
    private final AbstractMonitoredLock lockMonitor = new AbstractMonitoredLock() {
    };

    // Constructor for creating a monitored lock with fairness option
    public MonitoredReentrantLock(boolean fair) {
        super(fair);
    }

    // Constructor for creating a monitored lock with fairness option
    public MonitoredReentrantLock() {
    }

    /**
     * Acquires the lock.
     * Records the time when the lock is acquired.
     */
    @Override
    public void lock() {
        super.lock();
        lockMonitor.afterLock();
    }

    /**
     * Releases the lock.
     * Records the time when the lock is released and logs the duration.
     */
    @Override
    public void unlock() {
        lockMonitor.afterUnlock();
        super.unlock();
    }

    /**
     * Tries to acquire the lock.
     * Records the time when the lock attempt started and logs the result.
     *
     * @return true if the lock was acquired, false otherwise
     */
    @Override
    public boolean tryLock() {
        long start = System.nanoTime(); // Record start time
        boolean acquired = super.tryLock(); // Attempt to acquire the lock
        lockMonitor.afterTryLock(acquired, start); // Log result and elapsed time
        return acquired;
    }

    /**
     * Tries to acquire the lock within the specified time limit.
     * Records the time when the lock attempt started and logs the result.
     *
     * @param timeout the time to wait for the lock
     * @param unit    the time unit of the timeout argument
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        long start = System.nanoTime(); // Record start time
        boolean acquired = super.tryLock(timeout, unit); // Attempt to acquire the lock
        lockMonitor.afterTryLock(acquired, start); // Log result and elapsed time
        return acquired;
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }
}
