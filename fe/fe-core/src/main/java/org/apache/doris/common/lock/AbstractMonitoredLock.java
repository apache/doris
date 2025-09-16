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

import org.apache.doris.common.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for a monitored lock that tracks lock acquisition,
 * release, and attempt times. It provides mechanisms for monitoring the
 * duration for which a lock is held and logging any instances where locks
 * are held longer than a specified timeout or fail to be acquired within
 * a specified timeout.
 */
public abstract class AbstractMonitoredLock {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMonitoredLock.class);

    // Thread-local variable to store the lock start time
    private final ThreadLocal<Long> lockStartTime = new ThreadLocal<>();


    /**
     * Method to be called after successfully acquiring the lock.
     * Sets the start time for the lock.
     */
    protected void afterLock() {
        lockStartTime.set(System.nanoTime());
    }

    /**
     * Method to be called after releasing the lock.
     * Calculates the lock hold time and logs a warning if it exceeds the hold timeout.
     */
    protected void afterUnlock() {
        Long startTime = lockStartTime.get();
        if (startTime != null) {
            long lockHoldTimeNanos = System.nanoTime() - startTime;
            long lockHoldTimeMs = lockHoldTimeNanos >> 20;
            if (lockHoldTimeMs > Config.max_lock_hold_threshold_seconds * 1000) {
                Thread currentThread = Thread.currentThread();
                String stackTrace = getThreadStackTrace(currentThread.getStackTrace());
                LOG.warn("Thread ID: {}, Thread Name: {} - Lock held for {} ms, exceeding hold timeout of {} ms "
                                + "Thread stack trace:{}",
                        currentThread.getId(), currentThread.getName(), lockHoldTimeMs, lockHoldTimeMs, stackTrace);
            }
            lockStartTime.remove();
        }
    }

    /**
     * Method to be called after attempting to acquire the lock using tryLock.
     * Logs a warning if the lock was not acquired within a reasonable time.
     *
     * @param acquired  Whether the lock was successfully acquired
     * @param startTime The start time of the lock attempt
     */
    protected void afterTryLock(boolean acquired, long startTime) {
        if (acquired) {
            afterLock();
            return;
        }
        if (LOG.isDebugEnabled()) {
            long elapsedTime = (System.nanoTime() - startTime) >> 20;
            Thread currentThread = Thread.currentThread();
            String stackTrace = getThreadStackTrace(currentThread.getStackTrace());
            LOG.debug("Thread ID: {}, Thread Name: {} - Failed to acquire the lock within {} ms"
                            + "\nThread blocking info:\n{}",
                    currentThread.getId(), currentThread.getName(), elapsedTime, stackTrace);
        }
    }

    /**
     * Utility method to format the stack trace of a thread.
     *
     * @param stackTrace The stack trace elements of the thread
     * @return A formatted string of the stack trace
     */
    private String getThreadStackTrace(StackTraceElement[] stackTrace) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : stackTrace) {
            sb.append("\tat ").append(element).append("\n");
        }
        return sb.toString().replace("\n", "\\n");
    }
}


