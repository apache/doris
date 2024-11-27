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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A monitored version of ReentrantReadWriteLock that provides additional
 * monitoring capabilities for read and write locks.
 */
public class MonitoredReentrantReadWriteLock extends ReentrantReadWriteLock {
    // Monitored read and write lock instances
    private final ReadLock readLock = new ReadLock(this);
    private final WriteLock writeLock = new WriteLock(this);

    // Constructor for creating a monitored lock with fairness option
    public MonitoredReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    public MonitoredReentrantReadWriteLock() {
    }

    /**
     * Monitored read lock class that extends ReentrantReadWriteLock.ReadLock.
     */
    public class ReadLock extends ReentrantReadWriteLock.ReadLock {
        private static final long serialVersionUID = 1L;
        private final AbstractMonitoredLock monitor = new AbstractMonitoredLock() {};

        /**
         * Constructs a new ReadLock instance.
         *
         * @param lock The ReentrantReadWriteLock this lock is associated with
         */
        protected ReadLock(ReentrantReadWriteLock lock) {
            super(lock);
        }

        /**
         * Acquires the read lock.
         * Records the time when the lock is acquired.
         */
        @Override
        public void lock() {
            super.lock();
            monitor.afterLock();
        }

        /**
         * Releases the read lock.
         * Records the time when the lock is released and logs the duration.
         */
        @Override
        public void unlock() {
            monitor.afterUnlock();
            super.unlock();
        }
    }

    /**
     * Monitored write lock class that extends ReentrantReadWriteLock.WriteLock.
     */
    public class WriteLock extends ReentrantReadWriteLock.WriteLock {
        private static final long serialVersionUID = 1L;
        private final AbstractMonitoredLock monitor = new AbstractMonitoredLock() {};

        /**
         * Constructs a new WriteLock instance.
         *
         * @param lock The ReentrantReadWriteLock this lock is associated with
         */
        protected WriteLock(ReentrantReadWriteLock lock) {
            super(lock);
        }

        /**
         * Acquires the write lock.
         * Records the time when the lock is acquired.
         */
        @Override
        public void lock() {
            super.lock();
            monitor.afterLock();
        }

        /**
         * Releases the write lock.
         * Records the time when the lock is released and logs the duration.
         */
        @Override
        public void unlock() {
            monitor.afterUnlock();
            super.unlock();
        }
    }

    /**
     * Returns the read lock associated with this lock.
     *
     * @return The monitored read lock
     */
    @Override
    public ReadLock readLock() {
        return readLock;
    }

    /**
     * Returns the write lock associated with this lock.
     *
     * @return The monitored write lock
     */
    @Override
    public WriteLock writeLock() {
        return writeLock;
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }
}
