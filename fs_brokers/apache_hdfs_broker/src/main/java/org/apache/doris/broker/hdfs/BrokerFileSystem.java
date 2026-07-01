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

package org.apache.doris.broker.hdfs;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public class BrokerFileSystem {

    private static Logger logger = Logger.getLogger(BrokerFileSystem.class.getName());

    private ReentrantLock lock;
    private FileSystemIdentity identity;
    private volatile FileSystem dfsFileSystem;
    private volatile long lastAccessTimestamp;
    private UUID fileSystemId;
    private final AtomicInteger activeOperationCount = new AtomicInteger(0);
    private final AtomicInteger activeStreamCount = new AtomicInteger(0);

    public BrokerFileSystem(FileSystemIdentity identity) {
        this.identity = identity;
        this.lock = new ReentrantLock();
        this.dfsFileSystem = null;
        this.lastAccessTimestamp = System.currentTimeMillis();
        this.fileSystemId = UUID.randomUUID();
    }

    public void setFileSystem(FileSystem fileSystem) {
        lock.lock();
        try {
            this.dfsFileSystem = fileSystem;
            this.lastAccessTimestamp = System.currentTimeMillis();
        } finally {
            lock.unlock();
        }
    }

    public void closeFileSystem() throws IOException {
        lock.lock();
        try {
            if (this.dfsFileSystem != null) {
                try {
                    this.dfsFileSystem.close();
                } finally {
                    // Even if it fails, do not call close again, just set null.
                    this.dfsFileSystem = null;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public FileSystem getDFSFileSystem() {
        this.lastAccessTimestamp = System.currentTimeMillis();
        return dfsFileSystem;
    }

    public void updateLastUpdateAccessTime() {
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

    public FileSystemIdentity getIdentity() {
        return identity;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public UUID getFileSystemId() {
        return fileSystemId;
    }

    public long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    // now we only call incrementActiveOperations in updateCachedFileSystem.
    // concurrentHashMap.compute() ensures atomicity,
    // and all "brokerFileSystem object" that call this method must be in "FileSystemManager.cachedFileSystem",
    // so there is no need to lock it
    public void incrementActiveOperations() { activeOperationCount.incrementAndGet(); }
    public void decrementActiveOperations() { activeOperationCount.decrementAndGet(); }
    public int  getActiveOperationsCount() { return activeOperationCount.get(); }

    public void incrementActiveStreams() { activeStreamCount.incrementAndGet(); }
    public void decrementActiveStreams() { activeStreamCount.decrementAndGet(); }
    public int  getActiveStreamCount() { return activeStreamCount.get(); }

    public boolean isExpiredByLastAccessTime() {
        return System.currentTimeMillis() - lastAccessTimestamp > BrokerConfig.client_expire_seconds * 1000L;
    }

    @Override
    public String toString() {
        return "BrokerFileSystem [identity=" + identity + ", dfsFileSystem="
                + dfsFileSystem + ", fileSystemId=" + fileSystemId + "]";
    }
}
