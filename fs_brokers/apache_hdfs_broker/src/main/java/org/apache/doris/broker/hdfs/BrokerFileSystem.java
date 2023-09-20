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

import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public class BrokerFileSystem {

    private static Logger logger = Logger
            .getLogger(BrokerFileSystem.class.getName());

    private ReentrantLock lock;
    private FileSystemIdentity identity;
    private FileSystem dfsFileSystem;
    private volatile long lastAccessTimestamp;
    private long createTimestamp;
    private UUID fileSystemId;

    public BrokerFileSystem(FileSystemIdentity identity) {
        this.identity = identity;
        this.lock = new ReentrantLock();
        this.dfsFileSystem = null;
        this.lastAccessTimestamp = System.currentTimeMillis();
        this.createTimestamp = System.currentTimeMillis();
        this.fileSystemId = UUID.randomUUID();
    }

    public synchronized void setFileSystem(FileSystem fileSystem) {
        this.dfsFileSystem = fileSystem;
        this.lastAccessTimestamp = System.currentTimeMillis();
        this.createTimestamp = System.currentTimeMillis();
    }

    public void closeFileSystem() {
        lock.lock();
        try {
            if (this.dfsFileSystem != null) {
                try {
                    // do not close file system, it will be closed automatically.
                    // this.dfsFileSystem.close();
                } catch (Exception e) {
                    logger.error("errors while close file system", e);
                } finally {
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

    public boolean isExpiredByLastAccessTime(long expirationIntervalSecs) {
        return System.currentTimeMillis() - lastAccessTimestamp > expirationIntervalSecs * 1000;
    }

    public boolean isExpiredByCreateTime(long expirationIntervalSecs) {
        return System.currentTimeMillis() - createTimestamp > expirationIntervalSecs * 1000;
    }

    @Override
    public String toString() {
        return "BrokerFileSystem [identity=" + identity + ", dfsFileSystem="
                + dfsFileSystem + ", fileSystemId=" + fileSystemId + "]";
    }
}
