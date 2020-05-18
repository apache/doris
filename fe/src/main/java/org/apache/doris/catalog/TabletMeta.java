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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TabletMeta {
    private static final Logger LOG = LogManager.getLogger(TabletMeta.class);

    private final long dbId;
    private final long tableId;
    private final long partitionId;
    private final long indexId;

    private int oldSchemaHash;
    private int newSchemaHash;

    private TStorageMedium storageMedium;

    private ReentrantReadWriteLock lock;

    public TabletMeta(long dbId, long tableId, long partitionId, long indexId, int schemaHash,
            TStorageMedium storageMedium) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;

        this.oldSchemaHash = schemaHash;
        this.newSchemaHash = -1;

        this.storageMedium = storageMedium;

        lock = new ReentrantReadWriteLock();
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    public void setNewSchemaHash(int newSchemaHash) {
        lock.writeLock().lock();
        try {
            Preconditions.checkState(this.newSchemaHash == -1);
            this.newSchemaHash = newSchemaHash;
            LOG.debug("setNewSchemaHash: {}", toString());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateToNewSchemaHash() {
        lock.writeLock().lock();
        try {
            Preconditions.checkState(this.newSchemaHash != -1);
            int tmp = this.oldSchemaHash;
            this.oldSchemaHash = this.newSchemaHash;
            this.newSchemaHash = tmp;
            LOG.debug("updateToNewSchemaHash: " + toString());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deleteNewSchemaHash() {
        lock.writeLock().lock();
        try {
            LOG.debug("deleteNewSchemaHash: " + toString());
            this.newSchemaHash = -1;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int getOldSchemaHash() {
        lock.readLock().lock();
        try {
            return this.oldSchemaHash;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean containsSchemaHash(int schemaHash) {
        lock.readLock().lock();
        try {
            return this.oldSchemaHash == schemaHash || this.newSchemaHash == schemaHash;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("dbId=").append(dbId);
            sb.append(" tableId=").append(tableId);
            sb.append(" partitionId=").append(partitionId);
            sb.append(" indexId=").append(indexId);
            sb.append(" oldSchemaHash=").append(oldSchemaHash);
            sb.append(" newSchemaHash=").append(newSchemaHash);

            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
