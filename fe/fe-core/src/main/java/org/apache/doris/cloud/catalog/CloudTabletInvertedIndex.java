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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CloudTabletInvertedIndex extends TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(CloudTabletInvertedIndex.class);

    // tablet id -> replica
    // for cloud mode, no need to know the replica's backend
    private Map<Long, Replica> replicaMetaMap = Maps.newHashMap();

    public CloudTabletInvertedIndex() {
        super();
    }

    @Override
    public List<Replica> getReplicas(Long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaMap.containsKey(tabletId)) {
                return Collections.singletonList(replicaMetaMap.get(tabletId));
            }
            return Collections.emptyList();
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    public void deleteTablet(long tabletId) {
        long stamp = writeLock();
        try {
            replicaMetaMap.remove(tabletId);
            tabletMetaMap.remove(tabletId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("delete tablet: {}", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void addReplica(long tabletId, Replica replica) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, replica " + replica.getId());
            replicaMetaMap.put(tabletId, replica);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add replica {} of tablet {}", replica.getId(), tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public void deleteReplica(long tabletId, long backendId) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), "tablet " + tabletId + " not exists");
            if (replicaMetaMap.containsKey(tabletId)) {
                Replica replica = replicaMetaMap.remove(tabletId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete replica {} of tablet {}", replica.getId(), tabletId);
                }
            } else {
                // this may happen when fe restart after tablet is empty(bug cause)
                // add log instead of assertion to observe
                LOG.error("tablet[{}] contains no replica in inverted index", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    @Override
    public Replica getReplica(long tabletId, long backendId) {
        long stamp = readLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), "tablet " + tabletId + " not exists");
            return replicaMetaMap.get(tabletId);
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    public List<Replica> getReplicasByTabletId(long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaMap.containsKey(tabletId)) {
                return Collections.singletonList(replicaMetaMap.get(tabletId));
            }
            return Collections.emptyList();
        } finally {
            readUnlock(stamp);
        }
    }

    @Override
    protected void innerClear() {
        replicaMetaMap.clear();
    }
}
