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

package org.apache.doris.planner;

import org.apache.doris.catalog.Partition;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;

public class TabletLoadIndexRecorderMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletLoadIndexRecorderMgr.class);
    private static final long TABLET_LOAD_INDEX_KEEP_MAX_TIME_MS = 86400000; // 1 * 24 * 60 * 60 * 1000, 1 days
    private static final long TABLET_LOAD_INDEX_EXPIRE_CHECK_INTERVAL_MS = 3600000; // 1 hour
    private static final int TIMES_FOR_UPDATE_TIMESTAMP = 1000;
    private static final SecureRandom RANDOM = new SecureRandom();

    // <<db_id, table_id, partition_id> -> load_tablet_record>
    // 0 =< load_tablet_index < number_buckets
    private final ConcurrentHashMap<Triple<Long, Long, Long>, TabletLoadIndexRecord> loadTabletRecordMap =
            new ConcurrentHashMap<>();

    public TabletLoadIndexRecorderMgr() {
        super("tablet_load_index_recorder", TABLET_LOAD_INDEX_EXPIRE_CHECK_INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        int originRecordSize = loadTabletRecordMap.size();
        long expireTime = System.currentTimeMillis() - TABLET_LOAD_INDEX_KEEP_MAX_TIME_MS;
        loadTabletRecordMap.entrySet().removeIf(entry ->
                entry.getValue().getUpdateTimestamp() < expireTime
        );
        int currentRecordSize = loadTabletRecordMap.size();
        LOG.info("Remove expired load tablet index record successfully, before {}, current {}",
                originRecordSize, currentRecordSize);
    }

    public int getCurrentTabletLoadIndex(long dbId, long tableId, Partition partition) throws UserException {
        Triple<Long, Long, Long> key = Triple.of(dbId, tableId, partition.getId());
        return loadTabletRecordMap.compute(key, (k, existingRecord) ->
                existingRecord == null ? new TabletLoadIndexRecord(RANDOM.nextInt(
                    partition.getDistributionInfo().getBucketNum()),
                    partition.getDistributionInfo().getBucketNum()) : existingRecord).getAndIncrement();
    }

    static class TabletLoadIndexRecord {
        int loadIndex;
        int numBuckets;
        @Getter
        long updateTimestamp = System.currentTimeMillis();

        TabletLoadIndexRecord(long initialIndex, int numBuckets) {
            this.loadIndex = (int) (initialIndex % numBuckets);
            this.numBuckets = numBuckets;
        }

        public synchronized int getAndIncrement() {
            int tabletLoadIndex = loadIndex % numBuckets;
            loadIndex++;
            // To reduce the compute time cost, only update timestamp when load index is
            // greater than or equal to both TIMES_FOR_UPDATE_TIMESTAMP and numBuckets
            if (loadIndex >= Math.max(TIMES_FOR_UPDATE_TIMESTAMP, numBuckets)) {
                loadIndex = loadIndex % numBuckets;
                this.updateTimestamp = System.currentTimeMillis();
            }
            return tabletLoadIndex;
        }

    }
}
