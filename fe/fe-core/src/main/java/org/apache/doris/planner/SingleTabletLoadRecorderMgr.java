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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class SingleTabletLoadRecorderMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(SingleTabletLoadRecorderMgr.class);
    private static final long EXPIRY_TIME_INTERVAL_MS = 604800000; // 7 * 24 * 60 * 60 * 1000, 7 days

    // <db_id -> <table_id -> <partition_id -> load_tablet_record>>>
    // 0 =< load_tablet_index < number_buckets
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, TabletUpdateRecord>>>
            loadTabletRecordMap = new ConcurrentHashMap<>();

    public SingleTabletLoadRecorderMgr() {
        super("single_tablet_load_recorder", EXPIRY_TIME_INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        long expiryTime = System.currentTimeMillis() - EXPIRY_TIME_INTERVAL_MS;
        loadTabletRecordMap.forEach((dbId, dbTableMap) -> {
            dbTableMap.forEach((tableId, partitionMap) -> {
                partitionMap.entrySet().removeIf(entry -> entry.getValue().getUpdateTimestamp() < expiryTime);
                if (partitionMap.isEmpty()) {
                    dbTableMap.remove(tableId);
                }
            });
            if (dbTableMap.isEmpty()) {
                loadTabletRecordMap.remove(dbId);
            }
        });
        LOG.info("Remove expired load tablet record successfully.");
    }

    public int getCurrentLoadTabletIndex(long dbId, long tableId, long partitionId) throws UserException {
        TabletUpdateRecord record = loadTabletRecordMap.computeIfAbsent(dbId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tableId, k -> new ConcurrentHashMap<>())
                .get(partitionId);
        int numBuckets = -1;
        if (record == null) {
            numBuckets = getNumBuckets(dbId, tableId, partitionId);
        }
        return createOrUpdateLoadTabletRecord(dbId, tableId, partitionId, numBuckets);
    }

    private int getNumBuckets(long dbId, long tableId, long partitionId) throws UserException {
        OlapTable olapTable = (OlapTable) Env.getCurrentInternalCatalog().getDb(dbId)
                .flatMap(db -> db.getTable(tableId)).filter(t -> t.getType() == TableIf.TableType.OLAP)
                .orElse(null);
        if (olapTable == null) {
            throw new UserException("db[" + dbId + "], table[" + tableId + "] is not exist.");
        }
        return olapTable.getPartition(partitionId)
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)
                .get(0).getTablets().size();
    }

    private int createOrUpdateLoadTabletRecord(long dbId, long tableId, long partitionId, int numBuckets) {
        TabletUpdateRecord record = loadTabletRecordMap.computeIfAbsent(dbId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tableId, k -> new ConcurrentHashMap<>())
                .compute(partitionId, (k, existingRecord) -> {
                    if (existingRecord == null) {
                        return new TabletUpdateRecord(0, numBuckets);
                    } else {
                        existingRecord.updateRecord();
                        return existingRecord;
                    }
                });
        return record.getTabletIndex();
    }

    static class TabletUpdateRecord {
        @Getter
        // 0 =< load_tablet_index < number_buckets
        int tabletIndex;
        int numBuckets;
        @Getter
        long updateTimestamp = System.currentTimeMillis();

        TabletUpdateRecord(int tabletIndex, int numBuckets) {
            this.tabletIndex = tabletIndex;
            this.numBuckets = numBuckets;
        }

        public synchronized void updateRecord() {
            this.tabletIndex = this.tabletIndex + 1 >= numBuckets ? 0 : this.tabletIndex + 1;
            this.updateTimestamp = System.currentTimeMillis();
        }

    }
}
