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

package org.apache.doris.master;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PartitionInfoCollector extends MasterDaemon {

    // notice since collect partition info every Config.partition_info_update_interval_secs seconds,
    // so partition collect info may be stale
    public static class PartitionCollectInfo {
        private long visibleVersion;
        private boolean isInMemory;

        PartitionCollectInfo(long visibleVersion, boolean isInMemory) {
            this.visibleVersion = visibleVersion;
            this.isInMemory = isInMemory;
        }

        public long getVisibleVersion() {
            return this.visibleVersion;
        }

        public boolean isInMemory() {
            return this.isInMemory;
        }
    }

    private static final Logger LOG = LogManager.getLogger(PartitionInfoCollector.class);

    public PartitionInfoCollector() {
        super("PartitionInfoCollector", Config.partition_info_update_interval_secs * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        updatePartitionCollectInfo();
    }

    private void updatePartitionCollectInfo() {
        Env env = Env.getCurrentEnv();
        TabletInvertedIndex tabletInvertedIndex = env.getTabletInvertedIndex();
        ImmutableMap.Builder builder = ImmutableMap.builder();
        List<Long> dbIdList = env.getInternalCatalog().getDbIds();
        for (Long dbId : dbIdList) {
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("Database [" + dbId + "] does not exist, skip to update database used data quota");
                continue;
            }
            if (db instanceof MysqlCompatibleDatabase) {
                continue;
            }
            try {
                int partitionInMemoryCount = 0;
                for (Table table : db.getTables()) {
                    if (table.getType() != Table.TableType.OLAP) {
                        continue;
                    }
                    table.readLock();
                    try {
                        OlapTable olapTable = (OlapTable) table;
                        for (Partition partition : olapTable.getAllPartitions()) {
                            boolean isInMemory = olapTable.getPartitionInfo().getIsInMemory(partition.getId());
                            if (isInMemory) {
                                partitionInMemoryCount++;
                            }
                            builder.put(partition.getId(),
                                    new PartitionCollectInfo(partition.getVisibleVersion(), isInMemory));
                        }
                    } finally {
                        table.readUnlock();
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update database[{}] partition in memory info, partitionInMemoryCount : {}.",
                            db.getFullName(), partitionInMemoryCount);
                }
            } catch (Exception e) {
                LOG.warn("Update database[" + db.getFullName() + "] partition in memory info failed", e);
            }
        }
        tabletInvertedIndex.setPartitionCollectInfoMap(builder.build());
    }
}
