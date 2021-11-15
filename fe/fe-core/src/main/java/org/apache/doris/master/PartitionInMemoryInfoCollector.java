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

import com.google.common.collect.ImmutableSet;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PartitionInMemoryInfoCollector extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(PartitionInMemoryInfoCollector.class);

    public PartitionInMemoryInfoCollector() {
        super("PartitionInMemoryInfoCollector", Config.partition_in_memory_update_interval_secs * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        updatePartitionInMemoryInfo();
    }

    private void updatePartitionInMemoryInfo() {
        Catalog catalog = Catalog.getCurrentCatalog();
        TabletInvertedIndex tabletInvertedIndex = catalog.getTabletInvertedIndex();
        ImmutableSet.Builder builder = ImmutableSet.builder();
        List<Long> dbIdList = catalog.getDbIds();
        for (Long dbId : dbIdList) {
            Database db = catalog.getDbNullable(dbId);
            if (db == null) {
                LOG.warn("Database [" + dbId + "] does not exist, skip to update database used data quota");
                continue;
            }
            if (db.isInfoSchemaDb()) {
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
                            if (olapTable.getPartitionInfo().getIsInMemory(partition.getId())) {
                                partitionInMemoryCount++;
                                builder.add(partition.getId());
                            }
                        }
                    } finally {
                        table.readUnlock();
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update database[{}] partition in memory info, partitionInMemoryCount : {}.", db.getFullName(), partitionInMemoryCount);
                }
            } catch (Exception e) {
                LOG.warn("Update database[" + db.getFullName() + "] partition in memory info failed", e);
            }
        }
        ImmutableSet<Long> partitionIdInMemorySet = builder.build();
        tabletInvertedIndex.setPartitionIdInMemorySet(partitionIdInMemorySet);
    }


}
