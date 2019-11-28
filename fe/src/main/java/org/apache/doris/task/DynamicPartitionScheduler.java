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

package org.apache.doris.task;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class DynamicPartitionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionScheduler.class);

    private static Set<Pair<Long, Long>> dynamicPartitionTableInfo = new HashSet<>();
    private boolean initialize;

    public DynamicPartitionScheduler() {
        this.initialize = false;
    }
    public static void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(new Pair<>(dbId, tableId));
    }

    public static void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
    }

    public static Set<Pair<Long, Long>> getDynamicPartitionTableInfo() {
        return dynamicPartitionTableInfo;
    }

    private void initDynamicPartitionTable() {
        for (Long dbId : Catalog.getInstance().getDbIds()) {
            Database db = Catalog.getInstance().getDb(dbId);
            for (Table table : Catalog.getInstance().getDb(dbId).getTables()) {
                if (TableProperty.checkDynamicPartitionTable(table)) {
                    registerDynamicPartitionTable(db.getId(), table.getId());
                }
            }
        }
        initialize = true;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!initialize) {
            // check Dynamic Partition tables only when FE start
            initDynamicPartitionTable();
        }
        try {
            TableProperty.dynamicAddPartition();
        } catch (DdlException e) {
            LOG.warn("dynamic add partition failed: " + e.getMessage());
        }
    }
}
