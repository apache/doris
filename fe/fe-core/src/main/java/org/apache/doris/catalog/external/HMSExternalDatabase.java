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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.InitDatabaseLog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hive metastore external database.
 */
public class HMSExternalDatabase extends ExternalDatabase<HMSExternalTable> {
    private static final Logger LOG = LogManager.getLogger(HMSExternalDatabase.class);

    /**
     * Create HMS external database.
     *
     * @param extCatalog External catalog this database belongs to.
     * @param id database id.
     * @param name database name.
     */
    public HMSExternalDatabase(ExternalCatalog extCatalog, long id, String name) {
        super(extCatalog, id, name, InitDatabaseLog.Type.HMS);
    }

    @Override
    protected HMSExternalTable getExternalTable(String tableName, long tblId, ExternalCatalog catalog) {
        return new HMSExternalTable(tblId, tableName, name, (HMSExternalCatalog) extCatalog);
    }

    @Override
    public List<HMSExternalTable> getTablesOnIdOrder() {
        // Sort the name instead, because the id may change.
        return getTables().stream().sorted(Comparator.comparing(TableIf::getName)).collect(Collectors.toList());
    }

    public void addTableForTest(HMSExternalTable tbl) {
        idToTbl.put(tbl.getId(), tbl);
        tableNameToId.put(tbl.getName(), tbl.getId());
    }

    @Override
    public void dropTable(String tableName) {
        LOG.debug("drop table [{}]", tableName);
        makeSureInitialized();
        Long tableId = tableNameToId.remove(tableName);
        if (tableId == null) {
            LOG.warn("drop table [{}] failed", tableName);
        }
        idToTbl.remove(tableId);
    }

    @Override
    public void dropTableForReplay(String tableName) {
        LOG.debug("replayDropTableFromEvent [{}]", tableName);
        Long tableId = tableNameToId.remove(tableName);
        if (tableId == null) {
            LOG.warn("replayDropTableFromEvent [{}] failed", tableName);
            return;
        }
        idToTbl.remove(tableId);
    }

    @Override
    public void createTableForReplay(String tableName, long tableId) {
        LOG.debug("create table [{}]", tableName);
        tableNameToId.put(tableName, tableId);
        HMSExternalTable table = getExternalTable(tableName, tableId, extCatalog);
        idToTbl.put(tableId, table);
    }
}
