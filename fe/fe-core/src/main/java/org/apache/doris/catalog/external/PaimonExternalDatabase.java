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
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonExternalDatabase extends ExternalDatabase<PaimonExternalTable> implements GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalDatabase.class);
    // Cache of table name to table id.
    private Map<String, Long> tableNameToId = Maps.newConcurrentMap();
    @SerializedName(value = "idToTbl")
    private Map<Long, PaimonExternalTable> idToTbl = Maps.newConcurrentMap();

    public PaimonExternalDatabase(ExternalCatalog extCatalog, Long id, String name) {
        super(extCatalog, id, name, InitDatabaseLog.Type.PAIMON);
    }

    @Override
    protected PaimonExternalTable getExternalTable(String tableName, long tblId, ExternalCatalog catalog) {
        return new PaimonExternalTable(tblId, tableName, name, (PaimonExternalCatalog) extCatalog);
    }

    public void replayInitDb(InitDatabaseLog log, ExternalCatalog catalog) {
        Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
        Map<Long, PaimonExternalTable> tmpIdToTbl = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            PaimonExternalTable table = getTableForReplay(log.getRefreshTableIds().get(i));
            tmpTableNameToId.put(table.getName(), table.getId());
            tmpIdToTbl.put(table.getId(), table);
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            PaimonExternalTable table = new PaimonExternalTable(log.getCreateTableIds().get(i),
                    log.getCreateTableNames().get(i), name, (PaimonExternalCatalog) catalog);
            tmpTableNameToId.put(table.getName(), table.getId());
            tmpIdToTbl.put(table.getId(), table);
        }
        tableNameToId = tmpTableNameToId;
        idToTbl = tmpIdToTbl;
        initialized = true;
    }

    public void setTableExtCatalog(ExternalCatalog extCatalog) {
        for (PaimonExternalTable table : idToTbl.values()) {
            table.setCatalog(extCatalog);
        }
    }

    @Override
    public List<PaimonExternalTable> getTablesOnIdOrder() {
        // Sort the name instead, because the id may change.
        return getTables().stream().sorted(Comparator.comparing(TableIf::getName)).collect(Collectors.toList());
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
    public void createTable(String tableName, long tableId) {
        LOG.debug("create table [{}]", tableName);
        makeSureInitialized();
        tableNameToId.put(tableName, tableId);
        PaimonExternalTable table = new PaimonExternalTable(tableId, tableName, name,
                (PaimonExternalCatalog) extCatalog);
        idToTbl.put(tableId, table);
    }
}
