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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.datasource.JdbcExternalCatalog;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JdbcExternalDatabase extends ExternalDatabase<JdbcExternalTable> implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalDatabase.class);

    // Cache of table name to table id.
    private Map<String, Long> tableNameToId = Maps.newConcurrentMap();
    @SerializedName(value = "idToTbl")
    private Map<Long, JdbcExternalTable> idToTbl = Maps.newConcurrentMap();

    /**
     * Create Jdbc external database.
     *
     * @param extCatalog External data source this database belongs to.
     * @param id database id.
     * @param name database name.
     */
    public JdbcExternalDatabase(ExternalCatalog extCatalog, long id, String name) {
        super(extCatalog, id, name);
    }

    // TODO(ftw): drew out the public multiple parts
    @Override
    protected void init() {
        InitDatabaseLog initDatabaseLog = new InitDatabaseLog();
        initDatabaseLog.setType(InitDatabaseLog.Type.JDBC);
        initDatabaseLog.setCatalogId(extCatalog.getId());
        initDatabaseLog.setDbId(id);
        List<String> tableNames = extCatalog.listTableNames(null, name);
        if (tableNames != null) {
            Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
            Map<Long, JdbcExternalTable> tmpIdToTbl = Maps.newHashMap();
            for (String tableName : tableNames) {
                long tblId;
                if (tableNameToId != null && tableNameToId.containsKey(tableName)) {
                    tblId = tableNameToId.get(tableName);
                    tmpTableNameToId.put(tableName, tblId);
                    JdbcExternalTable table = idToTbl.get(tblId);
                    tmpIdToTbl.put(tblId, table);
                    initDatabaseLog.addRefreshTable(tblId);
                } else {
                    tblId = Env.getCurrentEnv().getNextId();
                    tmpTableNameToId.put(tableName, tblId);
                    JdbcExternalTable table = new JdbcExternalTable(tblId, tableName, name,
                                                                    (JdbcExternalCatalog) extCatalog);
                    tmpIdToTbl.put(tblId, table);
                    initDatabaseLog.addCreateTable(tblId, tableName);
                }
            }
            tableNameToId = tmpTableNameToId;
            idToTbl = tmpIdToTbl;
        }
        initialized = true;
        Env.getCurrentEnv().getEditLog().logInitExternalDb(initDatabaseLog);
    }

    public void setTableExtCatalog(ExternalCatalog extCatalog) {
        for (JdbcExternalTable table : idToTbl.values()) {
            table.setCatalog(extCatalog);
        }
    }

    public void replayInitDb(InitDatabaseLog log, ExternalCatalog catalog) {
        Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
        Map<Long, JdbcExternalTable> tmpIdToTbl = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            JdbcExternalTable table = getTableForReplay(log.getRefreshTableIds().get(i));
            tmpTableNameToId.put(table.getName(), table.getId());
            tmpIdToTbl.put(table.getId(), table);
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            JdbcExternalTable table = new JdbcExternalTable(log.getCreateTableIds().get(i),
                    log.getCreateTableNames().get(i), name, (JdbcExternalCatalog) catalog);
            tmpTableNameToId.put(table.getName(), table.getId());
            tmpIdToTbl.put(table.getId(), table);
        }
        tableNameToId = tmpTableNameToId;
        idToTbl = tmpIdToTbl;
        initialized = true;
    }

    // TODO(ftw): drew
    @Override
    public Set<String> getTableNamesWithLock() {
        makeSureInitialized();
        return Sets.newHashSet(tableNameToId.keySet());
    }

    @Override
    public List<JdbcExternalTable> getTables() {
        makeSureInitialized();
        return Lists.newArrayList(idToTbl.values());
    }

    // TODO(ftw): drew
    @Override
    public JdbcExternalTable getTableNullable(String tableName) {
        makeSureInitialized();
        if (!tableNameToId.containsKey(tableName)) {
            return null;
        }
        return idToTbl.get(tableNameToId.get(tableName));
    }

    @Override
    public JdbcExternalTable getTableNullable(long tableId) {
        makeSureInitialized();
        return idToTbl.get(tableId);
    }

    public JdbcExternalTable getTableForReplay(long tableId) {
        return idToTbl.get(tableId);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        tableNameToId = Maps.newConcurrentMap();
        for (JdbcExternalTable tbl : idToTbl.values()) {
            tableNameToId.put(tbl.getName(), tbl.getId());
        }
        rwLock = new ReentrantReadWriteLock(true);
    }

    public void addTableForTest(JdbcExternalTable tbl) {
        idToTbl.put(tbl.getId(), tbl);
        tableNameToId.put(tbl.getName(), tbl.getId());
    }
}
