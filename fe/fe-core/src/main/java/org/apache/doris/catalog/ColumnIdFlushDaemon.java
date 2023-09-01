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

import org.apache.doris.alter.AlterLightSchChangeHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.AlterLightSchemaChangeInfo;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

/**
 * note(tsy): this class is temporary, make table before 1.2 to enable light schema change
 */
public class ColumnIdFlushDaemon extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(ColumnIdFlushDaemon.class);

    /**
     * db name -> (tbl name -> status)
     */
    private final Map<String, Map<String, FlushStatus>> resultCollector;

    private final ReadWriteLock rwLock;

    private final BiConsumer<Database, OlapTable> flushFunc;

    public ColumnIdFlushDaemon() {
        super("colum-id-flusher", TimeUnit.HOURS.toMillis(1));
        resultCollector = Maps.newHashMap();
        rwLock = new ReentrantReadWriteLock();
        if (Config.enable_convert_light_weight_schema_change) {
            flushFunc = this::doFlush;
        } else {
            flushFunc = (db, table) -> record(db.getFullName(), table.getName(), FlushStatus.init());
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        flush();
    }

    private void flush() {
        List<Database> dbs = Env.getCurrentEnv().getInternalCatalog().getDbs();
        for (Database db : dbs) {
            rwLock.writeLock().lock();
            try {
                db.getTables()
                        .stream()
                        .filter(table -> table instanceof OlapTable)
                        .map(table -> (OlapTable) table)
                        .filter(olapTable -> !olapTable.getTableProperty().getUseSchemaLightChange())
                        .forEach(table -> flushFunc.accept(db, table));
            } finally {
                rwLock.writeLock().unlock();
            }
            try {
                // avoid too often to call be
                sleep(3000);
            } catch (InterruptedException ignore) {
                // do nothing
            }
        }
    }

    private void doFlush(Database db, OlapTable table) {
        record(db.getFullName(), table.getName(), FlushStatus.init());
        AlterLightSchChangeHelper schChangeHelper = new AlterLightSchChangeHelper(db, table);
        AlterLightSchemaChangeInfo changeInfo;
        try {
            changeInfo = schChangeHelper.callForColumnsInfo();
        } catch (IllegalStateException e) {
            record(db.getFullName(), table.getName(), FlushStatus.failed(e.getMessage()));
            return;
        }
        table.writeLock();
        try {
            if (table.getTableProperty().getUseSchemaLightChange()) {
                removeRecord(db.getFullName(), table.getName());
                return;
            }
            schChangeHelper.updateTableMeta(changeInfo);
            Env.getCurrentEnv().getEditLog().logAlterLightSchemaChange(changeInfo);
            LOG.info("successfully enable `light_schema_change`, db={}, tbl={}",
                    db.getFullName(), table.getName());
            removeRecord(db.getFullName(), table.getName());
        } catch (IllegalStateException e) {
            record(db.getFullName(), table.getName(), FlushStatus.failed(e.getMessage()));
        } finally {
            table.writeUnlock();
        }
    }

    private void record(String dbName, String tableName, FlushStatus status) {
        resultCollector.putIfAbsent(dbName, Maps.newHashMap());
        Map<String, FlushStatus> tableToStatus = resultCollector.get(dbName);
        tableToStatus.put(tableName, status);
    }

    private void removeRecord(String dbName, String tableName) {
        Map<String, FlushStatus> tableToStatus;
        if (resultCollector.containsKey(dbName)
                && (tableToStatus = resultCollector.get(dbName)).containsKey(tableName)) {
            tableToStatus.remove(tableName);
            if (tableToStatus.isEmpty()) {
                resultCollector.remove(dbName);
            }
        }
    }

    public Map<String, Map<String, FlushStatus>> getResultCollector() {
        return resultCollector;
    }

    public void readLock() {
        rwLock.readLock().lock();
    }

    public void readUnlock() {
        rwLock.readLock().unlock();
    }

    public static class FlushStatus {

        private FlushStatus() {
            this.success = true;
            this.msg = "Waiting to be converted";
        }

        private FlushStatus(String msg) {
            this.success = false;
            this.msg = msg;
        }

        public static FlushStatus init() {
            return new FlushStatus();
        }

        public static FlushStatus failed(String reason) {
            return new FlushStatus(reason);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMsg() {
            return msg;
        }

        private final boolean success;

        private final String msg;
    }
}
