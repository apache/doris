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

import com.google.common.collect.Maps;
import org.apache.doris.alter.AlterLightSchChangeHelper;
import org.apache.doris.common.util.MasterDaemon;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * note(tsy): this class is temporary, make table before 1.2 to enable light schema change
 */
public class ColumnIdFlushDaemon extends MasterDaemon {

    /**
     * db id -> (tbl_id->status)
     */
    private Map<Long, Map<Long, FlushStatus>> resultCollector;

    public ColumnIdFlushDaemon() {
        super("colum-id-flusher", TimeUnit.HOURS.toMillis(1));
        resultCollector = Maps.newHashMap();
    }

    @Override
    protected void runAfterCatalogReady() {
        flush();
    }

    private void flush() {
        List<Database> dbs = Env.getCurrentEnv().getInternalCatalog().getDbs();
        for (Database db : dbs) {
            db.getTables()
                    .stream()
                    .filter(table -> table instanceof OlapTable)
                    .map(table -> (OlapTable) table)
                    .filter(olapTable -> !olapTable.getTableProperty().getUseSchemaLightChange())
                    .forEach(table -> {
                        try {
                            table.writeLock();
                            if (table.getTableProperty().getUseSchemaLightChange()) {
                                table.writeUnlock();
                                return;
                            }
                            new AlterLightSchChangeHelper(db, table).enableLightSchemaChange();
                            table.writeUnlock();
                            recordResult(db.getId(), table.getId(), FlushStatus.ok());
                        } catch (IllegalStateException e) {
                            recordResult(db.getId(), table.getId(), FlushStatus.failed(e.getMessage()));
                        }
                    });
        }
    }

    private void recordResult(long dbId, long tableId, FlushStatus status) {
        resultCollector.putIfAbsent(dbId, Maps.newHashMap());
        Map<Long, FlushStatus> tableToStatus = resultCollector.get(dbId);
        tableToStatus.put(tableId, status);
    }

    private static class FlushStatus {

        private FlushStatus() {
            this.success = true;
            this.msg = "OK";
        }

        private FlushStatus(String msg) {
            this.success = false;
            this.msg = msg;
        }

        public static FlushStatus ok() {
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
