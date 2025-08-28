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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.Util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

/*
 * Delete temporary table when its creating session is gone
 */
public class TemporaryTableMgr extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(TemporaryTableMgr.class);

    public TemporaryTableMgr() {
        super("temporary-table-mgr");
    }

    @Override
    protected void runAfterCatalogReady() {
        Map<String, Long> sessionReportTimeMap = Env.getCurrentEnv().getSessionReportTimeMap();
        long currentTs = System.currentTimeMillis();
        Collection<DatabaseIf<? extends TableIf>> internalDBs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        for (DatabaseIf<? extends TableIf> db : internalDBs) {
            for (TableIf table : db.getTables()) {
                if (!table.isTemporary()) {
                    continue;
                }

                String sessionId = Util.getTempTableSessionId(table.getName());
                boolean needDelete = false;
                if (!sessionReportTimeMap.containsKey(sessionId)) {
                    LOG.info("Cannot find session id for table " + table.getName());
                    needDelete = true;
                } else if (currentTs > sessionReportTimeMap.get(sessionId)
                        + Config.loss_conn_fe_temp_table_keep_second * 1000) {
                    LOG.info("Temporary table " + table.getName() + " is out of time: "
                            + new Date(sessionReportTimeMap.get(sessionId)) + ", current: " + new Date(currentTs));
                    needDelete = true;
                }

                if (needDelete) {
                    LOG.info("Drop temporary table " + table);
                    try {
                        Env.getCurrentEnv().getInternalCatalog()
                            .dropTableWithoutCheck((Database) db, (Table) table, false, true);
                    } catch (Exception e) {
                        LOG.error("Drop temporary table error: db: {}, table: {}",
                                db.getFullName(), table.getName(), e);
                    }
                }
            }
        }
    }
}
