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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DbUsedDataQuotaInfoCollector extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DbUsedDataQuotaInfoCollector.class);

    public DbUsedDataQuotaInfoCollector() {
        super("DbUsedDataQuotaInfoCollector", Config.db_used_data_quota_update_interval_secs * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        updateAllDatabaseUsedDataQuota();
    }

    private void updateAllDatabaseUsedDataQuota() {
        Env env = Env.getCurrentEnv();
        List<Long> dbIdList = env.getInternalCatalog().getDbIds();
        GlobalTransactionMgrIface globalTransactionMgr = env.getGlobalTransactionMgr();
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
                long usedDataQuotaBytes = db.getUsedDataQuotaWithLock();
                globalTransactionMgr.updateDatabaseUsedQuotaData(dbId, usedDataQuotaBytes);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update database[{}] used data quota bytes : {}.", db.getFullName(), usedDataQuotaBytes);
                }
            } catch (AnalysisException e) {
                LOG.warn("Update database[" + db.getFullName() + "] used data quota failed", e);
            }
        }
    }
}
