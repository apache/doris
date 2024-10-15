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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/
 * show all dbs' info
 */
public class DbsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TableNum").add("Size").add("Quota")
            .add("LastConsistencyCheckTime").add("ReplicaCount").add("ReplicaQuota")
            .add("RunningTransactionNum").add("TransactionQuota").add("LastUpdateTime")
            .build();

    private Env env;
    private CatalogIf catalog;

    public DbsProcDir(Env env, CatalogIf catalog) {
        this.env = env;
        this.catalog = catalog;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        if (env == null || Strings.isNullOrEmpty(dbIdStr)) {
            throw new AnalysisException("Db id is null");
        }

        long dbId = -1L;
        try {
            dbId = Long.valueOf(dbIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }

        DatabaseIf db = catalog.getDbNullable(dbId);
        if (db == null) {
            throw new AnalysisException("Database " + dbId + " does not exist");
        }

        return new TablesProcDir(db);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(env);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<String> dbNames = catalog.getDbNames();
        if (dbNames == null || dbNames.isEmpty()) {
            // empty
            return result;
        }

        // get info
        List<List<Comparable>> dbInfos = new ArrayList<>();
        for (String dbName : dbNames) {
            DatabaseIf db = catalog.getDbNullable(dbName);
            if (db == null) {
                continue;
            }
            List<Comparable> dbInfo = new ArrayList<>();
            db.readLock();
            try {
                int tableNum = db.getTables().size();
                dbInfo.add(db.getId());
                dbInfo.add(dbName);
                dbInfo.add(tableNum);

                long usedDataQuota = (db instanceof Database) ? ((Database) db).getUsedDataQuotaWithLock() : 0;
                long dataQuota = (db instanceof Database) ? ((Database) db).getDataQuota() : 0;
                String readableUsedQuota = DebugUtil.printByteWithUnit(usedDataQuota);
                String readableQuota = DebugUtil.printByteWithUnit(dataQuota);
                String lastCheckTime = (db instanceof Database) ? TimeUtils.longToTimeString(
                        ((Database) db).getLastCheckTime()) : FeConstants.null_string;
                long replicaCount = (db instanceof Database) ? ((Database) db).getReplicaCount() : 0;
                long replicaQuota = (db instanceof Database) ? ((Database) db).getReplicaQuota() : 0;
                long transactionNum =  (db instanceof Database) ? env.getGlobalTransactionMgr()
                        .getRunningTxnNums(db.getId()) : 0;
                long transactionQuota = (db instanceof Database) ? ((Database) db).getTransactionQuotaSize() : 0;
                dbInfo.add(readableUsedQuota);
                dbInfo.add(readableQuota);
                dbInfo.add(lastCheckTime);
                dbInfo.add(replicaCount);
                dbInfo.add(replicaQuota);
                dbInfo.add(transactionNum);
                dbInfo.add(transactionQuota);
                dbInfo.add(TimeUtils.longToTimeString(db.getLastUpdateTime()));
            } finally {
                db.readUnlock();
            }
            dbInfos.add(dbInfo);
        }

        // order by dbId, asc
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(dbInfos, comparator);

        // set result
        for (List<Comparable> info : dbInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }
        return result;
    }
}
