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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Env;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * HiveAcidTransactionMgr is responsible for starting and automatically committing HMS transactions.
 * HiveAcidTransactionMgr should be at the catalog level.
 * <p>
 * beginQueryTransaction() starts the transaction and return HiveAcidTransaction.
 * HiveAcidTransaction.getValidWriteIds() requests table locks.
 * The scanNode needs to call beginQueryTransaction to obtain a HiveAcidTransaction.
 * The scanNode calls the getValidWriteIds method from HiveAcidTransaction to retrieve transaction information.
 * <p>
 * Example expected behavior as follows:
 * SQL1: catalog1.db.a JOIN catalog1.db.b
 *      scanNode1 (catalog1.db.a): beginQueryTransaction()
 *      scanNode2 (catalog1.db.b): beginQueryTransaction()
 *      Only one transaction will be started on HMS, table 'a' needs one lock, table 'b' needs one lock.
 * <p>
 * SQL2: catalog1.db.a JOIN catalog1.db.a
 *      scanNode1 (catalog1.db.a): beginQueryTransaction()
 *      scanNode2 (catalog1.db.a): beginQueryTransaction()
 *      Only one transaction will be started on HMS, table 'a' needs two locks (because locking needs to consider
 * the partitioning of the query, after partition pruning).
 * <p>
 * SQL3: catalog1.db.a JOIN catalog2.db.a
 *      scanNode1 (catalog1.db.a): beginQueryTransaction()
 *      scanNode2 (catalog2.db.a): beginQueryTransaction()
 *      For two different catalogs, even if they end up connecting to the same HMS, we still consider table 'a'
 * as two different tables. So two transactions will be started on HMS, and table 'a' needs two locks.
 */
public class HiveAcidTransactionMgr {
    private static final Logger LOG = LogManager.getLogger(HiveAcidTransactionMgr.class);
    // doris query id => hms transaction id.
    private Map<TUniqueId, Long> queryIdToHmsTxnIdMap = Maps.newConcurrentMap();

    public HiveAcidTransaction beginQueryTransaction(TUniqueId queryId, String user,
            HMSExternalCatalog catalog, HMSExternalTable hiveTable) {
        long hmsTxnId = queryIdToHmsTxnIdMap.computeIfAbsent(
                queryId,
                k -> {
                    long newHmsTxnId = catalog.getClient().openTxn(user);

                    // After the plan fails or the query ends, we need to commit the transaction to release the locks
                    // that were acquired during the plan execution.
                    Env.getCurrentQueryCallBackMgr().registerPlanFailFunc(queryId, () -> {
                        commitQueryTransaction(queryId, newHmsTxnId, catalog);
                    });

                    Env.getCurrentQueryCallBackMgr().registerQueryEndFunc(queryId, () -> {
                        commitQueryTransaction(queryId, newHmsTxnId, catalog);
                    });

                    return newHmsTxnId;
                }
        );
        return new HiveAcidTransaction(queryId, hmsTxnId, user, hiveTable);
    }

    private void commitQueryTransaction(TUniqueId queryId, long hmsTxnId, HMSExternalCatalog catalog) {
        queryIdToHmsTxnIdMap.remove(queryId);
        try {
            catalog.getClient().commitTxn(hmsTxnId);
        } catch (Exception e) {
            //The transaction may still exist on HMS. After a while, HMS will clear the transaction.
            LOG.warn("catalog ={},queryId = {},hmsTxnId = {},commit hms transaction failed",
                    catalog.getName(), queryId, hmsTxnId, e);
        }
    }
}
