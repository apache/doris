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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class CloudUpgradeMgr extends MasterDaemon {

    @Getter
    @Setter
    @RequiredArgsConstructor
    private static class DbWithWaterTxn {
        private final Long dbId;
        private final Long txnId;
    }

    @Getter
    @Setter
    @RequiredArgsConstructor
    private static class DbWithWaterTxnInBe {
        private final LinkedBlockingQueue<DbWithWaterTxn> dbWithWaterTxnQueue;
        private final Long beId;

    }

    private static final Logger LOG = LogManager.getLogger(CloudUpgradeMgr.class);
    /*   (<(<dbid1, txn1>, <dbid2, txn2>, ... <dbid3, txn3>), be>, <...>, ...) */
    private final LinkedBlockingQueue<DbWithWaterTxnInBe> txnBePairList = new LinkedBlockingQueue<>();

    private final CloudSystemInfoService cloudSystemInfoService;

    public CloudUpgradeMgr(CloudSystemInfoService cloudSystemInfoService) {
        super("cloud upgrade manager", Config.cloud_upgrade_mgr_interval_second * 1000L);
        this.cloudSystemInfoService = cloudSystemInfoService;
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("before cloud upgrade mgr txnBePairList size {}", txnBePairList.size());
        for (DbWithWaterTxnInBe txnBe : txnBePairList) {
            LinkedBlockingQueue<DbWithWaterTxn> txnList = txnBe.dbWithWaterTxnQueue;
            long be = txnBe.beId;

            boolean isFinished = false;
            boolean isBeInactive = true;
            for (DbWithWaterTxn dbWithWaterTxn : txnList) {
                List<Long> tableIdList = getAllTables();
                if (tableIdList.isEmpty()) {
                    /* no table in this cluster */
                    break;
                }
                try {
                    if (Config.enable_cloud_running_txn_check) {
                        isFinished = ((CloudGlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr())
                                .isPreviousNonTimeoutTxnFinished(dbWithWaterTxn.txnId,
                                    dbWithWaterTxn.dbId, tableIdList);
                    } else {
                        isFinished = true;
                    }
                } catch (AnalysisException e) {
                    LOG.warn("cloud upgrade mgr exception", e);
                    throw new RuntimeException(e);
                }
                if (!isFinished) {
                    isBeInactive = false;
                    LOG.info("BE {} is still active, waiting db {} txn {}",
                            be, dbWithWaterTxn.dbId, dbWithWaterTxn.txnId);
                    break;
                }
            }
            if (isBeInactive) {
                setBeStateInactive(be);
                LOG.info("BE {} is inactive", be);
                txnBePairList.remove(txnBe);
            }
        }
        LOG.info("finish cloud upgrade mgr");
    }

    /* called after tablets migrating to new BE process complete */
    public void registerWaterShedTxnId(long be) throws UserException {
        LinkedBlockingQueue<DbWithWaterTxn> txnIds = new LinkedBlockingQueue<>();
        List<Long> dbids = Env.getCurrentInternalCatalog().getDbIds();
        Long nextTransactionId = Env.getCurrentGlobalTransactionMgr().getNextTransactionId();
        for (long dbid : dbids) {
            txnIds.offer(new DbWithWaterTxn(dbid, nextTransactionId));
        }
        txnBePairList.offer(new DbWithWaterTxnInBe(txnIds, be));
        LOG.info("register watershedtxnid {} for BE {}", txnIds.stream()
                .map(dbWithWaterTxn -> "(" + dbWithWaterTxn.dbId + ":" + dbWithWaterTxn.txnId + ")")
                .collect((Collectors.joining(", ", "{", "}"))), be);
    }

    private List<Long> getAllTables() {
        List<Long> mergedList = new ArrayList<>();

        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            db.readLock();
            List<Long> tableList = db.getTableIds();
            db.readUnlock();
            mergedList.addAll(tableList);
        }
        return mergedList;
    }

    public void setBeStateInactive(long beId) {
        Backend be = cloudSystemInfoService.getBackend(beId);
        if (be == null) {
            LOG.warn("cannot get be {} to set inactive state", beId);
            return;
        }
        be.setActive(false); /* now user can get BE inactive status from `show backends;` */
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
        LOG.info("finished to modify backend {} ", be);
    }
}
