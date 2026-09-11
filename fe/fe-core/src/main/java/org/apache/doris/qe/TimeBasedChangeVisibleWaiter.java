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

package org.apache.doris.qe;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TAcquireTimeBasedChangeReadFenceRequest;
import org.apache.doris.thrift.TAcquireTimeBasedChangeReadFenceResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Establishes a closed upper fence before planning a time-based incremental read.
 *
 * <p>The master FE first captures its TSO, validates an explicit end timestamp, then captures a
 * transaction ID watermark and drains earlier transactions involving the target tables. In classic
 * mode, it also synchronizes with transaction publishers through the target table locks and returns
 * a journal watermark for a follower FE to replay. In cloud mode, partition versions are refreshed
 * directly from MetaService after this waiter completes.
 */
public class TimeBasedChangeVisibleWaiter {
    private static final long TXN_POLL_INTERVAL_MS = 100;

    /** Immutable result of establishing the fence on the master FE. */
    public static final class ChangeReadFence {
        private final long currentTso;
        private final long maxJournalId;

        public ChangeReadFence(long currentTso, long maxJournalId) {
            this.currentTso = currentTso;
            this.maxJournalId = maxJournalId;
        }

        public long getCurrentTso() {
            return currentTso;
        }

        public long getMaxJournalId() {
            return maxJournalId;
        }
    }

    @VisibleForTesting
    static final class ChangeReadInfo {
        private final Map<Long, List<Long>> dbToTableIds;
        private final Long maxEndTimestampMs;

        private ChangeReadInfo(Map<Long, List<Long>> dbToTableIds, Long maxEndTimestampMs) {
            this.dbToTableIds = dbToTableIds;
            this.maxEndTimestampMs = maxEndTimestampMs;
        }

        Map<Long, List<Long>> getDbToTableIds() {
            return dbToTableIds;
        }

        Long getMaxEndTimestampMs() {
            return maxEndTimestampMs;
        }
    }

    public static void waitForVisible(ConnectContext context, Plan plan, Map<List<String>, TableIf> tables)
            throws UserException {
        if (tables.isEmpty()) {
            return;
        }
        ChangeReadInfo changeReadInfo = collectChangeReadInfo(context, plan, tables);
        if (changeReadInfo.getDbToTableIds().isEmpty()) {
            return;
        }

        boolean waitForTransactions = !context.getSessionVariable().isEnableEventualConsistentChange();
        // Eventual-consistent reads without an explicit end do not need a closed fence.
        if (!waitForTransactions && changeReadInfo.getMaxEndTimestampMs() == null) {
            return;
        }

        long timeoutMs = context.getSessionVariable().getChangeVisibleTimeoutMs();
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        ChangeReadFence fence;
        boolean acquiredFromRemoteMaster = !context.getEnv().isMaster();
        if (!acquiredFromRemoteMaster) {
            fence = acquireFenceOnMaster(changeReadInfo.getDbToTableIds(),
                    changeReadInfo.getMaxEndTimestampMs(), timeoutMs, waitForTransactions);
        } else {
            fence = acquireFenceFromMaster(context, changeReadInfo, timeoutMs, waitForTransactions);
        }

        if (acquiredFromRemoteMaster && waitForTransactions && !Config.isCloudMode()
                && context.getEnv().getReplayedJournalId() < fence.getMaxJournalId()) {
            long remainingMs = deadlineMs - System.currentTimeMillis();
            if (remainingMs <= 0) {
                throw new UserException(String.format(
                        "timeout waiting follower journal replay for time-based read, maxJournalId=%d",
                        fence.getMaxJournalId()));
            }
            context.getEnv().getJournalObservable().waitOn(
                    fence.getMaxJournalId(), (int) Math.min(Integer.MAX_VALUE, remainingMs));
        }
    }

    /**
     * Capture and validate the TSO before acquiring the transaction ID watermark. This method must
     * execute on the master FE; follower FEs invoke it through FrontendService.
     */
    public static ChangeReadFence acquireFenceOnMaster(Map<Long, List<Long>> dbToTableIds,
            Long maxEndTimestampMs, long timeoutMs, boolean waitForTransactions) throws UserException {
        Env env = Env.getCurrentEnv();
        if (!env.isMaster()) {
            throw new UserException("time-based change read fence must be acquired on the master FE");
        }

        TSOService.TSOStatusSnapshot tsoSnapshot = env.getTSOService().getStatusSnapshot();
        if (!tsoSnapshot.isInitialized()) {
            throw new UserException("TSO timestamp is not calibrated, please check");
        }
        long currentTso = tsoSnapshot.getCurrentTso();
        validateEndTimestamp(maxEndTimestampMs, currentTso);

        if (waitForTransactions) {
            long deadlineMs = System.currentTimeMillis() + timeoutMs;
            GlobalTransactionMgrIface txnMgr = Env.getCurrentGlobalTransactionMgr();
            long txnIdWatermark;
            try {
                txnIdWatermark = txnMgr.getTransactionIdWatermark();
            } catch (UserException e) {
                throw new UserException("get transaction id watermark failed for time-based read", e);
            }
            waitForPreviousTransactions(txnMgr, txnIdWatermark, dbToTableIds, deadlineMs);
            if (!Config.isCloudMode()) {
                synchronizeClassicPublishers(dbToTableIds, deadlineMs);
            }
        }
        return new ChangeReadFence(currentTso, env.getMaxJournalId());
    }

    @VisibleForTesting
    static ChangeReadInfo collectChangeReadInfo(ConnectContext context, Plan plan,
            Map<List<String>, TableIf> tables) {
        Map<Long, Set<Long>> dbToTableIdSets = new TreeMap<>();
        long[] maxEndTimestampMs = {-1L};
        plan.foreach(node -> {
            if (!(node instanceof UnboundRelation)) {
                return;
            }
            UnboundRelation relation = (UnboundRelation) node;
            TableScanParams scanParams = relation.getScanParams();
            if (scanParams == null || !scanParams.incrementalRead()) {
                return;
            }
            TableIf table = tables.get(RelationUtil.getQualifierName(context, relation.getNameParts()));
            if (!(table instanceof OlapTable)) {
                return;
            }
            OlapTable olapTable = (OlapTable) table;
            dbToTableIdSets.computeIfAbsent(olapTable.getDatabase().getId(), ignored -> new TreeSet<>())
                    .add(olapTable.getId());
            if (scanParams.getMapParams().containsKey(OlapScanNode.OLAP_END_TIMESTAMP)) {
                long endTimestampMs = OlapScanNode.parseChangeTimestamp(
                        scanParams.getMapParams().get(OlapScanNode.OLAP_END_TIMESTAMP));
                if (endTimestampMs > 0) {
                    maxEndTimestampMs[0] = Math.max(maxEndTimestampMs[0], endTimestampMs);
                }
            }
        });

        Map<Long, List<Long>> dbToTableIds = new TreeMap<>();
        dbToTableIdSets.forEach((dbId, tableIds) -> dbToTableIds.put(dbId, new ArrayList<>(tableIds)));
        return new ChangeReadInfo(dbToTableIds, maxEndTimestampMs[0] < 0 ? null : maxEndTimestampMs[0]);
    }

    private static void validateEndTimestamp(Long maxEndTimestampMs, long currentTso) throws UserException {
        if (maxEndTimestampMs == null) {
            return;
        }
        long maxSupportedEndTimestampMs = TSOTimestamp.extractPhysicalTime(currentTso);
        if (maxEndTimestampMs > maxSupportedEndTimestampMs) {
            throw new UserException(String.format(
                    "endTimestamp exceeds the maximum supported time for an INCR read: "
                            + "requestedEndTimestampMs=%d, CURRENT_TSO_PHYSICAL_TIME=%d",
                    maxEndTimestampMs, maxSupportedEndTimestampMs));
        }
    }

    private static void waitForPreviousTransactions(GlobalTransactionMgrIface txnMgr, long txnIdWatermark,
            Map<Long, List<Long>> dbToTableIds, long deadlineMs) throws UserException {
        for (Map.Entry<Long, List<Long>> dbEntry : dbToTableIds.entrySet()) {
            long dbId = dbEntry.getKey();
            List<Long> tableIds = dbEntry.getValue();
            while (!isPreviousTransactionsFinished(txnMgr, txnIdWatermark, dbId, tableIds)) {
                long remainingMs = deadlineMs - System.currentTimeMillis();
                if (remainingMs <= 0) {
                    throw new UserException(String.format(
                            "timeout waiting previous transactions finish for time-based read, "
                                    + "txnIdWatermark=%d dbId=%d tableIds=%s",
                            txnIdWatermark, dbId, tableIds));
                }
                try {
                    Thread.sleep(Math.min(TXN_POLL_INTERVAL_MS, remainingMs));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new UserException(String.format(
                            "interrupted while waiting previous transactions finish for time-based read, "
                                    + "txnIdWatermark=%d dbId=%d tableIds=%s",
                            txnIdWatermark, dbId, tableIds), e);
                }
            }
        }
    }

    private static boolean isPreviousTransactionsFinished(GlobalTransactionMgrIface txnMgr, long txnIdWatermark,
            long dbId, List<Long> tableIds) throws UserException {
        try {
            return txnMgr.isPreviousTransactionsFinished(txnIdWatermark, dbId, tableIds);
        } catch (AnalysisException e) {
            throw new UserException(String.format(
                    "check previous transactions failed for time-based read, "
                            + "txnIdWatermark=%d dbId=%d tableIds=%s",
                    txnIdWatermark, dbId, tableIds), e);
        }
    }

    /**
     * A classic transaction becomes VISIBLE in memory while its publisher still owns table write
     * locks. Taking the corresponding read locks after the transaction drain guarantees that the
     * visible journal and partition metadata updates have completed before maxJournalId is read.
     */
    private static void synchronizeClassicPublishers(Map<Long, List<Long>> dbToTableIds, long deadlineMs)
            throws UserException {
        for (Map.Entry<Long, List<Long>> dbEntry : dbToTableIds.entrySet()) {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbEntry.getKey());
            List<Table> tables = db.getTablesOnIdOrderIfExist(dbEntry.getValue());
            List<Table> lockedTables = new ArrayList<>(tables.size());
            try {
                for (Table table : tables) {
                    long remainingMs = deadlineMs - System.currentTimeMillis();
                    if (remainingMs <= 0 || !table.tryReadLock(remainingMs, TimeUnit.MILLISECONDS)) {
                        throw new UserException(String.format(
                                "timeout synchronizing visible versions for time-based read, dbId=%d tableId=%d",
                                dbEntry.getKey(), table.getId()));
                    }
                    lockedTables.add(table);
                }
            } finally {
                Collections.reverse(lockedTables);
                lockedTables.forEach(Table::readUnlock);
            }
        }
    }

    private static ChangeReadFence acquireFenceFromMaster(ConnectContext context, ChangeReadInfo changeReadInfo,
            long timeoutMs, boolean waitForTransactions) throws UserException {
        TAcquireTimeBasedChangeReadFenceRequest request = new TAcquireTimeBasedChangeReadFenceRequest();
        request.setDbToTableIds(changeReadInfo.getDbToTableIds());
        request.setTimeoutMs(timeoutMs);
        request.setWaitForTransactions(waitForTransactions);
        if (changeReadInfo.getMaxEndTimestampMs() != null) {
            request.setEndTimestampMs(changeReadInfo.getMaxEndTimestampMs());
        }

        TNetworkAddress masterAddress = new TNetworkAddress(
                context.getEnv().getMasterHost(), context.getEnv().getMasterRpcPort());
        int thriftTimeoutMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, timeoutMs));
        FrontendService.Client client;
        try {
            client = ClientPool.frontendPool.borrowObject(masterAddress, thriftTimeoutMs);
        } catch (Exception e) {
            throw new UserException("failed to get master FE client for time-based read", e);
        }

        boolean returnToPool = false;
        try {
            TAcquireTimeBasedChangeReadFenceResult result = client.acquireTimeBasedChangeReadFence(request);
            returnToPool = true;
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                String error = result.getStatus().isSetErrorMsgs()
                        ? String.join(". ", result.getStatus().getErrorMsgs())
                        : "unknown error";
                throw new UserException("acquire time-based read fence from master FE failed: " + error);
            }
            Preconditions.checkState(result.isSetCurrentTso(), "master FE did not return current_tso");
            Preconditions.checkState(result.isSetMaxJournalId(), "master FE did not return max_journal_id");
            return new ChangeReadFence(result.getCurrentTso(), result.getMaxJournalId());
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            throw new UserException("acquire time-based read fence from master FE failed", e);
        } finally {
            if (returnToPool) {
                ClientPool.frontendPool.returnObject(masterAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(masterAddress, client);
            }
        }
    }
}
