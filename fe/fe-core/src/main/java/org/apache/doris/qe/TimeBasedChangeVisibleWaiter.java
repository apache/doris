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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Before executing a time-based incremental read, block until every transaction that committed at
 * or before the requested read timestamp of the target tables becomes visible. This guarantees the
 * read sees a complete set of changes up to that time point.
 *
 * <p>Skipped entirely when the session enables eventual-consistent change reads, or when no table
 * is involved. Waiting is bounded by session variable {@code change_visible_timeout_ms}; timing out
 * raises a {@link UserException}.
 */
public class TimeBasedChangeVisibleWaiter {
    private final ConnectContext context;

    public static void waitForVisible(ConnectContext context, Plan plan, Map<List<String>, TableIf> tables)
            throws UserException {
        if (context.getSessionVariable().isEnableEventualConsistentChange() || tables.isEmpty()) {
            return;
        }
        Map<Long, Map<Long, Long>> dbToTableEndTSO = collectDbToTableEndTSO(
                context, plan, tables, System.currentTimeMillis());
        new TimeBasedChangeVisibleWaiter(context).waitForDbToTableEndTSO(dbToTableEndTSO);
    }

    private TimeBasedChangeVisibleWaiter(ConnectContext context) {
        this.context = context;
    }

    /**
     * Walk the plan, pick out relations doing an incremental read, and for each OlapTable record the
     * read end timestamp (converted to a full TSO) aggregated as dbId -> (tableId -> max endTSO).
     */
    @VisibleForTesting
    static Map<Long, Map<Long, Long>> collectDbToTableEndTSO(ConnectContext context, Plan plan,
            Map<List<String>, TableIf> tables, long defaultEndTsMs) {
        Map<Long, Map<Long, Long>> dbToTableEndTSO = new HashMap<>();
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
            if (table instanceof OlapTable) {
                addTableEndTSO(dbToTableEndTSO, (OlapTable) table, getEndTsMs(scanParams, defaultEndTsMs));
            }
        });
        return dbToTableEndTSO;
    }

    /**
     * For each db, scan its committed-but-not-visible transactions; whenever a transaction's commit
     * TSO falls within a target table's endTSO, wait for that transaction to become visible.
     */
    private void waitForDbToTableEndTSO(Map<Long, Map<Long, Long>> dbToTableEndTSO) throws UserException {
        if (dbToTableEndTSO.isEmpty()) {
            return;
        }
        long deadlineMs = System.currentTimeMillis() + context.getSessionVariable().getChangeVisibleTimeoutMs();
        for (Map.Entry<Long, Map<Long, Long>> dbEntry : dbToTableEndTSO.entrySet()) {
            long dbId = dbEntry.getKey();
            Map<Long, Long> tableEndTSO = dbEntry.getValue();
            for (TransactionState txn : getCommittedTransactions(dbId)) {
                Pair<Long, Long> matchedTableEndTSO = findMatchedTableEndTSO(txn, tableEndTSO);
                if (matchedTableEndTSO != null) {
                    waitTransactionVisible(txn, dbId, matchedTableEndTSO.first, matchedTableEndTSO.second,
                            deadlineMs);
                }
            }
        }
    }

    /**
     * Return (tableId, endTSO) if the transaction is COMMITTED and its commit TSO is within the
     * requested endTSO of one of its tables; otherwise null (no need to wait).
     */
    private Pair<Long, Long> findMatchedTableEndTSO(TransactionState txn, Map<Long, Long> tableEndTSO) {
        long commitTSO = txn.getCommitTSO();
        if (txn.getTransactionStatus() != TransactionStatus.COMMITTED || commitTSO < 0) {
            return null;
        }
        for (Long tableId : txn.getTableIdList()) {
            Long endTSO = tableEndTSO.get(tableId);
            if (endTSO != null && commitTSO <= endTSO) {
                return Pair.of(tableId, endTSO);
            }
        }
        return null;
    }

    private List<TransactionState> getCommittedTransactions(long dbId) throws UserException {
        try {
            return Env.getCurrentGlobalTransactionMgr().getCommittedTransactions(dbId);
        } catch (Exception e) {
            throw new UserException("get committed transactions failed. dbId=" + dbId, e);
        }
    }

    /**
     * Poll-wait until the transaction leaves COMMITTED (becomes visible) or the deadline passes;
     * throw if it is still COMMITTED at timeout.
     */
    private void waitTransactionVisible(TransactionState txn, long dbId, long tableId,
            long endTSO, long deadlineMs) throws UserException {
        long remainingMs = deadlineMs - System.currentTimeMillis();
        while (txn.getTransactionStatus() == TransactionStatus.COMMITTED && remainingMs > 0) {
            try {
                txn.waitTransactionVisible(remainingMs);
            } catch (InterruptedException ignored) {
                // Keep the previous wait behavior.
            }
            remainingMs = deadlineMs - System.currentTimeMillis();
        }
        if (txn.getTransactionStatus() == TransactionStatus.COMMITTED) {
            throw new UserException(String.format(
                    "timeout waiting transaction become visible for time-based read, "
                            + "txnId=%d dbId=%d tableId=%d endTSO=%d",
                    txn.getTransactionId(), dbId, tableId, endTSO));
        }
    }

    // Resolve the read end timestamp (ms) from scan params; fall back to defaultEndTsMs (the query
    // start time) when absent or non-positive.
    private static long getEndTsMs(TableScanParams scanParams, long defaultEndTsMs) {
        if (scanParams.getMapParams().containsKey(OlapScanNode.OLAP_END_TIMESTAMP)) {
            long endTsMs = OlapScanNode.parseChangeTimestamp(
                    scanParams.getMapParams().get(OlapScanNode.OLAP_END_TIMESTAMP));
            return endTsMs > 0 ? endTsMs : defaultEndTsMs;
        }
        return defaultEndTsMs;
    }

    // Compose endTsMs into a full TSO and merge into dbId -> (tableId -> endTSO), keeping the max.
    private static void addTableEndTSO(Map<Long, Map<Long, Long>> dbToTableEndTSO, OlapTable table, long endTsMs) {
        dbToTableEndTSO.computeIfAbsent(table.getDatabase().getId(), ignored -> new HashMap<>())
                .merge(table.getId(), TSOTimestamp.composeFullTimestamp(endTsMs), Math::max);
    }
}
