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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class TimeBasedChangeVisibleWaiterTest {
    private static final List<String> TABLE_QUALIFIER = ImmutableList.of("internal", "db", "tbl");
    private static final long DB_ID = 100L;
    private static final long TABLE_ID = 200L;
    private static final long DEFAULT_END_TS_MS = 1700000000000L;

    @Test
    public void testCollectDbToTableEndTSOUsesDefaultEndTimestamp() {
        OlapTable table = mockOlapTable(DB_ID, TABLE_ID);
        TimeBasedChangeVisibleWaiter waiter = new TimeBasedChangeVisibleWaiter(
                mockContext(), newChangeRelation(1, ImmutableMap.of()),
                ImmutableMap.of(TABLE_QUALIFIER, table), DEFAULT_END_TS_MS);

        Map<Long, Map<Long, Long>> result = waiter.collectDbToTableEndTSO();

        Assertions.assertEquals(TSOTimestamp.composeFullTimestamp(DEFAULT_END_TS_MS),
                result.get(DB_ID).get(TABLE_ID));
    }

    @Test
    public void testCollectDbToTableEndTSOMergesMaxEndTimestamp() {
        String endTimestamp1 = "2024-01-01 00:00:00";
        String endTimestamp2 = "2024-01-02 00:00:00";
        Plan plan = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                newChangeRelation(1, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, endTimestamp1)),
                newChangeRelation(2, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, endTimestamp2)),
                null);
        OlapTable table = mockOlapTable(DB_ID, TABLE_ID);
        TimeBasedChangeVisibleWaiter waiter = new TimeBasedChangeVisibleWaiter(
                mockContext(), plan, ImmutableMap.of(TABLE_QUALIFIER, table), DEFAULT_END_TS_MS);

        Map<Long, Map<Long, Long>> result = waiter.collectDbToTableEndTSO();

        Assertions.assertEquals(
                TSOTimestamp.composeFullTimestamp(OlapScanNode.parseChangeTimestamp(endTimestamp2)),
                result.get(DB_ID).get(TABLE_ID));
    }

    @Test
    public void testWaitForVisibleWaitsMatchedCommittedTransactionOnce() throws Exception {
        ConnectContext context = mockContext();
        OlapTable table = mockOlapTable(DB_ID, TABLE_ID);
        TransactionState txn = mockCommittedTxn();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(txnMgr.getCommittedTransactions(DB_ID)).thenReturn(ImmutableList.of(txn));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);

            TimeBasedChangeVisibleWaiter.waitForVisible(context, newChangeRelation(1, ImmutableMap.of()),
                    ImmutableMap.of(TABLE_QUALIFIER, table));
        }

        Mockito.verify(txnMgr, Mockito.times(1)).getCommittedTransactions(DB_ID);
        Mockito.verify(txn, Mockito.times(1)).waitTransactionVisible(Mockito.anyLong());
    }

    private ConnectContext mockContext() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setChangeVisibleTimeoutMs(1000);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        return context;
    }

    private UnboundRelation newChangeRelation(int relationId, Map<String, String> mapParams) {
        return new UnboundRelation(new RelationId(relationId), TABLE_QUALIFIER,
                ImmutableList.of(), false, ImmutableList.of(), ImmutableList.of(),
                Optional.empty(), Optional.empty(),
                new TableScanParams(TableScanParams.INCREMENTAL_READ, mapParams, ImmutableList.of()),
                Optional.empty());
    }

    private OlapTable mockOlapTable(long dbId, long tableId) {
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getId()).thenReturn(dbId);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getId()).thenReturn(tableId);
        return table;
    }

    private TransactionState mockCommittedTxn() throws Exception {
        AtomicReference<TransactionStatus> status = new AtomicReference<>(TransactionStatus.COMMITTED);
        TransactionState txn = Mockito.mock(TransactionState.class);
        Mockito.when(txn.getTransactionStatus()).thenAnswer(invocation -> status.get());
        Mockito.when(txn.getTableIdList()).thenReturn(ImmutableList.of(TABLE_ID));
        Mockito.when(txn.getCommitTSO()).thenReturn(TSOTimestamp.composeFullTimestamp(1L));
        Mockito.doAnswer(invocation -> {
            status.set(TransactionStatus.VISIBLE);
            return null;
        }).when(txn).waitTransactionVisible(Mockito.anyLong());
        return txn;
    }
}
