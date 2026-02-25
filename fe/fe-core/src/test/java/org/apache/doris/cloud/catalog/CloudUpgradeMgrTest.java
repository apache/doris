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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudUpgradeMgrTest {

    private boolean oldEnableAbortConflictTxn;

    @Before
    public void setUp() {
        oldEnableAbortConflictTxn = Config.enable_abort_txn_by_checking_conflict_txn;
    }

    @After
    public void tearDown() {
        Config.enable_abort_txn_by_checking_conflict_txn = oldEnableAbortConflictTxn;
    }

    @Test
    public void testLogAndAbortFailedConflictTxnsWhenEnabled() throws Exception {
        Config.enable_abort_txn_by_checking_conflict_txn = true;
        CloudUpgradeMgr cloudUpgradeMgr = new CloudUpgradeMgr(null);
        CloudGlobalTransactionMgr txnMgr = new CloudGlobalTransactionMgr();

        long dbId = 1000L;
        long waterTxnId = 9000L;
        long beId = 2000L;
        List<Long> tableIdList = Lists.newArrayList(11L, 12L);

        TransactionState conflictTxn1 = newTxn(dbId, 101L, "txn_101");
        TransactionState conflictTxn2 = newTxn(dbId, 102L, "txn_102");
        TransactionState conflictTxn3 = newTxn(dbId, 103L, "txn_103");
        List<TransactionState> conflictTxns = Lists.newArrayList(conflictTxn1, conflictTxn2, conflictTxn3);
        List<TransactionState> failedTxns = Lists.newArrayList(conflictTxn1, conflictTxn3);

        List<Long> abortedTxnIds = new ArrayList<>();

        new MockUp<Env>() {
            @Mock
            public CloudGlobalTransactionMgr getCurrentGlobalTransactionMgr() {
                return txnMgr;
            }
        };
        new MockUp<CloudGlobalTransactionMgr>() {
            @Mock
            public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId, long actualDbId,
                    List<Long> actualTableIdList) {
                Assert.assertEquals(waterTxnId, endTransactionId);
                Assert.assertEquals(dbId, actualDbId);
                Assert.assertEquals(tableIdList, actualTableIdList);
                return conflictTxns;
            }

            @Mock
            public void abortTransaction(Long actualDbId, Long txnId, String reason) {
                Assert.assertEquals(dbId, actualDbId.longValue());
                Assert.assertEquals("Cancel by cloud upgrade", reason);
                abortedTxnIds.add(txnId);
            }
        };
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public List<TransactionState> checkFailedTxns(List<TransactionState> txns) {
                Assert.assertEquals(conflictTxns, txns);
                return failedTxns;
            }
        };

        Deencapsulation.invoke(cloudUpgradeMgr, "logAndAbortFailedConflictTxns", beId,
                createDbWithWaterTxn(dbId, waterTxnId), tableIdList);

        Assert.assertEquals(Lists.newArrayList(101L, 103L), abortedTxnIds);
    }

    @Test
    public void testLogAndAbortFailedConflictTxnsWhenDisabled() throws Exception {
        Config.enable_abort_txn_by_checking_conflict_txn = false;
        CloudUpgradeMgr cloudUpgradeMgr = new CloudUpgradeMgr(null);
        CloudGlobalTransactionMgr txnMgr = new CloudGlobalTransactionMgr();

        long dbId = 1000L;
        long waterTxnId = 9000L;
        long beId = 2000L;
        List<Long> tableIdList = Lists.newArrayList(11L, 12L);

        List<TransactionState> conflictTxns = Lists.newArrayList(
                newTxn(dbId, 201L, "txn_201"),
                newTxn(dbId, 202L, "txn_202"));

        AtomicInteger checkFailedCallCount = new AtomicInteger(0);
        AtomicInteger abortCallCount = new AtomicInteger(0);

        new MockUp<Env>() {
            @Mock
            public CloudGlobalTransactionMgr getCurrentGlobalTransactionMgr() {
                return txnMgr;
            }
        };
        new MockUp<CloudGlobalTransactionMgr>() {
            @Mock
            public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId, long actualDbId,
                    List<Long> actualTableIdList) {
                return conflictTxns;
            }

            @Mock
            public void abortTransaction(Long actualDbId, Long txnId, String reason) {
                abortCallCount.incrementAndGet();
            }
        };
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public List<TransactionState> checkFailedTxns(List<TransactionState> txns) {
                checkFailedCallCount.incrementAndGet();
                return txns;
            }
        };

        Deencapsulation.invoke(cloudUpgradeMgr, "logAndAbortFailedConflictTxns", beId,
                createDbWithWaterTxn(dbId, waterTxnId), tableIdList);

        Assert.assertEquals(0, checkFailedCallCount.get());
        Assert.assertEquals(0, abortCallCount.get());
    }

    @Test
    public void testLogAndAbortFailedConflictTxnsContinueWhenAbortFailed() throws Exception {
        Config.enable_abort_txn_by_checking_conflict_txn = true;
        CloudUpgradeMgr cloudUpgradeMgr = new CloudUpgradeMgr(null);
        CloudGlobalTransactionMgr txnMgr = new CloudGlobalTransactionMgr();

        long dbId = 1000L;
        long waterTxnId = 9000L;
        long beId = 2000L;
        List<Long> tableIdList = Lists.newArrayList(11L, 12L);

        TransactionState conflictTxn1 = newTxn(dbId, 301L, "txn_301");
        TransactionState conflictTxn2 = newTxn(dbId, 302L, "txn_302");
        List<TransactionState> conflictTxns = Lists.newArrayList(conflictTxn1, conflictTxn2);

        AtomicInteger abortAttemptCount = new AtomicInteger(0);

        new MockUp<Env>() {
            @Mock
            public CloudGlobalTransactionMgr getCurrentGlobalTransactionMgr() {
                return txnMgr;
            }
        };
        new MockUp<CloudGlobalTransactionMgr>() {
            @Mock
            public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId, long actualDbId,
                    List<Long> actualTableIdList) {
                return conflictTxns;
            }

            @Mock
            public void abortTransaction(Long actualDbId, Long txnId, String reason) throws UserException {
                abortAttemptCount.incrementAndGet();
                if (txnId == 301L) {
                    throw new UserException("mock abort failed");
                }
            }
        };
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public List<TransactionState> checkFailedTxns(List<TransactionState> txns) {
                return txns;
            }
        };

        Deencapsulation.invoke(cloudUpgradeMgr, "logAndAbortFailedConflictTxns", beId,
                createDbWithWaterTxn(dbId, waterTxnId), tableIdList);

        Assert.assertEquals(2, abortAttemptCount.get());
    }

    private static TransactionState newTxn(long dbId, long txnId, String label) {
        return new TransactionState(
                dbId,
                Lists.newArrayList(1L),
                txnId,
                label,
                null,
                TransactionState.LoadJobSourceType.FRONTEND,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0, "127.0.0.1", 0L),
                -1,
                1000L);
    }

    private static Object createDbWithWaterTxn(long dbId, long txnId) throws Exception {
        Class<?> clazz = Class.forName("org.apache.doris.cloud.catalog.CloudUpgradeMgr$DbWithWaterTxn");
        Constructor<?> constructor = clazz.getDeclaredConstructor(Long.class, Long.class);
        constructor.setAccessible(true);
        return constructor.newInstance(dbId, txnId);
    }
}
