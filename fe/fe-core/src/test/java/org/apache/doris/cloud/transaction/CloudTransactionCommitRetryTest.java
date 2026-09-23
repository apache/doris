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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.extensions.insert.streaming.StreamingTaskTxnCommitAttachment;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TxnCommitAttachment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class CloudTransactionCommitRetryTest {
    private static final long TXN_ID = 123L;
    private static final long TIMEOUT_MS = 1000L;
    private final Database db = new Database(1L, "retry_test");
    private final List<Table> tables = Collections.emptyList();
    private final List<TabletCommitInfo> commitInfos = Collections.emptyList();
    private final List<TableStreamUpdateInfo> streamUpdates = Collections.singletonList(
            Mockito.mock(TableStreamUpdateInfo.class));
    private final TxnCommitAttachment attachment = new StreamingTaskTxnCommitAttachment();
    private CloudGlobalTransactionMgr manager;
    private Map<Long, Long> signatures;
    private int previousRetryTimes;

    @BeforeEach
    void setUp() {
        previousRetryTimes = Config.mow_calculate_delete_bitmap_retry_times;
        Config.mow_calculate_delete_bitmap_retry_times = 3;
        manager = Mockito.spy(new CloudGlobalTransactionMgr());
        signatures = Deencapsulation.getField(manager, "txnLastSignatureMap");
    }

    @AfterEach
    void tearDown() {
        Config.mow_calculate_delete_bitmap_retry_times = previousRetryTimes;
    }

    @Test
    void testRetryPreservesAttachmentStreamUpdatesAndSignature() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            Assertions.assertSame(attachment, invocation.getArgument(5));
            Assertions.assertSame(streamUpdates, invocation.getArgument(6));
            if (attempts.incrementAndGet() == 1) {
                signatures.put(TXN_ID, 456L);
                throw retryableError();
            }
            Assertions.assertEquals(Long.valueOf(456L), signatures.get(TXN_ID));
            return true;
        }).when(manager).commitAndPublishTransaction(db, tables, TXN_ID, commitInfos, TIMEOUT_MS,
                attachment, streamUpdates);

        Assertions.assertTrue(manager.commitAndPublishTransactionWithRetry(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, streamUpdates));
        Assertions.assertEquals(2, attempts.get());
        Assertions.assertFalse(signatures.containsKey(TXN_ID));
    }

    @Test
    void testRetryExhaustionPreservesAttemptLimitAndCleansSignature() throws Exception {
        UserException failure = retryableError();
        signatures.put(TXN_ID, 456L);
        Mockito.doThrow(failure).when(manager).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, streamUpdates);

        Assertions.assertSame(failure, Assertions.assertThrows(UserException.class,
                () -> manager.commitAndPublishTransactionWithRetry(db, tables, TXN_ID,
                        commitInfos, TIMEOUT_MS, attachment, streamUpdates)));
        Mockito.verify(manager, Mockito.times(3)).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, streamUpdates);
        Assertions.assertFalse(signatures.containsKey(TXN_ID));
    }

    @Test
    void testNonRetryableErrorIsPropagatedImmediately() throws Exception {
        UserException failure = new UserException("injected non-retryable error");
        signatures.put(TXN_ID, 456L);
        Mockito.doThrow(failure).when(manager).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, streamUpdates);

        Assertions.assertSame(failure, Assertions.assertThrows(UserException.class,
                () -> manager.commitAndPublishTransactionWithRetry(db, tables, TXN_ID,
                        commitInfos, TIMEOUT_MS, attachment, streamUpdates)));
        Mockito.verify(manager).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, streamUpdates);
        Assertions.assertFalse(signatures.containsKey(TXN_ID));
    }

    @Test
    void testFiveArgumentEntryStillRetriesWithoutAttachment() throws Exception {
        Mockito.doThrow(retryableError()).doReturn(true).when(manager).commitAndPublishTransaction(
                db, tables, TXN_ID, commitInfos, TIMEOUT_MS, null, Collections.emptyList());

        Assertions.assertTrue(manager.commitAndPublishTransaction(db, tables, TXN_ID, commitInfos, TIMEOUT_MS));
        Mockito.verify(manager, Mockito.times(2)).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, null, Collections.emptyList());
    }

    @Test
    void testBeCommitEntryLeavesRetryAndSignatureToCaller() throws Exception {
        UserException failure = retryableError();
        signatures.put(TXN_ID, 456L);
        Mockito.doThrow(failure).when(manager).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, Collections.emptyList());

        Assertions.assertSame(failure, Assertions.assertThrows(UserException.class,
                () -> manager.commitAndPublishTransaction(db, tables, TXN_ID,
                        commitInfos, TIMEOUT_MS, attachment)));
        Mockito.verify(manager).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, Collections.emptyList());
        Assertions.assertEquals(Long.valueOf(456L), signatures.get(TXN_ID));
    }

    @Test
    void testSharedNothingDefaultPreservesPublishTimeoutResult() throws Exception {
        GlobalTransactionMgrIface localManager = Mockito.mock(GlobalTransactionMgrIface.class,
                Mockito.CALLS_REAL_METHODS);
        Mockito.when(localManager.commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment)).thenReturn(false);

        Assertions.assertFalse(localManager.commitAndPublishTransactionWithRetry(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment, streamUpdates));
        Mockito.verify(localManager).commitAndPublishTransaction(db, tables, TXN_ID,
                commitInfos, TIMEOUT_MS, attachment);
    }

    private UserException retryableError() {
        return new UserException(InternalErrorCode.DELETE_BITMAP_LOCK_ERR,
                "Failed to calculate delete bitmap. Timeout.");
    }
}
