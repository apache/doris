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

import org.apache.doris.common.LabelAlreadyUsedException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CommitJournalPendingTest {

    @Test
    public void testPendingCommitIsNotReportedAsFinished() {
        TransactionState txn = new TransactionState();
        txn.setTransactionStatus(TransactionStatus.COMMITTED);
        txn.setCommitJournalPending(true);
        Assertions.assertEquals(TransactionStatus.PREPARE, txn.getDurableTransactionStatus());
        Assertions.assertEquals("RUNNING", new LabelAlreadyUsedException(txn).getJobStatus());

        txn.setCommitJournalPending(false);
        Assertions.assertEquals(TransactionStatus.COMMITTED, txn.getDurableTransactionStatus());
        Assertions.assertEquals("FINISHED", new LabelAlreadyUsedException(txn).getJobStatus());
    }

    @Test
    public void testOtherStatusesAreReportedAsTheyAre() {
        TransactionState txn = new TransactionState();
        txn.setTransactionStatus(TransactionStatus.VISIBLE);
        Assertions.assertEquals(TransactionStatus.VISIBLE, txn.getDurableTransactionStatus());
        txn.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        Assertions.assertEquals(TransactionStatus.PRECOMMITTED, txn.getDurableTransactionStatus());
    }
}
