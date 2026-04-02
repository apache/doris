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

import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TxnCommitAttachment;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * CommittedTxnEntry records all information about a transaction that has completed
 * the two-phase commit (but has not yet been published).
 *
 * Lifecycle: added in commit phase -> removed after publish completes.
 */
public class CommittedTxnEntry {
    // ---- Basic identifiers ----
    private final long txnId;
    private final long dbId;
    private final long tableId;

    // ---- Partition commit versions ----
    // Commit versions returned from MS commit phase for each partition
    // Map<partitionId, commitVersion>
    private final Map<Long, Long> partitionCommitVersions;

    // ---- Tablet commit infos ----
    // Tablet commit info reported by BE, needed for CalcDeleteBitmapTask in publish phase
    // Contains tabletId and backendId
    private final List<TabletCommitInfo> tabletCommitInfos;

    // ---- Additional info ----
    private final TxnCommitAttachment txnCommitAttachment;
    private final long commitTimeMs; // Timestamp when commit completed, for timeout detection

    // ---- Publish waiting mechanism ----
    // Import thread waits for publish completion through this latch after commit
    private final CountDownLatch publishLatch = new CountDownLatch(1);
    private volatile boolean publishSucceeded = false;
    private volatile String publishErrorMsg = null;

    public CommittedTxnEntry(long txnId, long dbId, long tableId,
                             Map<Long, Long> partitionCommitVersions,
                             List<TabletCommitInfo> tabletCommitInfos,
                             TxnCommitAttachment txnCommitAttachment) {
        this.txnId = txnId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionCommitVersions = partitionCommitVersions;
        this.tabletCommitInfos = tabletCommitInfos;
        this.txnCommitAttachment = txnCommitAttachment;
        this.commitTimeMs = System.currentTimeMillis();
    }

    /**
     * Called by publish thread: mark publish succeeded, wake up waiting import thread
     */
    public void markPublishSucceeded() {
        this.publishSucceeded = true;
        this.publishLatch.countDown();
    }

    /**
     * Called by publish thread: mark publish failed, wake up waiting import thread
     */
    public void markPublishFailed(String errorMsg) {
        this.publishSucceeded = false;
        this.publishErrorMsg = errorMsg;
        this.publishLatch.countDown();
    }

    /**
     * Called by import thread: wait for publish completion
     *
     * @param timeoutMs timeout in milliseconds
     * @return true if publish completed (success or failure), false if timeout
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitPublish(long timeoutMs) throws InterruptedException {
        return publishLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait for publish completion with timeout, throw exception if timeout
     *
     * @param timeoutMs timeout in milliseconds
     * @throws InterruptedException if interrupted while waiting
     * @throws TimeoutException if timeout before publish completes
     */
    public void awaitPublishOrThrow(long timeoutMs) throws InterruptedException, TimeoutException {
        boolean completed = publishLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (!completed) {
            throw new TimeoutException("Publish timeout for txn " + txnId + " after " + timeoutMs + "ms");
        }
    }

    // Getters
    public long getTxnId() {
        return txnId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, Long> getPartitionCommitVersions() {
        return partitionCommitVersions;
    }

    public List<TabletCommitInfo> getTabletCommitInfos() {
        return tabletCommitInfos;
    }

    public TxnCommitAttachment getTxnCommitAttachment() {
        return txnCommitAttachment;
    }

    public long getCommitTimeMs() {
        return commitTimeMs;
    }

    public boolean isPublishSucceeded() {
        return publishSucceeded;
    }

    public String getPublishErrorMsg() {
        return publishErrorMsg;
    }

    public boolean isPublishCompleted() {
        return publishLatch.getCount() == 0;
    }

    @Override
    public String toString() {
        return "CommittedTxnEntry{"
                + "txnId=" + txnId
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", partitionCount="
                + (partitionCommitVersions != null ? partitionCommitVersions.size() : 0)
                + ", tabletCount=" + (tabletCommitInfos != null ? tabletCommitInfos.size() : 0)
                + ", commitTimeMs=" + commitTimeMs
                + ", publishCompleted=" + isPublishCompleted()
                + ", publishSucceeded=" + publishSucceeded
                + '}';
    }
}
