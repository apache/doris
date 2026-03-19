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

import org.apache.doris.common.io.Text;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * CommittedTxnManager manages all transactions that have committed but not yet published
 * in the async publish flow.
 *
 * Thread safety: Uses ConcurrentHashMap for storage, supports concurrent access from multiple threads.
 *
 * Location: As a member variable of CloudGlobalTransactionMgr.
 */
public class CommittedTxnManager {
    private static final Logger LOG = LogManager.getLogger(CommittedTxnManager.class);

    // Index by txnId: fast lookup of a single transaction
    // key: txnId, value: CommittedTxnEntry
    private final ConcurrentHashMap<Long, CommittedTxnEntry> txnIdToEntry = new ConcurrentHashMap<>();

    // Index by tableId: publish background thread iterates committed txns by table
    // key: tableId, value: {txnId -> CommittedTxnEntry}
    // Inner map also uses ConcurrentHashMap for thread safety
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, CommittedTxnEntry>> tableToTxns
            = new ConcurrentHashMap<>();

    /**
     * Called by commit phase: add a transaction to committed set.
     * Called within FE commit lock, but ConcurrentHashMap operation is O(1),
     * so it doesn't increase lock hold time significantly.
     *
     * @param entry the committed transaction entry to add
     */
    public void addCommittedTxn(CommittedTxnEntry entry) {
        if (entry == null) {
            LOG.warn("Failed to add committed txn: entry is null");
            return;
        }
        txnIdToEntry.put(entry.getTxnId(), entry);
        tableToTxns.computeIfAbsent(entry.getTableId(), k -> new ConcurrentHashMap<>())
                   .put(entry.getTxnId(), entry);
        LOG.debug("Added committed txn: {}", entry);
    }

    /**
     * Called by publish completion: remove a transaction from committed set.
     *
     * @param txnId the transaction id to remove
     */
    public void removeCommittedTxn(long txnId) {
        CommittedTxnEntry entry = txnIdToEntry.remove(txnId);
        if (entry != null) {
            ConcurrentHashMap<Long, CommittedTxnEntry> tableTxns = tableToTxns.get(entry.getTableId());
            if (tableTxns != null) {
                tableTxns.remove(txnId);
                // Clean up empty map (reduce memory usage) if this table has no more committed txns
                if (tableTxns.isEmpty()) {
                    tableToTxns.remove(entry.getTableId(), tableTxns);
                }
            }
            LOG.debug("Removed committed txn: txnId={}, tableId={}", txnId, entry.getTableId());
        } else {
            LOG.warn("Failed to remove committed txn: txnId={} not found", txnId);
        }
    }

    /**
     * Lookup by txnId
     *
     * @param txnId the transaction id
     * @return the CommittedTxnEntry, or null if not found
     */
    public CommittedTxnEntry getByTxnId(long txnId) {
        return txnIdToEntry.get(txnId);
    }

    /**
     * Get all pending transactions for a specific table (sorted by commit time)
     *
     * @param tableId the table id
     * @return list of CommittedTxnEntry sorted by commitTimeMs ascending
     */
    public List<CommittedTxnEntry> getByTableId(long tableId) {
        ConcurrentHashMap<Long, CommittedTxnEntry> tableTxns = tableToTxns.get(tableId);
        if (tableTxns == null) {
            return Collections.emptyList();
        }
        return tableTxns.values().stream()
                .sorted((a, b) -> Long.compare(a.getCommitTimeMs(), b.getCommitTimeMs()))
                .collect(Collectors.toList());
    }

    /**
     * Get all table ids that have pending committed transactions
     *
     * @return collection of table ids
     */
    public Collection<Long> getTablesWithCommittedTxns() {
        return tableToTxns.keySet();
    }

    /**
     * Get total count of committed transactions
     *
     * @return total count
     */
    public int size() {
        return txnIdToEntry.size();
    }

    /**
     * Check if a transaction is in committed set
     *
     * @param txnId the transaction id
     * @return true if the transaction is in committed set
     */
    public boolean contains(long txnId) {
        return txnIdToEntry.containsKey(txnId);
    }

    /**
     * Get all transaction ids
     *
     * @return collection of transaction ids
     */
    public Collection<Long> getAllTxnIds() {
        return txnIdToEntry.keySet();
    }

    /**
     * Clear all committed transactions (for testing or recovery)
     */
    public void clear() {
        txnIdToEntry.clear();
        tableToTxns.clear();
        LOG.info("Cleared all committed transactions");
    }

    /**
     * Get debug info as text
     *
     * @return debug information
     */
    public Text getDebugInfo() {
        Text info = new Text();
        StringBuilder sb = new StringBuilder();
        sb.append("CommittedTxnManager: total=").append(size())
                .append(", tables=").append(tableToTxns.size());
        for (Map.Entry<Long, ConcurrentHashMap<Long, CommittedTxnEntry>> tableEntry : tableToTxns.entrySet()) {
            sb.append("\n  tableId=").append(tableEntry.getKey())
                    .append(": ").append(tableEntry.getValue().size()).append(" txns");
        }
        byte[] bytes = sb.toString().getBytes();
        info.append(bytes, 0, bytes.length);
        return info;
    }
}
