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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.hms.HmsClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Holds the state of one Hive read transaction, used when reading a transactional (ACID) Hive table.
 * Each instance is bound to a single query.
 *
 * <p>Plugin-side port of fe-core {@code HiveTransaction}. It drives the four read-side metastore
 * primitives through the shared {@link HmsClient} ({@code openTxn} / {@code acquireSharedLock} /
 * {@code getValidWriteIds} / {@code commitTxn}) instead of the fe-core {@code HMSExternalCatalog}/
 * {@code HMSCachedClient}/{@code TableNameInfo} chain — the table identity is carried as plain
 * {@code (dbName, tableName)} strings.</p>
 */
public class HiveReadTransaction {
    private final String queryId;
    private final String user;
    private final String dbName;
    private final String tableName;
    private final boolean isFullAcid;
    private final HmsClient hmsClient;

    private long txnId;
    private final List<String> partitionNames = new ArrayList<>();

    private Map<String, String> txnValidIds = null;

    public HiveReadTransaction(String queryId, String user, String dbName, String tableName,
            boolean isFullAcid, HmsClient hmsClient) {
        this.queryId = queryId;
        this.user = user;
        this.dbName = dbName;
        this.tableName = tableName;
        this.isFullAcid = isFullAcid;
        this.hmsClient = hmsClient;
    }

    public String getQueryId() {
        return queryId;
    }

    public void addPartition(String partitionName) {
        this.partitionNames.add(partitionName);
    }

    public boolean isFullAcid() {
        return isFullAcid;
    }

    /**
     * Acquires a shared read lock and fetches the snapshot ({@code ValidTxnList} + {@code
     * ValidWriteIdList}) for the current transaction, memoizing so the lock is taken at most once. The
     * lock is released when the transaction is committed at query finish.
     */
    public Map<String, String> getValidWriteIds() {
        if (txnValidIds == null) {
            hmsClient.acquireSharedLock(queryId, txnId, user, dbName, tableName, partitionNames, 5000);
            txnValidIds = hmsClient.getValidWriteIds(dbName + "." + tableName, txnId);
        }
        return txnValidIds;
    }

    public void begin() {
        try {
            this.txnId = hmsClient.openTxn(user);
        } catch (RuntimeException e) {
            throw new DorisConnectorException(e.getMessage(), e);
        }
    }

    public void commit() {
        try {
            hmsClient.commitTxn(txnId);
        } catch (RuntimeException e) {
            throw new DorisConnectorException(e.getMessage(), e);
        }
    }
}
