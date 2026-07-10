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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the plugin read-side transaction lifecycle ({@link HiveReadTransaction} /
 * {@link HiveReadTransactionManager}) against a recording-fake {@link HmsClient}.
 *
 * <p>WHY: reading a transactional Hive table must open a metastore transaction, take a shared read lock
 * exactly once (holding it for the whole scan pins a consistent write-id snapshot), and release it by
 * committing when the query finishes. Acquiring the lock more than once, or forgetting to commit, either
 * corrupts snapshot isolation or leaks the lock. These tests pin that once-per-query lock + commit
 * contract.</p>
 */
public class HiveReadTransactionTest {

    @Test
    public void testBeginOpensTxnAndValidWriteIdsAcquiresLockOnce() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.openTxnReturn = 777L;
        client.validIds.put("hive.txn.valid.writeids", "db.tbl:8:x");

        HiveReadTransaction txn = new HiveReadTransaction(
                "q1", "alice", "db", "tbl", true, client);
        txn.begin();
        Assertions.assertEquals(List.of("openTxn:alice"), client.calls);

        txn.addPartition("dt=2026-01-01");
        Map<String, String> ids = txn.getValidWriteIds();
        Assertions.assertEquals("db.tbl:8:x", ids.get("hive.txn.valid.writeids"));
        // The lock must be scoped to the transaction, the (db, table), and the added partitions.
        Assertions.assertEquals(777L, client.lockTxnId);
        Assertions.assertEquals("db.tbl", client.getValidWriteIdsTable);
        Assertions.assertEquals(777L, client.getValidWriteIdsTxnId);
        Assertions.assertEquals(List.of("dt=2026-01-01"), client.lockPartitions);

        // Second call must be memoized: no extra lock / getValidWriteIds round-trips.
        Map<String, String> ids2 = txn.getValidWriteIds();
        Assertions.assertSame(ids, ids2);
        Assertions.assertEquals(1, count(client.calls, "acquireSharedLock:q1"));
        Assertions.assertEquals(1, count(client.calls, "getValidWriteIds:db.tbl"));
    }

    @Test
    public void testCommitCommitsTheTxn() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.openTxnReturn = 42L;
        HiveReadTransaction txn = new HiveReadTransaction("q1", "bob", "db", "t", false, client);
        txn.begin();
        txn.commit();
        Assertions.assertTrue(client.calls.contains("commitTxn:42"), client.calls.toString());
    }

    @Test
    public void testManagerRegisterBeginsAndDeregisterCommits() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.openTxnReturn = 5L;
        HiveReadTransactionManager mgr = new HiveReadTransactionManager();

        HiveReadTransaction txn = new HiveReadTransaction("q1", "u", "db", "t", true, client);
        mgr.register(txn);
        Assertions.assertEquals(1, count(client.calls, "openTxn:u"), "register opens the txn");

        mgr.deregister("q1");
        Assertions.assertEquals(1, count(client.calls, "commitTxn:5"), "deregister commits the txn");

        // Idempotent: a second deregister and an unknown query must be no-ops.
        mgr.deregister("q1");
        mgr.deregister("unknown");
        Assertions.assertEquals(1, count(client.calls, "commitTxn:5"), "no double commit");
    }

    @Test
    public void testScanProviderReleaseReadTransactionCommitsViaSharedManager() {
        RecordingHmsClient client = new RecordingHmsClient();
        client.openTxnReturn = 9L;
        // register and the provider's release MUST share the same per-connector manager (HiveConnector injects
        // one instance into every provider), so the release finds and commits the txn register opened.
        HiveReadTransactionManager mgr = new HiveReadTransactionManager();
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), mgr,
                new HiveFileListingCache(new HashMap<>()));

        // A transactional-hive scan opened a read txn (as planAcidScan does via mgr.register), taking the shared
        // read lock.
        mgr.register(new HiveReadTransaction("q9", "u", "db", "t", true, client));
        Assertions.assertEquals(1, count(client.calls, "openTxn:u"), "the scan opened a read txn");

        // The query-finish callback routes through the provider's releaseReadTransaction, which must commit the
        // txn (releasing the shared read lock) exactly once — otherwise the lock leaks for the metastore's life.
        provider.releaseReadTransaction("q9");
        Assertions.assertEquals(1, count(client.calls, "commitTxn:9"),
                "releaseReadTransaction must commit the txn (release the shared read lock) exactly once");

        // Idempotent: a second release, or a release for a query that opened no txn, is a safe no-op.
        provider.releaseReadTransaction("q9");
        provider.releaseReadTransaction("never-opened");
        Assertions.assertEquals(1, count(client.calls, "commitTxn:9"), "no double commit on repeat release");
    }

    private static int count(List<String> calls, String prefix) {
        int n = 0;
        for (String c : calls) {
            if (c.startsWith(prefix)) {
                n++;
            }
        }
        return n;
    }

    /**
     * Recording {@link HmsClient} fake: stubs the abstract read surface and records the four read-ACID
     * primitives the read transaction drives. All other primitives keep their default {@code throw}.
     */
    private static final class RecordingHmsClient implements HmsClient {
        final List<String> calls = new ArrayList<>();
        long openTxnReturn;
        long lockTxnId;
        List<String> lockPartitions;
        long getValidWriteIdsTxnId;
        String getValidWriteIdsTable;
        final Map<String, String> validIds = new HashMap<>();

        @Override
        public long openTxn(String user) {
            calls.add("openTxn:" + user);
            return openTxnReturn;
        }

        @Override
        public void acquireSharedLock(String queryId, long txnId, String user, String dbName,
                String tableName, List<String> partitionNames, long timeoutMs) {
            calls.add("acquireSharedLock:" + queryId + ":" + txnId);
            this.lockTxnId = txnId;
            this.lockPartitions = new ArrayList<>(partitionNames);
        }

        @Override
        public Map<String, String> getValidWriteIds(String fullTableName, long currentTransactionId) {
            calls.add("getValidWriteIds:" + fullTableName + ":" + currentTransactionId);
            this.getValidWriteIdsTxnId = currentTransactionId;
            this.getValidWriteIdsTable = fullTableName;
            return validIds;
        }

        @Override
        public void commitTxn(long txnId) {
            calls.add("commitTxn:" + txnId);
        }

        // ---- unused abstract read surface ----

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException("getDatabase");
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return false;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException("getTable");
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            return Collections.emptyList();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
                List<String> partNames) {
            return Collections.emptyList();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException("getPartition");
        }

        @Override
        public void close() {
        }
    }
}
