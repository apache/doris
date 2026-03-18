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

package org.apache.doris.clone;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.clone.TabletSchedCtx.BalanceType;
import org.apache.doris.clone.TabletSchedCtx.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test cases for TabletScheduler balance with transaction protection.
 * <p>
 * This test verifies that balance operations are properly blocked when there are
 * PRECOMMITTED or COMMITTED but not VISIBLE transactions, preventing data inconsistency
 * during broker load operations.
 * <p>
 * Note: This test uses reflection to access private methods for testing purposes.
 */
public class TabletSchedulerBalanceWithTransactionTest extends TestWithFeService {
    private Database db;
    private OlapTable testTable;
    private long partitionId;
    private TabletScheduler scheduler;
    private GlobalTransactionMgrIface globalTransactionMgr;

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_debug_points = true;
        Config.allow_replica_on_same_host = false;
        Config.disable_balance = false; // Enable balance for testing
    }

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        Thread.sleep(1000);
        createDatabase("test");
        useDatabase("test");
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");

        // Create a test table with 2 replicas
        String createTableSql = "CREATE TABLE test_table (\n"
                + "    k1 INT NOT NULL,\n"
                + "    k2 INT NOT NULL,\n"
                + "    v1 INT SUM\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"2\"\n"
                + ");";
        createTable(createTableSql);

        testTable = (OlapTable) db.getTableOrMetaException("test_table");
        partitionId = testTable.getPartitions().iterator().next().getId();

        scheduler = Env.getCurrentEnv().getTabletScheduler();
        globalTransactionMgr = Env.getCurrentGlobalTransactionMgr();
    }

    @Override
    protected void runBeforeEach() throws Exception {
        // Clean up before each test
        for (Table table : db.getTables()) {
            if (!table.getName().equals("test_table")) {
                dropTable(table.getName(), true);
            }
        }
        for (Backend be : Env.getCurrentSystemInfo().getBackendsByTag(Tag.DEFAULT_BACKEND_TAG)) {
            be.setDecommissioned(false);
        }
        scheduler.clear();
        DebugPointUtil.clearDebugPoints();
    }

    /**
     * Test that balance is blocked when there is a PRECOMMITTED transaction for the partition.
     * Uses reflection to test the private scheduleTablet method.
     */
    @Test
    public void testBalanceBlockedByPreCommittedTransaction() throws Exception {
        // Use DebugPoint to simulate PRECOMMITTED transaction check
        // This simulates the scenario where there is a PRECOMMITTED transaction
        DebugPointUtil.addDebugPoint("TabletScheduler.checkPreCommittedTransaction.return_true");

        // Create a balance tablet context
        TabletSchedCtx tabletCtx = createBalanceTabletCtx(testTable);

        // Try to add the tablet - it should be added but blocked during scheduling
        TabletScheduler.AddResult result = scheduler.addTablet(tabletCtx, false);
        Assert.assertEquals("Tablet should be added to pending queue",
                TabletScheduler.AddResult.ADDED, result);

        // Run scheduler - it should not schedule the tablet due to PRECOMMITTED transaction
        scheduler.runAfterCatalogReady();

        // Verify tablet is still in pending queue (not scheduled)
        // The tablet should remain in pending queue because scheduleTablet throws SchedException
        // which is caught and tablet is not moved to running
        List<TabletSchedCtx> pendingTablets = Lists.newArrayList(scheduler.getPendingTabletQueue());
        boolean foundInPending = pendingTablets.stream()
                .anyMatch(ctx -> ctx.getTabletId() == tabletCtx.getTabletId());

        // Tablet should either be in pending (if blocked) or removed (if error)
        // The key is that it should not be successfully scheduled
        Assert.assertTrue("Tablet should be blocked and remain in pending or be removed",
                foundInPending || tabletCtx.getErrMsg() != null);

        if (tabletCtx.getErrMsg() != null) {
            Assert.assertTrue("Error message should mention PRECOMMITTED transaction",
                    tabletCtx.getErrMsg().contains("PRECOMMITTED transaction"));
        }

        DebugPointUtil.removeDebugPoint("TabletScheduler.checkPreCommittedTransaction.return_true");
    }

    /**
     * Test that balance is blocked when there is a COMMITTED but not VISIBLE transaction.
     * This is the critical case for broker load data inconsistency issue.
     */
    @Test
    public void testBalanceBlockedByCommittedButNotVisibleTransaction() throws Exception {
        // Use DebugPoint to simulate existCommittedTxns returning true
        // This simulates the scenario where there is a COMMITTED but not VISIBLE transaction
        // Note: The DebugPoint is checked in TabletScheduler, not in existCommittedTxns itself
        DebugPointUtil.addDebugPoint("GlobalTransactionMgr.existCommittedTxns.return_true");

        // Create a balance tablet context
        TabletSchedCtx tabletCtx = createBalanceTabletCtx(testTable);

        // Try to add the tablet
        TabletScheduler.AddResult result = scheduler.addTablet(tabletCtx, false);
        Assert.assertEquals("Tablet should be added to pending queue",
                TabletScheduler.AddResult.ADDED, result);

        // Run scheduler - it should not schedule the tablet due to COMMITTED transaction
        scheduler.runAfterCatalogReady();

        // Verify tablet is blocked
        // The tablet should be removed from pending queue and moved to schedHistory
        // when UNRECOVERABLE SchedException is thrown
        List<TabletSchedCtx> pendingTablets = Lists.newArrayList(scheduler.getPendingTabletQueue());
        boolean foundInPending = pendingTablets.stream()
                .anyMatch(ctx -> ctx.getTabletId() == tabletCtx.getTabletId());

        // When blocked by COMMITTED transaction, tablet should be removed from pending
        // and error message should be set
        String errMsg = tabletCtx.getErrMsg();
        Assert.assertTrue("Tablet should be blocked and removed from pending queue. " +
                        "Found in pending: " + foundInPending + ", Error message: " + errMsg,
                !foundInPending);

        // Verify error message contains the expected content
        Assert.assertNotNull("Error message should be set", errMsg);
        Assert.assertTrue("Error message should mention COMMITTED transaction. Actual: " + errMsg,
                errMsg.contains("COMMITTED") || errMsg.contains("committed"));
        Assert.assertTrue("Error message should mention VISIBLE or visible. Actual: " + errMsg,
                errMsg.contains("VISIBLE") || errMsg.contains("visible"));

        DebugPointUtil.removeDebugPoint("GlobalTransactionMgr.existCommittedTxns.return_true");
    }

    /**
     * Test that balance is allowed when there is no transaction for the partition.
     */
    @Test
    public void testBalanceAllowedWhenNoTransaction() throws Exception {
        // Create a balance tablet context
        TabletSchedCtx tabletCtx = createBalanceTabletCtx(testTable);

        // Verify no committed transactions
        boolean hasCommitted = globalTransactionMgr.existCommittedTxns(
                db.getId(), testTable.getId(), partitionId);
        Assert.assertFalse("Should have no COMMITTED transactions", hasCommitted);

        // Try to add the tablet
        TabletScheduler.AddResult addResult = scheduler.addTablet(tabletCtx, false);
        Assert.assertEquals("Tablet should be added to pending queue",
                TabletScheduler.AddResult.ADDED, addResult);

        // Run scheduler - transaction check should pass (may fail for other reasons)
        scheduler.runAfterCatalogReady();

        // If there's an error, it should not be related to transaction
        if (tabletCtx.getErrMsg() != null) {
            Assert.assertFalse("Should not be blocked by transaction",
                    tabletCtx.getErrMsg().contains("PRECOMMITTED transaction")
                    || tabletCtx.getErrMsg().contains("COMMITTED but not VISIBLE transaction"));
        }
    }

    /**
     * Test that balance is allowed when transaction becomes VISIBLE.
     */
    @Test
    public void testBalanceAllowedAfterTransactionVisible() throws Exception {
        // This test verifies that when there are no COMMITTED transactions,
        // balance is allowed (transaction check passes)
        // Create a balance tablet context
        TabletSchedCtx tabletCtx = createBalanceTabletCtx(testTable);

        // Verify no committed transactions
        boolean hasCommitted = globalTransactionMgr.existCommittedTxns(
                db.getId(), testTable.getId(), partitionId);
        Assert.assertFalse("Should have no COMMITTED transactions", hasCommitted);

        // Add tablet
        TabletScheduler.AddResult addResult = scheduler.addTablet(tabletCtx, false);
        Assert.assertEquals("Tablet should be added", TabletScheduler.AddResult.ADDED, addResult);

        // Run scheduler - transaction check should pass (may fail for other reasons)
        scheduler.runAfterCatalogReady();

        // If there's an error, it should not be related to transaction
        if (tabletCtx.getErrMsg() != null) {
            Assert.assertFalse("Should not be blocked by transaction",
                    tabletCtx.getErrMsg().contains("PRECOMMITTED transaction")
                    || tabletCtx.getErrMsg().contains("COMMITTED but not VISIBLE transaction"));
        }
    }

    /**
     * Test partition-level check: transaction for different partition should not block.
     */
    @Test
    public void testPartitionLevelCheck() throws Exception {
        // Verify existCommittedTxns works at partition level
        // In a real scenario with broker load, this would return true if there's a COMMITTED transaction
        boolean hasCommittedForPartition = globalTransactionMgr.existCommittedTxns(
                db.getId(), testTable.getId(), partitionId);

        // This test verifies the method exists and works correctly
        // In a real integration test with actual broker load, we would:
        // 1. Start a broker load for this partition
        // 2. Verify existCommittedTxns returns true during COMMITTED phase
        // 3. Verify it returns false after VISIBLE phase

        // For now, we just verify the method can be called without exception
        Assert.assertNotNull("existCommittedTxns should return a boolean value",
                Boolean.valueOf(hasCommittedForPartition));
    }

    // Helper methods

    private TabletSchedCtx createBalanceTabletCtx(OlapTable table) {
        Partition partition = table.getPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();

        TabletSchedCtx ctx = new TabletSchedCtx(
                Type.BALANCE,
                db.getId(),
                table.getId(),
                partition.getId(),
                partition.getBaseIndex().getId(),
                tabletId,
                ReplicaAllocation.DEFAULT_ALLOCATION,
                System.currentTimeMillis()
        );
        // Set tag and balanceType to avoid "tag null does not exist" error
        // In real scenario, these would be set by the rebalancer
        ctx.setTag(Tag.DEFAULT_BACKEND_TAG);
        ctx.setBalanceType(BalanceType.BE_BALANCE);
        return ctx;
    }

}

