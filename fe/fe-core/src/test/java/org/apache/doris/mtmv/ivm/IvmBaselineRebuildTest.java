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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVPartitionState;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IvmBaselineRebuildTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_table_stream = true;
    }

    @Test
    public void testTruncateMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_truncate";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    /**
     * A truncated partition is placed on the MV partitions that read it, the way a dropped partition is:
     * the requirement is what carries the change, not the state, and every other partition keeps catching
     * up incrementally.
     */
    @Test
    public void testTruncateAPartitionStaysOnThePartitionsThatReadIt() throws Exception {
        String db = "ivm_truncate_one_partition";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        Set<String> expected = mvPartitionsWithSameRange(mtmv, getBaseTable(db), "p202001");
        Assertions.assertEquals(1, expected.size());

        alignStatesOf(mtmv);
        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        for (String partitionName : mtmv.getPartitionNames()) {
            long expectedLatest = expected.contains(partitionName) ? 2 : 1;
            Assertions.assertEquals(expectedLatest,
                    mtmv.getPartitionStates().get(partitionName).getLatestEpoch());
        }
    }

    /**
     * An entire-table truncate is recorded the same way, and this is the case that says the scope of the
     * change is not what decides: every MV partition reads a partition that was replaced, so every one of
     * them has its requirement raised -- and the state, which is the coarser record, is not what carries
     * it. Recording it as a whole-MV change instead would send every partition of the MV to a rebuild and
     * reset the streams, for a change whose scope the per-partition requirement already expresses.
     */
    @Test
    public void testTruncateTableStaysOnThePartitionsThatReadIt() throws Exception {
        String db = "ivm_truncate_entire_table";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        Assertions.assertEquals(2, mtmv.getPartitionNames().size());

        alignStatesOf(mtmv);
        executeSql("TRUNCATE TABLE ivm_base");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        for (String partitionName : mtmv.getPartitionNames()) {
            Assertions.assertEquals(2,
                    mtmv.getPartitionStates().get(partitionName).getLatestEpoch());
        }
    }

    @Test
    public void testTruncatePartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_truncate_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testRepeatedBrokenEventsAdvanceSchemaChangeVersion() throws Exception {
        String db = "ivm_broken_repeated_partition_changes";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long initialSchemaChangeVersion = mtmv.getSchemaChangeVersion();

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");
        Assertions.assertEquals(initialSchemaChangeVersion + 1, mtmv.getSchemaChangeVersion());

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202002)");
        Assertions.assertEquals(initialSchemaChangeVersion + 2, mtmv.getSchemaChangeVersion());
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    @Test
    public void testDropPartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_drop_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        // SELF_MANAGE: the single MV partition reads every base partition, and the partition mapping API
        // answers nothing for it, so the whole MV has to be rebuilt.
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testDropColumnMarksBaselineRebuildOnlyWhenReferenced() throws Exception {
        String db = "ivm_broken_drop_column";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);

        // ivm_mv selects dt, k1, v1. Dropping a column it does not use leaves the MV where it is: no
        // partition's requirement is raised, and the MV is not invalidated either. The criterion is the
        // MV's own query -- a column the query does not name leaves it analyzable, so there is no rebuild
        // for the state to stand for, and writing one would discard the result of a refresh running
        // against rows that are still the rows they should be.
        alignStatesOf(mtmv);
        long versionBefore = mtmv.getSchemaChangeVersion();
        Map<String, Long> before = latestEpochsOf(mtmv);
        executeSql("ALTER TABLE ivm_base ADD COLUMN spare int");
        executeSql("ALTER TABLE ivm_base DROP COLUMN spare");
        Assertions.assertEquals(before, latestEpochsOf(mtmv),
                "a column the MV does not use must not raise any partition's requirement");
        assertNotInvalidated(mtmv, versionBefore, "a column the MV does not use must not invalidate the MV");

        // Dropping a column the MV uses makes the MV query unanalyzable: the change is metadata-only
        // and emits no binlog, so an incremental refresh would silently keep the rows of the old
        // column. The baseline has to be invalidated instead.
        executeSql("ALTER TABLE ivm_base DROP COLUMN v1");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        // And the state is the only record: the invalidation stands for the change, so the detail names
        // the reason rather than the alter that caused it.
        assertUnanalyzableDetail(mtmv);
    }

    /**
     * The sync window is a property any MV can carry, but only an IVM MV maintains a baseline through it.
     * Widening it on a plain MV brings base partitions back into a set it never stopped maintaining, so a
     * whole-MV rebuild would recompute partitions it already has -- which is why the widening checks are
     * judged together, under the guard that the MV maintains an IVM baseline at all.
     */
    @Test
    public void testWideningTheSyncWindowDoesNotInvalidateANonIvmMv() throws Exception {
        String db = "ivm_sync_window_non_ivm";
        createPartitionedIvmTable(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertFalse(mtmv.isIvm());

        // A window starts applying, is narrowed, then widened. The widen is what owes a rebuild for an IVM
        // MV; for this one it is a change to a window nothing maintains.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '10')");
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '1')");
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '10')");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        // The window is still applied: it is the rebuild it owes that is IVM's, not the property.
        Assertions.assertEquals("10", mtmv.getMvProperties().get("partition_sync_limit"));
    }

    /**
     * A plain MV can name its columns itself, and the check has to keep matching those by position. The MV
     * persists the names it was given while re-analysing its query yields the names it selects -- and only
     * an IVM MV has the stored query rewritten with its aliases -- so a name lookup finds nothing and
     * reports a column that exists as missing. The check runs on every refresh of an MV in SCHEMA_CHANGE,
     * so that MV would stop refreshing for good after an otherwise compatible base-table change.
     */
    @Test
    public void testAnMvWithItsOwnColumnNamesIsStillUsable() throws Exception {
        String db = "ivm_mv_column_aliases";
        createPartitionedIvmTable(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv (c_dt, c_k1, c_v1)\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertFalse(mtmv.isIvm());
        Assertions.assertEquals("c_v1", mtmv.getBaseSchema(true).get(2).getName());

        Assertions.assertDoesNotThrow(() -> MTMVPlanUtil.ensureMTMVQueryUsable(mtmv,
                MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK)));
    }

    /**
     * The query check belongs to the MV, not to IVM. A plain MV's query is taken away by the same base
     * table change as an IVM MV's is, and it is left in the same state -- the state its refresh re-analyzes
     * the query under, which is how it finds out. The detail is what the two have to agree on, and it has
     * to say the query is gone: "the base table has been updated" is true of every alter, including the
     * ones the query survives.
     */
    @Test
    public void testDroppingAReadColumnInvalidatesANonIvmMv() throws Exception {
        String db = "ivm_query_unusable_non_ivm";
        createPartitionedIvmTable(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertFalse(mtmv.isIvm());

        // A column this MV does not read leaves its query alone, so the MV is left alone too, IVM or not:
        // the state is a rebuild the next refresh owes, and there is none to owe. What an IVM MV has on top
        // of the state is its epoch state, and that stays where it was as well.
        long versionBefore = mtmv.getSchemaChangeVersion();
        executeSql("ALTER TABLE ivm_base ADD COLUMN spare int");
        executeSql("ALTER TABLE ivm_base DROP COLUMN spare");
        assertNotInvalidated(mtmv, versionBefore, "a column the MV does not read must not invalidate it");

        // A column it does read takes the query away, and that is what the MV is invalidated with.
        executeSql("ALTER TABLE ivm_base DROP COLUMN v1");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        assertUnanalyzableDetail(mtmv);
    }

    /**
     * A renamed column is judged the same way a dropped one is, and the operation says so for itself: a row
     * binlog table refuses a column rename outright, so this is a plain MV's path, and the re-analysis is
     * the answer for it -- the query names the old name, or it does not name the column at all.
     */
    @Test
    public void testARenamedColumnIsJudgedByTheQuery() throws Exception {
        String db = "ivm_renamed_column";
        createPartitionedTableWithoutRowBinlog(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertFalse(mtmv.isIvm());

        executeSql("ALTER TABLE ivm_base ADD COLUMN spare int");
        long versionBefore = mtmv.getSchemaChangeVersion();
        executeSql("ALTER TABLE ivm_base RENAME COLUMN spare spare_renamed");
        assertNotInvalidated(mtmv, versionBefore, "a column the MV does not read must not invalidate it");

        executeSql("ALTER TABLE ivm_base RENAME COLUMN v1 v1_renamed");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        assertUnanalyzableDetail(mtmv);
    }

    /**
     * A column change is only judged by the queries once the table has it. The query is analysed against the
     * table as it is when the hook runs, so a schema change that has not been applied yet leaves the column
     * in place and the answer would be about the table from before the change -- which is why the alter asks
     * the operation instead of trusting its own submission. Pinned here: the answer flips when the change
     * lands, and a path into a column is one the question cannot be answered for.
     */
    @Test
    public void testAColumnChangeIsJudgedOnlyOnceTheTableHasIt() throws Exception {
        String db = "ivm_column_change_reached";
        createPartitionedTableWithoutRowBinlog(db);
        OlapTable baseTable = getBaseTable(db);

        RenameColumnOp pendingRename = new RenameColumnOp("v1", "v1_renamed");
        Assertions.assertFalse(pendingRename.hasReachedTheTable(baseTable),
                "the name the query spells is still in the table, so the rename has not reached it");
        DropColumnOp pendingDrop = new DropColumnOp("v1", null, null);
        Assertions.assertFalse(pendingDrop.hasReachedTheTable(baseTable),
                "the column is still in the table, so the drop has not reached it");

        executeSql("ALTER TABLE ivm_base RENAME COLUMN v1 v1_renamed");

        Assertions.assertTrue(pendingRename.hasReachedTheTable(getBaseTable(db)),
                "the old name is gone from the table, so the rename has reached it");

        DropColumnOp dropAfterRename = new DropColumnOp("v1_renamed", null, null);
        Assertions.assertFalse(dropAfterRename.hasReachedTheTable(getBaseTable(db)));
        executeSql("ALTER TABLE ivm_base DROP COLUMN v1_renamed");
        Assertions.assertTrue(dropAfterRename.hasReachedTheTable(getBaseTable(db)),
                "the column is gone from the table, so the drop has reached it");
        Assertions.assertTrue(pendingDrop.hasReachedTheTable(getBaseTable(db)),
                "and the name that drop asks about is one the table does not have either");

        // A path that addresses part of a column leaves the column itself in place, so the question has no
        // answer: the change is treated as one that has not reached the table, which keeps the views
        // invalidated rather than cleared on a look that cannot see the part.
        DropColumnOp nestedDrop = new DropColumnOp(ColumnPath.fromDotName("k1.leaf"), null, null);
        Assertions.assertFalse(nestedDrop.hasReachedTheTable(getBaseTable(db)));
    }

    /**
     * Changing a column's type is not judged by the query, and this is the case that says why: the query
     * still analyses -- it is projected or not, which is all the check compares -- while the rows the MV
     * already holds were computed under the old type. The operation invalidates on its own then, whether or
     * not the MV reads the column, which is what keeps a predicate that now compares something else from
     * being applied to rows that were filtered under the old meaning.
     */
    @Test
    public void testATypeChangeInvalidatesWithoutAskingTheQuery() throws Exception {
        String db = "ivm_type_change";
        createPartitionedTableWithoutRowBinlog(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertFalse(mtmv.isIvm());

        executeSql("ALTER TABLE ivm_base ADD COLUMN spare int");
        executeSql("ALTER TABLE ivm_base MODIFY COLUMN spare bigint");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        // The change is recorded as the base-table change it is, not as a query that went away: the query
        // is still there, and the reason is what tells a reader which of the two happened.
        Assertions.assertFalse(
                mtmv.getStatus().getSchemaChangeDetail().contains("no longer analyzable"),
                "the query is untouched by a type change, was: " + mtmv.getStatus().getSchemaChangeDetail());
    }

    /**
     * A subquery that answers for a name of its own is a dependency only where the table being changed is
     * one that subquery reads: what a name falls back to is what the scope that used to answer for it
     * holds, so a scope that never read the table holds nothing for it. Two sibling subqueries are the
     * shape -- the changed table is one only the first reads, and the `flag` of the second is its own --
     * where reading every subquery's own names reads a dependency the change cannot have made.
     */
    @Test
    public void testASubqueryTheChangeDoesNotReachIsNotADependency() throws Exception {
        String db = "ivm_column_change_sibling_scopes";
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (id int, flag int) DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE " + db + ".ivm_other (u int) DUPLICATE KEY(u)\n"
                + "DISTRIBUTED BY HASH(u) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE " + db + ".ivm_spare (id int) DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT o.id FROM ivm_base o\n"
                + "WHERE EXISTS (SELECT 1 FROM ivm_spare c WHERE c.id = o.id)\n"
                + "  AND EXISTS (SELECT 1 AS flag, COUNT(*) AS n FROM ivm_other u\n"
                + "      GROUP BY flag HAVING flag = 1)");
        MTMV mtmv = getMtmv(db);

        long versionBefore = mtmv.getSchemaChangeVersion();
        executeSql("ALTER TABLE ivm_spare ADD COLUMN flag int default 0");
        assertNotInvalidated(mtmv, versionBefore,
                "a name a subquery names itself, where the changed table is not one that subquery reads,"
                        + " must not invalidate the MV");
    }

    /**
     * A subquery inside another one is judged by its own output being read: an EXISTS is asked whether it
     * has a row and never what it projects, and an IN around it reads its own subquery, not the scopes
     * inside it. Adding the column the inner projection is then read from leaves every row of the query
     * where it was, so the MV is left alone -- while a column the IN itself compares is not.
     */
    @Test
    public void testAProjectionTheSubqueryAroundItDoesNotReadIsNotADependency() throws Exception {
        String db = "ivm_column_change_nested_scope";
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (id int, k int) DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE " + db + ".ivm_mid (k int, id int, spare int) DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE " + db + ".ivm_spare (id int) DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT o.id FROM ivm_base o\n"
                + "WHERE o.k IN (SELECT m.k FROM ivm_mid m\n"
                + "    WHERE EXISTS (SELECT spare FROM ivm_spare i WHERE i.id = m.id))");
        MTMV mtmv = getMtmv(db);

        long versionBefore = mtmv.getSchemaChangeVersion();
        executeSql("ALTER TABLE ivm_spare ADD COLUMN spare int default 0");
        assertNotInvalidated(mtmv, versionBefore,
                "a projection an EXISTS does not compare, inside a subquery the IN around it reads the rows"
                        + " of, must not invalidate the MV");
    }

    /**
     * A name in a view's own definition has to say which column it was bound to, wherever it is written.
     * `QUALIFY` reads a base table's column while that column is there and whatever else answers for the
     * name once it is not -- here the view's own `v1`, whose output is a constant -- which changes the rows
     * the query returns without changing the columns the view produces. The judgement re-analyses this
     * definition, so a name it never bound to a column is one it cannot hold the change against, and the
     * view is left holding rows the query no longer returns. Pinned as the reason it is invalidated with:
     * the query is gone, because the column it named is.
     */
    @Test
    public void testAQualifyNameIsBoundToItsColumnInTheStoredDefinition() throws Exception {
        String db = "ivm_qualify_bound_name";
        createPartitionedTableWithoutRowBinlog(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT 1 AS v1, ROW_NUMBER() OVER (ORDER BY dt) AS rn FROM ivm_base\n"
                + "QUALIFY v1 = 1 AND rn = 1");
        MTMV mtmv = getMtmv(db);

        executeSql("ALTER TABLE ivm_base DROP COLUMN v1");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        assertUnanalyzableDetail(mtmv);
    }

    /**
     * Which MV partitions must be rebuilt is decided by the MV's partition mapping, not by what the
     * refresh snapshot happens to record. This test publishes no snapshot at all: an MV whose partitions
     * follow the base table's still narrows the rebuild down to the partitions that read the dropped one.
     */
    @Test
    public void testDropPartitionMarksOnlyMvPartitionsThatReadIt() throws Exception {
        String db = "ivm_partitions_baseline_rebuild";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        Assertions.assertEquals(2, mtmv.getPartitionNames().size());
        Set<String> expected = mvPartitionsWithSameRange(mtmv, getBaseTable(db), "p202001");
        Assertions.assertEquals(1, expected.size());

        alignStatesOf(mtmv);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        // The partitions that read the dropped one have their requirement raised, and only those: every
        // other partition keeps catching up incrementally, and no whole-MV barrier is raised.
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        for (String partitionName : mtmv.getPartitionNames()) {
            long expectedLatest = expected.contains(partitionName) ? 2 : 1;
            Assertions.assertEquals(expectedLatest,
                    mtmv.getPartitionStates().get(partitionName).getLatestEpoch());
        }
    }

    /**
     * The other half of an invalidation: the partitions it marks lose their refresh snapshot, which is what
     * keeps transparent rewrite away from them until the rebuild has replaced their rows.
     */
    @Test
    public void testInvalidationDropsTheSnapshotsOfThePartitionsItMarks() throws Exception {
        String db = "ivm_invalidation_drops_snapshots";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        Set<String> expected = mvPartitionsWithSameRange(mtmv, getBaseTable(db), "p202001");
        Assertions.assertEquals(1, expected.size());
        Map<String, MTMVRefreshPartitionSnapshot> snapshots = Maps.newHashMap();
        for (String partitionName : mtmv.getPartitionNames()) {
            snapshots.put(partitionName, new MTMVRefreshPartitionSnapshot());
        }
        mtmv.getRefreshSnapshot().updateSnapshots(snapshots, mtmv.getPartitionNames());
        alignStatesOf(mtmv);

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().keySet().stream()
                .anyMatch(expected::contains));
        Assertions.assertFalse(mtmv.getRefreshSnapshot().getPartitionSnapshots().isEmpty());
    }

    /**
     * What a partition-scoped invalidation writes to the journal: the partitions it marked, as the delta they
     * are, together with the snapshots it dropped.
     *
     * <p>The MV's whole state map is the wrong shape for it twice over. A partition DDL reaches a few
     * partitions of an MV that may have very many, so a record carrying every entry copies and journals all of
     * them, under the MV write lock, to say what one of them now requires; and a replay that replaced the map
     * with it would drop the entries other records -- an alignment, the raise a refresh makes -- wrote in
     * between. The delta is replayed over those entries rather than in place of them.
     */
    @Test
    public void testAPartitionScopedInvalidationJournalsOnlyWhatItMarked() throws Exception {
        String db = "ivm_invalidation_journals_the_delta";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        alignStatesOf(mtmv);
        Map<String, MTMVPartitionState> before = mtmv.getPartitionStates();
        Assertions.assertEquals(2, before.size());
        Set<String> marked = mvPartitionsWithSameRange(mtmv, getBaseTable(db), "p202001");
        Assertions.assertEquals(1, marked.size());
        String other = before.keySet().stream().filter(name -> !marked.contains(name)).findFirst().get();
        Map<String, MTMVRefreshPartitionSnapshot> snapshots = Maps.newHashMap();
        for (String partitionName : before.keySet()) {
            snapshots.put(partitionName, new MTMVRefreshPartitionSnapshot());
        }

        List<AlterMTMV> journaled = journalPartitionStates(
                () -> executeSql("ALTER TABLE ivm_base DROP PARTITION p202001"));

        Assertions.assertEquals(1, journaled.size());
        AlterMTMV payload = journaled.get(0);
        Assertions.assertTrue(payload.isMergePartitionStates());
        Assertions.assertEquals(marked, payload.getPartitionStates().keySet());
        Assertions.assertEquals(marked, payload.getRemovedSnapshotPartitions());
        Assertions.assertEquals(2L, payload.getPartitionStates().values().iterator().next().getLatestEpoch());

        // What a restart does with it: the states go back to what they were, a record that ran in between
        // moves the partition this payload does not name, and then this one is replayed over both.
        mtmv.replayAlterPartitionStates(before, Collections.emptySet(), false);
        mtmv.replayAlterPartitionStates(Collections.singletonMap(other, new MTMVPartitionState(1, 4)),
                Collections.emptySet(), true);
        mtmv.getRefreshSnapshot().updateSnapshots(snapshots, before.keySet());
        mtmv.replayAlterPartitionStates(payload.getPartitionStates(), payload.getRemovedSnapshotPartitions(),
                payload.isMergePartitionStates());

        // The partition it names is raised and lost its snapshot; the one it does not keeps both, which is
        // what the merge buys -- replacing the map with the delta would have dropped its entry.
        Assertions.assertEquals(2L, mtmv.getPartitionStates().get(marked.iterator().next()).getLatestEpoch());
        Assertions.assertEquals(4L, mtmv.getPartitionStates().get(other).getLatestEpoch());
        Assertions.assertEquals(Sets.newHashSet(other),
                mtmv.getRefreshSnapshot().getPartitionSnapshots().keySet());
    }

    /**
     * A base partition that no MV partition reads: dropping it cannot leave any of its rows in the MV, so
     * there is nothing to rebuild. The previous selection could not tell this apart from "the snapshot does
     * not know this partition" and rebuilt the whole MV instead.
     */
    @Test
    public void testDropPartitionOutsideMvPartitionsMarksNothing() throws Exception {
        String db = "ivm_partition_outside_mv";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        // Added after the MV was created and never synced into it, so no MV partition reads it.
        executeSql("ALTER TABLE ivm_base ADD PARTITION p202003 "
                + "VALUES [('2020-03-01'), ('2020-04-01'))");
        Assertions.assertTrue(mvPartitionsWithSameRange(mtmv, getBaseTable(db), "p202003").isEmpty());

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202003");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * A base partition the partition_sync_limit window no longer covers can still have its rows in an MV
     * partition: the MV was built while that partition was inside the window, shrinking the window does not
     * touch the MV's own partitions, and widening it again makes partition sync keep the partition holding
     * those rows. TRUNCATE emits no binlog, so nothing incremental can repair them -- the whole MV has to be
     * rebuilt rather than a partition being guessed at.
     */
    @Test
    public void testChangedPartitionOutsideTheSyncWindowRebuildsTheWholeMv() throws Exception {
        String db = "ivm_baseline_sync_window";
        String thisYear = LocalDate.now().withDayOfYear(1).toString();
        // The cutoff is now() truncated to the year, read when the marker runs, and a partition is kept
        // while its upper bound is after it. The recent partition therefore ends more than one year out:
        // a year boundary falling between building this DDL and marking the change would otherwise put
        // its upper bound exactly on the cutoff, drop it from the mapping, and let this test pass through
        // the "nothing was selected" answer it exists to rule out.
        String recentEnd = LocalDate.now().withDayOfYear(1).plusYears(2).toString();
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01')),\n"
                + "  PARTITION pThisYear VALUES [('" + thisYear + "'), ('" + recentEnd + "'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PARTITION BY(dt)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertEquals(3, mtmv.getPartitionNames().size());

        // The window now keeps only this year's partition, so p202001 leaves the mapping while the MV's own
        // partition for it stays. TRUNCATE leaves the base partition in place, so partition sync would keep
        // that MV partition too -- the rows it still holds are exactly what the rebuild has to remove.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '1',"
                + " 'partition_sync_time_unit' = 'YEAR')");
        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * The same window, with a change that touches a partition inside it and one outside it at once: the
     * partition inside fills the selection, and the one outside contributes nothing because the window
     * left it out of the mapping. Judging the change by "was anything selected" would mark only the MV
     * partition backed by the inside half, and the rows of the outside half -- which the MV partition for
     * it still holds -- would never be rebuilt.
     */
    @Test
    public void testChangeThatMixesInWindowAndOutOfWindowPartitionsRebuildsTheWholeMv() throws Exception {
        String db = "ivm_baseline_sync_window_mixed";
        String thisYear = LocalDate.now().withDayOfYear(1).toString();
        // The cutoff is now() truncated to the year, read when the marker runs, and a partition is kept
        // while its upper bound is after it. The recent partition therefore ends more than one year out:
        // a year boundary falling between building this DDL and marking the change would otherwise put
        // its upper bound exactly on the cutoff, drop it from the mapping, and let this test pass through
        // the "nothing was selected" answer it exists to rule out.
        String recentEnd = LocalDate.now().withDayOfYear(1).plusYears(2).toString();
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01')),\n"
                + "  PARTITION pThisYear VALUES [('" + thisYear + "'), ('" + recentEnd + "'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PARTITION BY(dt)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertEquals(3, mtmv.getPartitionNames().size());

        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '1',"
                + " 'partition_sync_time_unit' = 'YEAR')");
        // One statement, so the marker sees both partitions together: pThisYear is inside the window while
        // p202001 is not, which is exactly the mix a non-empty selection must not be allowed to hide.
        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001, pThisYear)");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * A window property decides which base partitions the MV maintains. Only a change that can bring a
     * partition back into that set needs a complete baseline rebuild: its deltas were skipped while it was
     * outside, so nothing incremental can repair them. A window that starts applying, a narrower one and
     * one that describes the same set as before all leave the deltas that were applied intact, and the
     * partitions they take out are dropped by partition sync before the refresh plans.
     */
    @Test
    public void testOnlyAWiderSyncWindowRequiresCompleteBaselineRebuild() throws Exception {
        String db = "ivm_sync_window_property_change";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        // No limit is in effect, so the unit it is paired with decides nothing.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_time_unit' = 'YEAR')");
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        // The window starts applying: it takes partitions out of what the MV maintains, it brings none back.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '10')");
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        // The same window, restated.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '10')");
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        // Narrower: it only removes partitions from the maintained set.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '1')");
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        // Wider: the partitions it takes back in skipped their deltas while they were outside.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '10')");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        resetMvState(mtmv);

        // The limit is gone: every partition comes back.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '0')");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        resetMvState(mtmv);

        // Still no limit in effect, so the unit decides nothing again.
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_time_unit' = 'DAY')");
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * The limit is read on both sides of the mapping the selection judges: the mapping is built under
     * whatever window the properties hold at that moment, and MV properties are mutable in between --
     * <code>ALTER MATERIALIZED VIEW ... SET</code> is not generation guarded, so a limit can be cleared
     * while the mapping is built. A read that happens only afterwards then sees no limit and trusts a
     * windowed mapping, and a change outside that window is answered with "no MV partition reads it",
     * which records no barrier at all. The read taken before the mapping is the one that cannot be
     * reconstructed afterwards, so this pins that the selection takes both.
     *
     * <p>The interleaving itself is not staged: the mapping is built with no injection point between the
     * two reads, so the test pins that both reads happen rather than a racy outcome.
     */
    @Test
    public void testTheSyncLimitIsReadOnBothSidesOfTheMapping() throws Exception {
        String db = "ivm_baseline_sync_limit_both_reads";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        OlapTable baseTable = getBaseTable(db);
        executeSql("ALTER MATERIALIZED VIEW ivm_mv SET ('partition_sync_limit' = '1',"
                + " 'partition_sync_time_unit' = 'YEAR')");

        try (MockedStatic<MTMVPartitionUtil> partitionUtil = Mockito.mockStatic(MTMVPartitionUtil.class,
                Mockito.CALLS_REAL_METHODS)) {
            Assertions.assertTrue(mtmv.invalidateIvmBaseline(new BaseTableInfo(baseTable),
                    Collections.singletonMap("p202001", baseTable.getPartition("p202001").getId()),
                    "test partition change"));
            partitionUtil.verify(() -> MTMVPartitionUtil.isPartitionSyncLimitActive(Mockito.any()),
                    Mockito.times(2));
        }

        // p202001 is outside the window while it is in effect, so its rows are described by no mapping
        // entry and only the limit can tell that apart from "no MV partition reads it".
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * The partition mapping is built from the MV's PCT tables only. A changed partition of a joined table
     * the MV's partition column does not reach is invisible to it, and missing such a change leaves rows
     * of the dropped partition in the MV forever, so the whole MV has to be rebuilt.
     */
    @Test
    public void testNonPctBaseTablePartitionChangeRequiresCompleteBaselineRebuild() throws Exception {
        String db = "ivm_non_pct_partition_change";
        createPartitionedIvmTable(db);
        createTable("CREATE TABLE " + db + ".ivm_dim (\n"
                + "  dt date NOT NULL,\n"
                + "  id int NOT NULL,\n"
                + "  v int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, id)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION d202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION d202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', "
                + "'binlog.format' = 'ROW')");
        // The join is on a non-partition column, so ivm_dim is a base table of the MV but not a PCT table.
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PARTITION BY(dt)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT b.dt, b.k1, b.v1 FROM ivm_base b JOIN ivm_dim d ON b.k1 = d.id");
        MTMV mtmv = getMtmv(db);
        Assertions.assertTrue(mtmv.isIvm());
        Assertions.assertEquals(Sets.newHashSet("ivm_base"),
                mtmv.getMvPartitionInfo().getPctInfos().stream()
                        .map(pctInfo -> pctInfo.getTableInfo().getTableName())
                        .collect(Collectors.toSet()));

        executeSql("ALTER TABLE ivm_dim DROP PARTITION d202001");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * A join whose condition carries the MV's partition column makes both tables PCT tables, so the mapping
     * reads both of them. The marker already holds this table's write lock, so it takes the other one with a
     * bounded tryLock: while that table is free, the rebuild is still narrowed to the partitions that read
     * the dropped one.
     */
    @Test
    public void testMultiPctTablePartitionChangeStillNarrows() throws Exception {
        String db = "ivm_multi_pct_partition_change";
        createTwoPctTableIvm(db);
        MTMV mtmv = getMtmv(db);
        Set<String> expected = mvPartitionsWithSameRange(mtmv, getBaseTable(db), "p202001");
        Assertions.assertEquals(1, expected.size());

        alignStatesOf(mtmv);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        for (String partitionName : mtmv.getPartitionNames()) {
            long expectedLatest = expected.contains(partitionName) ? 2 : 1;
            Assertions.assertEquals(expectedLatest,
                    mtmv.getPartitionStates().get(partitionName).getLatestEpoch());
        }
    }

    /**
     * The same MV, but the other PCT table is being written while the partition DDL marks. Waiting for it
     * would close a cycle with the DDL that holds it -- each would hold the write lock the other one needs --
     * so the marker gives up on the mapping and the whole MV is rebuilt.
     */
    @Test
    public void testMultiPctTableBusyOtherTableRebuildsWholeMv() throws Exception {
        String db = "ivm_multi_pct_busy";
        createTwoPctTableIvm(db);
        MTMV mtmv = getMtmv(db);
        OlapTable otherPctTable = (OlapTable) getDb(db).getTableOrMetaException("ivm_dim");
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        // The acquisitions are bounded on both sides so that a failure cannot leave this worker holding
        // the table forever: it is not a daemon, and the test asserts that it ends.
        Thread writer = new Thread(() -> {
            Assertions.assertTrue(otherPctTable.tryWriteLock(30, TimeUnit.SECONDS),
                    "the test worker should be able to take the other PCT table");
            locked.countDown();
            try {
                released.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                otherPctTable.writeUnlock();
            }
        });
        writer.start();
        Assertions.assertTrue(locked.await(30, TimeUnit.SECONDS));
        try {
            executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        } finally {
            released.countDown();
        }
        writer.join(TimeUnit.SECONDS.toMillis(30));
        Assertions.assertFalse(writer.isAlive(), "the test worker should have released the table");

        // The batch fails on its first table here, so what this covers is the whole-MV fallback; the
        // release of the locks taken before the busy one is covered in MetaLockUtilsTest.
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * A join whose condition carries the MV's partition column: both tables become PCT tables.
     */
    private void createTwoPctTableIvm(String db) throws Exception {
        createPartitionedIvmTable(db);
        createTable("CREATE TABLE " + db + ".ivm_dim (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', "
                + "'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PARTITION BY(dt)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT b.dt, b.k1, b.v1 FROM ivm_base b JOIN ivm_dim d ON b.dt = d.dt");
        MTMV mtmv = getMtmv(db);
        Assertions.assertTrue(mtmv.isIvm());
        Assertions.assertEquals(2, mtmv.getMvPartitionInfo().getPctInfos().size());
    }

    @Test
    public void testReplacePartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_replace_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");
        executeSql("ALTER TABLE ivm_base REPLACE PARTITION (p202001) "
                + "WITH TEMPORARY PARTITION (tp202001)");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testRecoverPartitionMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_recover_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        resetMvState(getMtmv(db));

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    /**
     * RECOVER PARTITION marks before the partition is added back to the table, so at that moment the
     * partition is still in the recycle bin and the mapping cannot describe it. The rebuild must not lean
     * on the DROP that came before either: its barrier is released here before the recovery.
     */
    @Test
    public void testRecoverPartitionOnPartitionedMvRequiresCompleteBaselineRebuild() throws Exception {
        String db = "ivm_recover_partition_complete";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        resetMvState(mtmv);

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    @Test
    public void testRecoverAndDropKeepGlobalBrokenState() throws Exception {
        String db = "ivm_broken_recover_and_drop";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202002");
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * RECOVER reports the recycled partition under the name it had, and a partition added after the drop
     * can be live under that name again by then, with a different range. The change is then not the one
     * the mapping describes: the recovered range is the one whose rows have to come back, and its MV
     * partition -- which partition sync adds when the recovered partition returns -- is read from a base
     * partition that the replacement does not describe at all. Narrowing to the replacement's MV
     * partitions would leave that one out, and recovery emits no row binlog to fill it later, so the
     * whole MV has to be rebuilt.
     */
    @Test
    public void testRecoveredPartitionWhoseNameWasReusedRebuildsTheWholeMv() throws Exception {
        String db = "ivm_recover_partition_name_reused";
        createPartitionedIvmTableAndPartitionedMv(db);
        MTMV mtmv = getMtmv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        resetMvState(mtmv);
        // Live again under the dropped name, with a range no MV partition covers: the RECOVER below is
        // still about the recycled partition, not about this one.
        executeSql("ALTER TABLE ivm_base ADD PARTITION p202001 VALUES [('2020-04-01'), ('2020-05-01'))");

        executeSql("RECOVER PARTITION p202001 AS p202003 FROM ivm_base");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    @Test
    public void testAddPartitionDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_add_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD PARTITION p202003 "
                + "VALUES [('2020-03-01'), ('2020-04-01'))");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testDropTempPartitionDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_drop_temp_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");

        executeSql("ALTER TABLE ivm_base DROP TEMPORARY PARTITION tp202001");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testDropMissingPartitionIfExistsDoesNotMarkBaselineRebuild() throws Exception {
        String db = "ivm_broken_drop_missing_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION IF EXISTS p_missing");

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testRenameTableMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_rename_table";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        alignStatesOf(mtmv);
        Map<String, Long> before = latestEpochsOf(mtmv);

        executeSql("ALTER TABLE ivm_base RENAME ivm_base_renamed");

        // A rename leaves every column alone, so it raises no partition's requirement: no partition's rows
        // have to be recomputed, and an epoch is not the place to record this change. What the rename does
        // move is the MV state, through the shared base-table hook: the MV query still spells the old name,
        // so it no longer analyzes, and the state is what sends the next refresh to a whole-MV COMPLETE
        // rather than let it report SUCCESS over rows it can no longer recompute.
        Assertions.assertEquals(before, latestEpochsOf(mtmv));
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    /**
     * The state a rename sets is the shared hook's, not IVM's: a non-IVM MV gets it for the same reason --
     * its refresh re-analyzes the query under it -- and an IVM MV is not exempt. What an IVM MV has instead
     * of a re-analysis is the epoch state, and that is untouched by a rename, which is the division of
     * labour the two tests around this one pin.
     */
    @Test
    public void testRenameStillInvalidatesANonIvmMv() throws Exception {
        String db = "ivm_broken_rename_non_ivm";
        createPartitionedIvmTable(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        MTMV mtmv = getMtmv(db);
        Assertions.assertFalse(mtmv.isIvm());

        executeSql("ALTER TABLE ivm_base RENAME ivm_base_renamed");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    @Test
    public void testRenameTableBackStillRequiresAWholeMvRefresh() throws Exception {
        String db = "ivm_broken_rename_table_back";
        createPartitionedIvmTableAndMv(db);

        MTMV mtmv = getMtmv(db);
        alignStatesOf(mtmv);
        Map<String, Long> before = latestEpochsOf(mtmv);
        executeSql("ALTER TABLE ivm_base RENAME ivm_base_renamed");
        executeSql("ALTER TABLE ivm_base_renamed RENAME ivm_base");

        // Renaming the table back makes the MV query analyzable again, but the state stays where the first
        // rename put it, and the second rename is why: the dependencies are registered under the name the MV
        // query spells, so a rename of the table away from that name finds nothing to update and the rename
        // back finds nothing to clear. The cost of the round trip is one whole-MV COMPLETE refresh, paid on
        // the next refresh, which is what a rename is worth here -- the state is the only durable thing that
        // can carry the fact that the MV was un-analyzable in between, and a strict INCREMENTAL refresh is
        // not the way to find that out. Neither direction raises a partition requirement: no rows moved.
        Assertions.assertEquals(before, latestEpochsOf(mtmv));
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    @Test
    public void testReplaceTableMarksBaselineRebuild() throws Exception {
        String db = "ivm_broken_replace_table";
        createPartitionedIvmTableAndMv(db);
        createTable("CREATE TABLE " + db + ".ivm_new_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW')");

        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'false')");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testReconcileIvmStreamAfterTableReplace() throws Exception {
        String db = "ivm_reconcile_replace_table";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        Database database = getDb(db);
        OlapTable originalBaseTable = getBaseTable(db);
        String streamName = IvmUtil.streamName(mtmv.getId(), originalBaseTable.getFullQualifiers());
        OlapTableStream originalStream = (OlapTableStream) database.getTableOrMetaException(streamName);
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase(db);

        reconcileIvmStreams(mtmv, ctx);
        Assertions.assertSame(originalStream, database.getTableOrMetaException(streamName));

        createTable("CREATE TABLE " + db + ".ivm_new_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'false')");

        OlapTable replacedBaseTable = getBaseTable(db);
        OlapTableStream staleStream = (OlapTableStream) database.getTableOrMetaException(streamName);
        Assertions.assertFalse(IvmUtil.isIvmStreamUsable(staleStream, replacedBaseTable));

        reconcileIvmStreams(mtmv, ctx);
        OlapTableStream reconciledStream = (OlapTableStream) database.getTableOrMetaException(streamName);
        Assertions.assertNotSame(originalStream, reconciledStream);
        Assertions.assertEquals(replacedBaseTable.getId(), reconciledStream.getBaseTableNullable().getId());
    }

    private void reconcileIvmStreams(MTMV mtmv, ConnectContext ctx) throws Exception {
        MTMVPlanUtil.QueryAnalysisResult result = MTMVPlanUtil.getBaseTableFromQuery(mtmv.getQuerySql(), ctx);
        MTMVRelation relation = MTMVPlanUtil.generateMTMVRelation(
                result.getAllLevelTables(), result.getOneLevelTables());
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.invoke(task, "reconcileIvmStreams", ctx);
    }

    @Test
    public void testReplaceTableSwapMarksBothSidesBaselineRebuild() throws Exception {
        String db = "ivm_broken_replace_table_swap";
        createPartitionedIvmTableAndMv(db);
        createTable("CREATE TABLE " + db + ".ivm_new_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_new_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_new_base");
        MTMV oldSideMtmv = getMtmv(db);
        MTMV newSideMtmv = (MTMV) getDb(db).getTableOrMetaException("ivm_new_mv");
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, oldSideMtmv.getStatus().getState());
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, newSideMtmv.getStatus().getState());

        executeSql("ALTER TABLE ivm_base REPLACE WITH TABLE ivm_new_base PROPERTIES('swap' = 'true')");

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, oldSideMtmv.getStatus().getState());
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, newSideMtmv.getStatus().getState());
    }

    @Test
    public void testReplayDropPartitionDoesNotCreateBarrier() throws Exception {
        String db = "ivm_broken_replay_drop_partition";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        Partition partition = table.getPartition("p202001");

        DropPartitionInfo info = new DropPartitionInfo(database.getId(), table.getId(), partition.getId(),
                "p202001", false, false, 0L, table.getVisibleVersion(), table.getVisibleVersionTime());
        Env.getCurrentInternalCatalog().replayDropPartition(info);

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testReplayTruncateDoesNotCreateBarrier() throws Exception {
        String db = "ivm_broken_replay_truncate";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        Partition oldPartition = table.getPartition("p202001");
        Partition newPartition = new Partition(Env.getCurrentEnv().getNextId(), oldPartition.getName(),
                new MaterializedIndex(table.getBaseIndexId(), MaterializedIndex.IndexState.NORMAL),
                oldPartition.getDistributionInfo());

        TruncateTableInfo info = new TruncateTableInfo(database.getId(), database.getFullName(), table.getId(),
                table.getName(), Collections.singletonList(newPartition), false,
                "TRUNCATE TABLE ivm_base PARTITION(p202001)", Collections.singletonList(oldPartition), true,
                Collections.emptyMap(), table.getNextVersion(), System.currentTimeMillis());
        Env.getCurrentInternalCatalog().replayTruncateTable(info);

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testReplayReplacePartitionDoesNotCreateBarrier() throws Exception {
        String db = "ivm_broken_replay_replace_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);

        ReplacePartitionOperationLog log = new ReplacePartitionOperationLog(database.getId(), database.getFullName(),
                table.getId(), table.getName(), Collections.singletonList("p202001"),
                Collections.singletonList("tp202001"), Collections.emptyList(), false, false,
                table.getVisibleVersion(), table.getVisibleVersionTime(), false);
        Env.getCurrentEnv().replayReplaceTempPartition(log);

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testReplayRecoverPartitionDoesNotCreateBarrier() throws Exception {
        String db = "ivm_broken_replay_recover_partition";
        createPartitionedIvmTableAndMv(db);
        Database database = getDb(db);
        OlapTable table = getBaseTable(db);
        long partitionId = table.getPartition("p202001").getId();
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        resetMvState(getMtmv(db));

        RecoverInfo info = new RecoverInfo(database.getId(), table.getId(), partitionId, "", table.getName(),
                "", "p202001", null);
        Env.getCurrentInternalCatalog().replayRecoverPartition(info);

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    @Test
    public void testStaleTaskResultDoesNotMutateMtmv() throws Exception {
        String db = "ivm_stale_task_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        mtmv.invalidateWholeMv("seed").await();
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        long taskVersion = mtmv.getSchemaChangeVersion();
        Deencapsulation.setField(mtmv, "schemaChangeVersion", taskVersion + 1);
        int historySize = mtmv.getHistoryTasks().size();

        AlterMTMV result = taskResult(mtmv, TaskStatus.FAILED, taskVersion);
        Deencapsulation.setField(result.getTask(), "refreshedIvmPlanSignature", "new_signature");
        String planSignature = mtmv.getIvmInfo().getPlanSignature();

        Assertions.assertFalse(mtmv.addTaskResult(result, false));
        Assertions.assertEquals(historySize, mtmv.getHistoryTasks().size());
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        Assertions.assertEquals(planSignature, mtmv.getIvmInfo().getPlanSignature());
        Assertions.assertEquals(taskVersion + 1, mtmv.getSchemaChangeVersion());
    }

    @Test
    public void testIvmRefreshStartRejectsAStaleSchemaChangeVersion() throws Exception {
        String db = "ivm_refresh_start_validation";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long version = mtmv.getSchemaChangeVersion();

        mtmv.validateIvmRefreshStart(version);
        Assertions.assertThrows(JobException.class, () -> mtmv.validateIvmRefreshStart(version + 1));
    }

    @Test
    public void testReplayTaskResultAppliesIvmStateWithoutChangingVersion() throws Exception {
        String db = "ivm_replay_task_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        long schemaChangeVersion = mtmv.getSchemaChangeVersion();
        AlterMTMV result = taskResult(mtmv, TaskStatus.FAILED, schemaChangeVersion);
        IvmInfo replayedInfo = mtmv.getIvmInfo();
        replayedInfo.setPlanSignature("replayed_signature");
        result.setIvmInfo(replayedInfo);

        Assertions.assertTrue(mtmv.addTaskResult(result, true));
        Assertions.assertEquals("replayed_signature", mtmv.getIvmInfo().getPlanSignature());
        Assertions.assertEquals(schemaChangeVersion, mtmv.getSchemaChangeVersion());
    }

    @Test
    public void testSuccessfulResultKeepsThePlanSignature() throws Exception {
        String db = "ivm_successful_baseline_result";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        String planSignature = mtmv.getIvmInfo().getPlanSignature();
        AlterMTMV result = taskResult(mtmv, TaskStatus.SUCCESS, mtmv.getSchemaChangeVersion());
        boolean compatibilityMode = Config.enable_check_compatibility_mode;
        try {
            Config.enable_check_compatibility_mode = true;
            Assertions.assertTrue(mtmv.addTaskResult(result, false));
        } finally {
            Config.enable_check_compatibility_mode = compatibilityMode;
        }

        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        Assertions.assertEquals(planSignature, mtmv.getIvmInfo().getPlanSignature());
    }

    @Test
    public void testSuccessfulSignatureFallbackPublishesNewPlanSignature() throws Exception {
        String db = "ivm_successful_signature_fallback";
        createPartitionedIvmTableAndMv(db);
        MTMV mtmv = getMtmv(db);
        AlterMTMV result = taskResult(mtmv, TaskStatus.SUCCESS, mtmv.getSchemaChangeVersion());
        Deencapsulation.setField(result.getTask(), "refreshedIvmPlanSignature", "new_signature");
        boolean compatibilityMode = Config.enable_check_compatibility_mode;
        try {
            Config.enable_check_compatibility_mode = true;
            Assertions.assertTrue(mtmv.addTaskResult(result, false));
        } finally {
            Config.enable_check_compatibility_mode = compatibilityMode;
        }

        Assertions.assertEquals("new_signature", mtmv.getIvmInfo().getPlanSignature());
        Assertions.assertEquals("new_signature", result.getIvmInfo().getPlanSignature());
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }


    private void createPartitionedIvmTableAndMv(String db) throws Exception {
        createPartitionedIvmTable(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        assertFreshMv(db);
    }

    /**
     * The same base table, but the MV follows the base table's partitions: it is created with one MV
     * partition per base partition, so a partition change can be narrowed to the MV partitions that read
     * the changed one.
     */
    private void createPartitionedIvmTableAndPartitionedMv(String db) throws Exception {
        createPartitionedIvmTable(db);
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PARTITION BY(dt)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        assertFreshMv(db);
    }

    private void assertFreshMv(String db) throws Exception {
        Assertions.assertTrue(getMtmv(db).isIvm());
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, getMtmv(db).getStatus().getState());
    }

    private void createPartitionedIvmTable(String db) throws Exception {
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
    }

    /**
     * The same base table without row binlog. A row binlog table only accepts the operations marked as safe
     * for it, and a column rename or a type change is not one of them, so the changes that need one of those
     * are pinned against a plain table and a plain MV -- which is where those two operations reach the
     * shared base-table hook at all.
     */
    private void createPartitionedTableWithoutRowBinlog(String db) throws Exception {
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
    }

    /**
     * Gives every MV partition the entry a refresh would have created. A refresh task aligns before it
     * reads a base table, so an MV that has been refreshed has one entry per partition; the marker tests
     * drive the marker on its own, so they set that state up directly.
     */
    private void alignStatesOf(MTMV mtmv) {
        Map<String, MTMVPartitionState> aligned = Maps.newHashMap();
        for (String partitionName : mtmv.getPartitionNames()) {
            aligned.put(partitionName, MTMVPartitionState.initial());
        }
        mtmv.alterPartitionStates(aligned);
    }

    /**
     * What each MV partition currently requires, keyed by partition name. The requirement is what an
     * invalidation raises, so comparing two of these is how "nothing was marked" is observed.
     */
    private Map<String, Long> latestEpochsOf(MTMV mtmv) {
        Map<String, Long> res = Maps.newHashMap();
        for (Entry<String, MTMVPartitionState> entry : mtmv.getPartitionStates().entrySet()) {
            res.put(entry.getKey(), entry.getValue().getLatestEpoch());
        }
        return res;
    }

    private MTMV getMtmv(String db) throws Exception {
        return (MTMV) Env.getCurrentInternalCatalog()
                .getDb(db).get()
                .getTableOrMetaException("ivm_mv");
    }

    /**
     * The detail a whole-MV invalidation records when the query can no longer be analyzed. It is the only
     * record of that change, so the reason has to survive in it: a state alone cannot say why.
     */
    private void assertUnanalyzableDetail(MTMV mtmv) {
        Assertions.assertTrue(
                mtmv.getStatus().getSchemaChangeDetail().contains("no longer analyzable"),
                "the detail must name the reason, was: " + mtmv.getStatus().getSchemaChangeDetail());
    }

    /**
     * The MV was left alone by a base-table change. An invalidation moves three things -- the state, the
     * version that discards a task result computed against it, and the snapshot that stops the transparent
     * rewrite -- so the state alone is only a third of the answer, and the version is the one of the three
     * that is observable after a change that names no snapshot.
     */
    private void assertNotInvalidated(MTMV mtmv, long versionBefore, String message) {
        Assertions.assertNotEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState(), message);
        Assertions.assertEquals(versionBefore, mtmv.getSchemaChangeVersion(), message);
    }

    private Database getDb(String db) {
        return Env.getCurrentInternalCatalog().getDb(db).get();
    }

    private OlapTable getBaseTable(String db) throws Exception {
        return (OlapTable) getDb(db).getTableOrMetaException("ivm_base");
    }

    /**
     * The MV partitions whose range is exactly the range of the given base partition, derived from the two
     * tables' partition items rather than from the mapping the implementation under test computes.
     */
    private Set<String> mvPartitionsWithSameRange(MTMV mtmv, OlapTable baseTable, String basePartitionName) {
        PartitionItem basePartitionItem = baseTable.getPartitionInfo()
                .getItem(baseTable.getPartition(basePartitionName).getId());
        Set<String> res = Sets.newHashSet();
        for (Entry<String, PartitionItem> entry : mtmv.getAndCopyPartitionItems().entrySet()) {
            if (entry.getValue().toPartitionKeyDesc().equals(basePartitionItem.toPartitionKeyDesc())) {
                res.add(entry.getKey());
            }
        }
        return res;
    }

    /** Puts the MV back to a state where only a new invalidation can move it. */
    private void resetMvState(MTMV mtmv) {
        mtmv.alterStatus(new MTMVStatus(MTMVState.NORMAL, "reset"));
    }

    /** An action that may throw, for {@link #journalPartitionStates}. */
    private interface JournalingAction {
        void run() throws Exception;
    }

    /**
     * Runs {@code action} with the edit log spied, collecting the partition-state payloads it journals. The
     * spy delegates, so the journal the test's own DDL writes is still written.
     */
    private List<AlterMTMV> journalPartitionStates(JournalingAction action) throws Exception {
        List<AlterMTMV> journaled = Lists.newArrayList();
        Env env = Env.getCurrentEnv();
        EditLog original = env.getEditLog();
        EditLog spyEditLog = Mockito.spy(original);
        Mockito.doAnswer(invocation -> {
            Writable writable = invocation.getArgument(1);
            if (writable instanceof AlterMTMV
                    && ((AlterMTMV) writable).getOpType() == MTMVAlterOpType.ALTER_PARTITION_STATES) {
                journaled.add((AlterMTMV) writable);
            }
            return invocation.callRealMethod();
        }).when(spyEditLog).submitEdit(Mockito.anyShort(), Mockito.any(Writable.class));
        env.setEditLog(spyEditLog);
        try {
            action.run();
        } finally {
            env.setEditLog(original);
        }
        return journaled;
    }

    private AlterMTMV taskResult(MTMV mtmv, TaskStatus status, long schemaChangeVersion) {
        MTMVTask task = new MTMVTask();
        task.setStatus(status);
        Deencapsulation.setField(task, "mtmvSchemaChangeVersion", schemaChangeVersion);
        AlterMTMV result = new AlterMTMV(
                new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), MTMVAlterOpType.ADD_TASK);
        result.setTask(task);
        result.setRelation(mtmv.getRelation());
        result.setPartitionSnapshots(Collections.emptyMap());
        return result;
    }
}
