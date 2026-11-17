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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ForwardingMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateCommandTest extends TestWithFeService implements PlanPatternMatchSupported {
    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("create table t1 (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t2 (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table src (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "duplicate key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table generated_column_table (\n"
                + "    k1 int,\n"
                + "    v1 int,\n"
                + "    v2 int as (v1 + 1),\n"
                + "    v3 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\",\n"
                + "    \"enable_unique_key_merge_on_write\"=\"true\"\n"
                + ")");
    }

    @Test
    public void testSimpleUpdate() {
        String sql = "update t1 set v1 = v1 + 2, v2 = v1 * 2 where k1 = 3";
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, parsed);
        UpdateCommand command = ((UpdateCommand) parsed);
        LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
        PlanChecker.from(connectContext, plan)
                .analyze(plan)
                .rewrite()
                .matches(
                        logicalOlapTableSink(
                                logicalProject(
                                        logicalFilter(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }

    @Test
    public void testUpdatePlanWithConcurrentSchemaRebuild() throws Exception {
        OlapTable table = (OlapTable) RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, List.of("t1")),
                connectContext.getEnv(), Optional.empty());
        Assertions.assertTrue(table.getEnableUniqueKeyMergeOnWrite());
        Map<String, Column> columns = Deencapsulation.getField(table, "nameToColumn");
        ExecutorService schemaChange = Executors.newSingleThreadExecutor();
        AtomicBoolean schemaRebuiltDuringCopy = new AtomicBoolean();
        AtomicBoolean copyStarted = new AtomicBoolean();
        // Keep the real TreeMap iterator, but try a schema rebuild after its first entry is read.
        // Without the UPDATE read lock, clear/put in rebuildFullSchema invalidates this iterator.
        Map<String, Column> instrumentedColumns = new ForwardingMap<String, Column>() {
            @Override
            protected Map<String, Column> delegate() {
                return columns;
            }

            @Override
            public Collection<Column> values() {
                Collection<Column> values = columns.values();
                return new AbstractCollection<Column>() {
                    @Override
                    public int size() {
                        return values.size();
                    }

                    @Override
                    public Iterator<Column> iterator() {
                        Iterator<Column> iterator = values.iterator();
                        return new Iterator<Column>() {
                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public Column next() {
                                Column column = iterator.next();
                                if (copyStarted.compareAndSet(false, true)) {
                                    schemaRebuiltDuringCopy.set(Assertions.assertDoesNotThrow(() ->
                                            schemaChange.submit(() -> tryRebuildSchema(table))
                                                    .get(10, TimeUnit.SECONDS)));
                                }
                                return column;
                            }
                        };
                    }
                };
            }
        };
        Deencapsulation.setField(table, "nameToColumn", instrumentedColumns);
        try {
            UpdateCommand command = (UpdateCommand) new NereidsParser()
                    .parseSingle("update t1 set v1 = v1 + 1 where k1 = 1");
            LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
            Assertions.assertTrue(((UnboundTableSink<?>) plan).isPartialUpdate());
            Assertions.assertTrue(copyStarted.get(), "The test must reach the column-map copy");
            Assertions.assertFalse(schemaRebuiltDuringCopy.get(), "Schema changes must wait for UPDATE planning");
            Assertions.assertTrue(schemaChange.submit(() -> tryRebuildSchema(table)).get(10, TimeUnit.SECONDS),
                    "UPDATE planning must release its read lock before returning");
            PlanChecker.from(connectContext, plan).analyze(plan).rewrite();
        } finally {
            schemaChange.shutdownNow();
            Deencapsulation.setField(table, "nameToColumn", columns);
        }
    }

    @Test
    public void testUpdatePlanReleasesReadLockOnAnalysisFailure() throws Exception {
        OlapTable table = (OlapTable) RelationUtil.getTable(
                RelationUtil.getQualifierName(connectContext, List.of("t1")),
                connectContext.getEnv(), Optional.empty());
        ExecutorService schemaChange = Executors.newSingleThreadExecutor();
        try {
            UpdateCommand command = (UpdateCommand) new NereidsParser()
                    .parseSingle("update t1 set k1 = 2 where k1 = 1");
            Assertions.assertThrows(AnalysisException.class,
                    () -> command.completeQueryPlan(connectContext, command.getLogicalQuery()));
            Assertions.assertTrue(schemaChange.submit(() -> tryRebuildSchema(table)).get(10, TimeUnit.SECONDS),
                    "Failed UPDATE planning must release its read lock");
        } finally {
            schemaChange.shutdownNow();
        }
    }

    private static boolean tryRebuildSchema(OlapTable table) {
        if (!table.tryWriteLock(0, TimeUnit.SECONDS)) {
            return false;
        }
        try {
            table.rebuildFullSchema();
            return true;
        } finally {
            table.writeUnlock();
        }
    }

    @Test
    public void testDirectRowTtlUpdate() throws Exception {
        createTable("create table ttl_update(k int, v int, a int, b int, c int, d int) unique key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'function_column.enable_row_ttl'='true')");
        for (String assignment : new String[] {"v = v + 1", "__DORIS_TTL_COL__ = NULL",
                "__DORIS_TTL_COL__ = cast('2099-01-01 00:00:00.123456' as datetime(6))"}) {
            UpdateCommand command = (UpdateCommand) new NereidsParser()
                    .parseSingle("update ttl_update set " + assignment + " where k=1");
            LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
            Assertions.assertFalse(((UnboundTableSink<?>) plan).isPartialUpdate());
            String rewritten = PlanChecker.from(connectContext, plan).analyze(plan).rewrite().getPlan().treeString();
            Assertions.assertTrue(rewritten.contains("__DORIS_TTL_COL__"), rewritten);
        }
    }

    @Test
    public void testSourceRowTtlUpdateUsesFullRow() throws Exception {
        createTable("create table ttl_source_update(k int, event_time datetimev2(6), v int, a int, b int, c int) "
                + "unique key(k) distributed by hash(k) buckets 1 properties('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'function_column.enable_row_ttl'='true', "
                + "'function_column.ttl_col'='event_time', 'function_column.ttl'='0', "
                + "'function_column.ttl_time_zone'='+00:00')");
        UpdateCommand command = (UpdateCommand) new NereidsParser()
                .parseSingle("update ttl_source_update set v=v+1 where k=1");
        LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
        Assertions.assertFalse(((UnboundTableSink<?>) plan).isPartialUpdate());
        String rewritten = PlanChecker.from(connectContext, plan).analyze(plan).rewrite().getPlan().treeString();
        Assertions.assertTrue(rewritten.contains("event_time"), rewritten);
        Assertions.assertTrue(rewritten.contains("__DORIS_TTL_COL__"), rewritten);
    }

    @Test
    public void testFromClauseUpdate() {
        String sql = "update t1 a set v1 = t2.v1 + 2, v2 = a.v1 * 2 "
                + "from src join t2 on src.k1 = t2.k1 where t2.k1 = a.k1";
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, parsed);
        UpdateCommand command = ((UpdateCommand) parsed);
        LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
        PlanChecker.from(connectContext, plan)
                .analyze(plan)
                .matches(
                        logicalFilter(
                                logicalJoin(
                                        logicalSubQueryAlias(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalJoin(
                                                logicalOlapScan(),
                                                logicalFilter(
                                                        logicalOlapScan()
                                                )
                                        )
                                )
                        )
                );
    }

    @Test
    public void testPartialUpdateWithGeneratedColumnDependency() {
        String sql = "update generated_column_table set v3 = 999 where k1 = 1";
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, parsed);
        UpdateCommand command = ((UpdateCommand) parsed);
        LogicalPlan plan = command.completeQueryPlan(connectContext, command.getLogicalQuery());
        PlanChecker.from(connectContext, plan)
                .analyze(plan)
                .rewrite()
                .matches(
                        logicalOlapTableSink(
                                logicalProject(
                                        logicalFilter(
                                                logicalOlapScan()
                                        )
                                )
                        )
                );
    }
}
