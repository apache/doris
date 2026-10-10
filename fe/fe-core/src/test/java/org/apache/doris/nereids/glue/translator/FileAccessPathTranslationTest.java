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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class FileAccessPathTranslationTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("file_access_path");
        connectContext.setDatabase("file_access_path");
        // Keep the empty fixture's scan so these tests exercise access paths, not empty-relation elimination.
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        createTable("CREATE TABLE file_scan (id INT NOT NULL, f FILE, required FILE NOT NULL, "
                + "a ARRAY<FILE>, s STRUCT<f:FILE,other:INT>, m MAP<STRING,FILE>) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testLogicalGetterPaths() throws Exception {
        assertPaths("select element_at(f, 'uri') from file_scan", "f", data("f", "uri"));
        assertPaths("select element_at(required, 'size') from file_scan", "required", data("required", "size"));
        assertPaths("select element_at(a[1], 'uri') from file_scan", "a", data("a", "*", "uri"));
        assertPaths("select element_at(element_at(s,'f'), 'uri') from file_scan", "s", data("s", "f", "uri"));
        assertPaths("select element_at(m['key'], 'uri') from file_scan", "m",
                data("m", "KEYS"), data("m", "VALUES", "uri"));
    }

    @Test
    public void testInlineGetterAndNullableChildScanPaths() throws Exception {
        assertPaths("select element_at(f, 'inline') from file_scan", "f", data("f", "inline"));
        assertPaths("select element_at(required,'INLINE') from file_scan", "required", data("required", "inline"));
        assertPaths("select element_at(a[1], 'inline') from file_scan", "a", data("a", "*", "inline"));
        assertPaths("select element_at(element_at(s,'f'), 'inline') from file_scan", "s", data("s", "f", "inline"));
        assertPaths("select element_at(m['key'], 'inline') from file_scan", "m",
                data("m", "KEYS"), data("m", "VALUES", "inline"));
        assertLocalScan("select element_at(f, 'inline') from file_scan", "f", data("f", "inline"));
        assertLocalScan("select element_at(required,'INLINE') from file_scan", "required", data("required", "inline"));
        assertLocalScan("select element_at(a[1], 'inline') from file_scan", "a", data("a", "*", "inline"));
        assertLocalScan("select element_at(m['key'], 'inline') from file_scan", "m",
                data("m", "KEYS"), data("m", "VALUES", "inline"));
        assertPaths("select element_at(required, 'inline') is null from file_scan", "required",
                meta("required", "inline", "NULL"));
        assertLocalScan("select element_at(required, 'inline') is null from file_scan", "required",
                meta("required", "inline", "NULL"));
    }

    @Test
    public void testCanonicalStructCastGetterLogicalPath() throws Exception {
        String publicStruct = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT,"
                + "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>";
        assertPaths("select element_at(cast(f as " + publicStruct + "),'size') from file_scan",
                "f", data("f", "size"));
        assertPaths("select element_at(cast(f as " + publicStruct + "),3) is null from file_scan",
                "f", meta("f", "size", "NULL"));
        assertLocalScan("select element_at(cast(f as " + publicStruct + "),'size') from file_scan",
                "f", data("f", "size"));
        assertLocalScan("select element_at(cast(f as " + publicStruct + "),3) is null from file_scan",
                "f", meta("f", "size", "NULL"));
        assertPaths("select element_at(cast(f as " + publicStruct + "),6) from file_scan",
                "f", data("f", "inline"));
        assertLocalScan("select element_at(cast(f as " + publicStruct + "),'INLINE') from file_scan",
                "f", data("f", "inline"));
        assertLocalScan("select element_at(cast(f as " + publicStruct + "),6) is null from file_scan",
                "f", meta("f", "inline", "NULL"));
    }

    @Test
    public void testLogicalNullAndCountPaths() throws Exception {
        assertPaths("select f is null from file_scan", "f", meta("f", "NULL"));
        assertPaths("select element_at(f, 'size') is null from file_scan", "f", meta("f", "size", "NULL"));
        // Exercise ordinary aggregate input pruning independently of storage COUNT pushdown.
        boolean old = connectContext.getSessionVariable().enablePushDownNoGroupAgg;
        connectContext.getSessionVariable().enablePushDownNoGroupAgg = false;
        try {
            assertPaths("select count(f) from file_scan", "f", data("f"));
        } finally {
            connectContext.getSessionVariable().enablePushDownNoGroupAgg = old;
        }
    }

    @Test
    public void testLocalScalarScanProjectionBeforeExchange() throws Exception {
        assertLocalScan("select element_at(f, 'uri') from file_scan", "f", data("f", "uri"));
        assertLocalScan("select element_at(required, 'size') from file_scan", "required", data("required", "size"));
        assertLocalScan("select element_at(a[1], 'uri') from file_scan", "a", data("a", "*", "uri"));
        assertLocalScan("select element_at(element_at(s,'f'), 'uri') from file_scan", "s", data("s", "f", "uri"));
        assertLocalScan("select element_at(m['key'], 'uri') from file_scan", "m",
                data("m", "KEYS"), data("m", "VALUES", "uri"));
        assertLocalScan("select f is null from file_scan", "f", meta("f", "NULL"));
        assertLocalScan("select id from file_scan where f is null", "f", meta("f", "NULL"));
        assertLocalScan("select element_at(f, 'size') is null from file_scan", "f", meta("f", "size", "NULL"));
    }

    @Test
    public void testCountReadsFullFileThroughGenericPath() throws Exception {
        assertCompleteScan("select count(f) from file_scan", "f", data("f"));
        assertCompleteScan("select id, count(f) from file_scan group by id", "f", data("f"));
        assertCompleteScan("select count(f) from file_scan where id > 0", "f", data("f"));
        // Without a dedicated scalar scan projection, transport materializes the whole container.
        assertCompleteScan("select count(element_at(s,'f')) from file_scan", "s", data("s"));
        assertCompleteScan("select count(a[1]) from file_scan", "a", data("a"));
        assertCompleteScan("select count(m['key']) from file_scan", "m", data("m"));
        assertCompleteScan("select count(f), count(*) from file_scan", "f", data("f"));
    }

    @Test
    public void testCorrelatedFileScalarSubqueriesRejectGeneratedAggregate() {
        for (String field : ImmutableList.of("f", "a", "s", "m")) {
            String sql = "select (select i." + field
                    + " from file_scan i where i.id=o.id) from file_scan o";
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
            Assertions.assertTrue(error.getMessage().contains("any_value does not support FILE"),
                    error.getMessage());
        }
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(
                "select (select i.id from file_scan i where i.id=o.id) from file_scan o").rewrite());
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(
                "select (select element_at(i.f, 'uri') from file_scan i where i.id=o.id) from file_scan o").rewrite());
    }

    @Test
    public void testRelaxedGroupByRejectsGeneratedFileAggregate() {
        long sqlMode = connectContext.getSessionVariable().getSqlMode();
        connectContext.getSessionVariable().setSqlMode(sqlMode & ~SqlModeHelper.MODE_ONLY_FULL_GROUP_BY);
        try {
            for (String field : ImmutableList.of("f", "a", "s", "m")) {
                String sql = "select " + field + ", count(*) from file_scan";
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), sql);
                Assertions.assertTrue(error.getMessage().contains("any_value does not support FILE"),
                        error.getMessage());
            }
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(
                    "select id, count(*) from file_scan").rewrite());
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(
                    "select count(f), count(*) from file_scan").rewrite());
        } finally {
            connectContext.getSessionVariable().setSqlMode(sqlMode);
        }
    }

    @Test
    public void testFullConsumersContinueReadingEveryFileChild() throws Exception {
        Assertions.assertEquals(ImmutableList.of(data("f")),
                findSlot("select f, element_at(f, 'uri') from file_scan", "f").getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(data("a")),
                findSlot("select a, element_at(a[1], 'uri') from file_scan", "a").getAllAccessPaths());
    }

    @Test
    public void testFullFileOutputDoesNotDisableOtherFilePruning() throws Exception {
        String fullAndSize = "select f, element_at(required, 'size') from file_scan";
        Assertions.assertEquals(ImmutableList.of(data("f")),
                findSlot(fullAndSize, "f").getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(data("required", "size")),
                findSlot(fullAndSize, "required").getAllAccessPaths());

        String fullAndUri = "select required, element_at(f, 'uri') from file_scan";
        Assertions.assertEquals(ImmutableList.of(data("required")),
                findSlot(fullAndUri, "required").getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(data("f", "uri")),
                findSlot(fullAndUri, "f").getAllAccessPaths());
    }

    @Test
    public void testFullValueDominatesLocalPredicate() throws Exception {
        SlotDescriptor descriptor = findSlot("select f from file_scan where element_at(f, 'size') > 10", "f");
        Assertions.assertEquals(ImmutableList.of(data("f")), descriptor.getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(data("f", "size")), descriptor.getPredicateAccessPaths());
        Assertions.assertSame(Type.FILE, descriptor.getType());
    }

    @Test
    public void testUnprovenTransportRequestsCompleteValue() {
        SlotReference file = partialFile();
        PlanTranslatorContext context = new PlanTranslatorContext();
        SlotDescriptor descriptor = context.createSlotDesc(context.generateTupleDesc(), file);
        Assertions.assertEquals(ImmutableList.of(data("f")), descriptor.getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(meta("f", "size", "NULL")), descriptor.getPredicateAccessPaths());
        Assertions.assertSame(Type.FILE, descriptor.getType());
    }

    private void assertPaths(String sql, String column, ColumnAccessPath... paths) throws Exception {
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getCascadesContext().getRewritePlan();
        LogicalOlapScan scan = (LogicalOlapScan) plan.collect(LogicalOlapScan.class::isInstance)
                .stream().findFirst().orElseThrow(() -> new AssertionError(sql + "\n" + plan.treeString()));
        SlotReference slot = (SlotReference) scan.getOutput().stream()
                .filter(output -> output.getName().equals(column)).findFirst().orElseThrow();
        Assertions.assertEquals(new TreeSet<>(ImmutableList.copyOf(paths)),
                new TreeSet<>(slot.getAllAccessPaths().orElseThrow()), sql);
        if (column.equals("f") || column.equals("required")) {
            Assertions.assertSame(FileType.INSTANCE, slot.getDataType());
        }
    }

    private void assertCompleteScan(String sql, String column, ColumnAccessPath... paths) throws Exception {
        Assertions.assertEquals(new TreeSet<>(ImmutableList.copyOf(paths)),
                new TreeSet<>(findSlot(sql, column).getAllAccessPaths()), sql);
    }

    private SlotDescriptor findSlot(String sql, String column) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) executeNereidsSql(sql).planner();
        List<SlotDescriptor> found = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            List<OlapScanNode> scans = fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance);
            for (OlapScanNode scan : scans) {
                scan.getTupleDesc().getSlots().stream()
                        .filter(slot -> slot.getColumn() != null && slot.getColumn().getName().equals(column))
                        .forEach(found::add);
            }
        }
        Assertions.assertEquals(1, found.size(), sql);
        return found.get(0);
    }

    private void assertLocalScan(String sql, String column, ColumnAccessPath... paths) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) executeNereidsSql(sql).planner();
        List<OlapScanNode> scans = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            scans.addAll(fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance));
            if (fragment.getSink() instanceof DataStreamSink) {
                for (TupleId tuple : fragment.getPlanRoot().getOutputTupleIds()) {
                    for (SlotDescriptor output : planner.getDescTable().getTupleDesc(tuple).getSlots()) {
                        Assertions.assertFalse(output.getType().typeContainsFile(),
                                "Exchange must receive scalar results: " + sql);
                    }
                }
            }
        }
        Assertions.assertEquals(1, scans.size(), sql);
        OlapScanNode scan = scans.get(0);
        Assertions.assertNotNull(scan.getOutputTupleDesc(), sql);
        Assertions.assertNotNull(scan.getProjectList(), sql);
        Assertions.assertFalse(scan.getProjectList().isEmpty(), sql);
        for (SlotDescriptor output : scan.getOutputTupleDesc().getSlots()) {
            Assertions.assertFalse(output.getType().typeContainsFile(), sql);
        }
        SlotDescriptor file = scan.getTupleDesc().getSlots().stream()
                .filter(slot -> slot.getColumn() != null && slot.getColumn().getName().equals(column))
                .findFirst().orElseThrow();
        Assertions.assertEquals(new TreeSet<>(ImmutableList.copyOf(paths)),
                new TreeSet<>(file.getAllAccessPaths()), sql);
    }

    private SlotReference partialFile() {
        return new SlotReference("f", FileType.INSTANCE, true).withColumn(new Column("f", Type.FILE, true))
                .withAccessPaths(ImmutableList.of(data("f", "size")),
                        ImmutableList.of(meta("f", "size", "NULL")));
    }

    private static ColumnAccessPath data(String... path) {
        return ColumnAccessPath.data(ImmutableList.copyOf(path));
    }

    private static ColumnAccessPath meta(String... path) {
        return ColumnAccessPath.meta(ImmutableList.copyOf(path));
    }
}
