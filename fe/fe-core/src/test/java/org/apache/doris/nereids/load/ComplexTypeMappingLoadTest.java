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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.property.fileformat.ArrowFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Binds the complete load plan of an Arrow source whose complex column has a default_value or
 * replace_value mapping. Arrow keeps the declared ARRAY/MAP/STRUCT type in the scan slot, unlike the
 * text formats, which read the column as VARCHAR; the rewritten IF and its search comparison must
 * bind under both type coercion behaviors.
 */
public class ComplexTypeMappingLoadTest extends TestWithFeService {
    private static final String DATABASE = "complex_type_mapping_db";
    private static final String ARRAY_TABLE = "complex_mapping_array";
    private static final String MAP_TABLE = "complex_mapping_map";
    private static final String STRUCT_TABLE = "complex_mapping_struct";
    private static final String MAP_INT_KEY_TABLE = "complex_mapping_map_int_key";
    private static final String STRUCT_WITH_MAP_TABLE = "complex_mapping_struct_with_map";
    private static final String MAP_WITH_STRUCT_VALUE_TABLE = "complex_mapping_map_struct_value";
    private static final String ARRAY_STRUCT_TABLE = "complex_mapping_array_struct";
    private static final String ARRAY_DEFAULT_TABLE = "complex_mapping_array_default";
    private static final String MAP_DEFAULT_TABLE = "complex_mapping_map_default";
    private static final String STRUCT_DEFAULT_TABLE = "complex_mapping_struct_default";

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        createTable("create table " + ARRAY_TABLE + " (k int, v array<int>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + MAP_TABLE + " (k int, v map<string, int>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + STRUCT_TABLE + " (k int, v struct<f1: int, f2: string>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + MAP_INT_KEY_TABLE + " (k int, v map<int, int>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + STRUCT_WITH_MAP_TABLE
                + " (k int, v struct<f1: int, f2: map<string, int>>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + MAP_WITH_STRUCT_VALUE_TABLE
                + " (k int, v map<string, struct<f1: int>>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + ARRAY_STRUCT_TABLE
                + " (k int, v array<struct<f1: int>>) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + ARRAY_DEFAULT_TABLE + " (k int, v array<int> default '[1, 2]') "
                + "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + MAP_DEFAULT_TABLE + " (k int, v map<string, int> default '{\"a\": 1}') "
                + "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')");
        createTable("create table " + STRUCT_DEFAULT_TABLE
                + " (k int, v struct<f1: int, f2: string> default '{7, \"x\"}') "
                + "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')");
    }

    @Test
    public void testArrowComplexMappingsBindInBothCoercionModes() throws Exception {
        boolean oldBehavior = GlobalVariable.enableNewTypeCoercionBehavior;
        try {
            for (boolean newBehavior : new boolean[] {true, false}) {
                GlobalVariable.enableNewTypeCoercionBehavior = newBehavior;
                for (TableFixture fixture : ImmutableList.of(
                        new TableFixture(ARRAY_TABLE, ARRAY_DEFAULT_TABLE, "[]", "[1e3]"),
                        new TableFixture(MAP_TABLE, MAP_DEFAULT_TABLE, "{}", "{\"a\": 1}"),
                        new TableFixture(STRUCT_TABLE, STRUCT_DEFAULT_TABLE, "{}", "{7, \"x\"}"),
                        new TableFixture(MAP_INT_KEY_TABLE, null, "{1: 2}", "{3: 4}"),
                        new TableFixture(STRUCT_WITH_MAP_TABLE, null, "{7, {\"a\": 1}}", "{7, {\"a\": 2}}"),
                        new TableFixture(MAP_WITH_STRUCT_VALUE_TABLE, null, "{\"a\": {1}}", "{\"a\": {2}}"),
                        new TableFixture(ARRAY_STRUCT_TABLE, null, "[{1}]", "[{2}]"))) {
                    assertLoadPlanBinds(fixture, false, null, new StringLiteral(fixture.replacement));
                    assertLoadPlanBinds(fixture, false, new NullLiteral(), new StringLiteral(fixture.replacement));
                    assertLoadPlanBinds(fixture, false, new StringLiteral(fixture.search),
                            new StringLiteral(fixture.replacement));
                    // replace_value(NULL) falls back to the schema default of the column
                    if (fixture.defaultTable != null) {
                        assertLoadPlanBinds(fixture, true, null, null);
                    }
                }
            }
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = oldBehavior;
        }
    }

    private void assertLoadPlanBinds(TableFixture fixture, boolean oneArgReplace, Expression search,
            Expression replacement) throws Exception {
        UnboundFunction mapping;
        Pair<String, List<String>> function;
        if (oneArgReplace) {
            mapping = new UnboundFunction("replace_value", ImmutableList.of(new NullLiteral()));
            function = Pair.of("replace_value", Lists.newArrayList((String) null));
        } else if (search == null) {
            mapping = new UnboundFunction("default_value", ImmutableList.of(replacement));
            function = Pair.of("default_value", Lists.newArrayList(fixture.replacement));
        } else {
            mapping = new UnboundFunction("replace_value", ImmutableList.of(search, replacement));
            function = Pair.of("replace_value", Lists.newArrayList(
                    search instanceof NullLiteral ? null : fixture.search, fixture.replacement));
        }
        String table = oneArgReplace ? fixture.defaultTable : fixture.table;

        StatementScopeIdGenerator.clear();
        createStatementCtx("complex type mapping load " + table + " " + function.first);
        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(DATABASE);
        OlapTable targetTable = (OlapTable) database.getTableOrAnalysisException(table);
        Map<String, Pair<String, List<String>>> columnToHadoopFunction =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columnToHadoopFunction.put("v", function);
        NereidsBrokerFileGroup fileGroup = new NereidsBrokerFileGroup(1L, false, null,
                Lists.newArrayList("dummy"), null, Lists.newArrayList("k", "v"), null,
                Lists.newArrayList(new NereidsImportColumnDesc("k"), new NereidsImportColumnDesc("v"),
                        new NereidsImportColumnDesc("v", mapping)),
                columnToHadoopFunction, null, null, null, LoadTask.MergeType.APPEND, null, -1L, false, false,
                new ArrowFileFormatProperties());
        TBrokerFileStatus fileStatus = new TBrokerFileStatus();
        fileStatus.setPath("");
        fileStatus.setIsDir(false);
        fileStatus.setSize(-1);
        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(new TUniqueId(1, 2), 3L, targetTable,
                BrokerDesc.createForStreamLoad(), fileGroup, fileStatus, false, TFileType.FILE_STREAM, null,
                TUniqueKeyUpdateMode.UPSERT, null);
        NereidsParamCreateContext context = new NereidsLoadScanProvider(fileGroupInfo, Collections.emptySet())
                .createLoadContext();
        String evidence = table + " " + function.first + " newBehavior="
                + GlobalVariable.enableNewTypeCoercionBehavior;
        try {
            LogicalPlan plan = NereidsLoadUtils.createLoadPlan(fileGroupInfo, null, context, false,
                    TPartialUpdateNewRowPolicy.APPEND);
            Assertions.assertEquals(1, plan.<LogicalOlapTableSink>collectToList(
                    LogicalOlapTableSink.class::isInstance).size(), evidence);
        } catch (Exception e) {
            throw new AssertionError(evidence + ": " + e.getMessage(), e);
        }
    }

    private static class TableFixture {
        private final String table;
        private final String defaultTable;
        private final String search;
        private final String replacement;

        private TableFixture(String table, String defaultTable, String search, String replacement) {
            this.table = table;
            this.defaultTable = defaultTable;
            this.search = search;
            this.replacement = replacement;
        }
    }
}
