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
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.load.NereidsLoadTaskInfo.NereidsImportColumnDescs;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TryParseToVariant;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class VariantLoadParseInjectionTest extends TestWithFeService {
    private static final String DATABASE = "variant_load_parse_db";
    private static final String TABLE = "variant_load_parse_t";

    private enum InputShape {
        DIRECT,
        MAPPING,
        EXPLICIT_CAST
    }

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        createTable("create table " + TABLE + " (\n"
                + "    k int,\n"
                + "    v variant<properties(\"variant_max_subcolumns_count\"=\"2048\",\n"
                + "        \"variant_enable_typed_paths_to_sparse\"=\"true\",\n"
                + "        \"variant_max_sparse_column_statistics_size\"=\"321\",\n"
                + "        \"variant_sparse_hash_shard_count\"=\"4\")>\n"
                + ")\n"
                + "duplicate key(k)\n"
                + "distributed by hash(k) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
    }

    @Test
    public void testStreamLoadParseInjection() throws Exception {
        for (InputShape shape : InputShape.values()) {
            assertParseInjectionInBothModes(() -> createStreamFixture(shape), shape);
        }
    }

    @Test
    public void testBrokerLoadParseInjection() throws Exception {
        for (InputShape shape : InputShape.values()) {
            assertParseInjectionInBothModes(() -> createBrokerFixture(shape), shape);
        }
    }

    @Test
    public void testRoutineLoadParseInjection() throws Exception {
        for (InputShape shape : InputShape.values()) {
            assertParseInjectionInBothModes(() -> createRoutineFixture(shape), shape);
        }
    }

    private void assertParseInjectionInBothModes(LoadFixtureSupplier fixtureSupplier, InputShape shape)
            throws Exception {
        assertParseInjection(fixtureSupplier.get(), shape, true);
    }

    private void assertParseInjection(LoadFixture fixture, InputShape shape, boolean enableVariantV2) {
        List<LogicalOlapTableSink> sinks = fixture.plan.<LogicalOlapTableSink>collectToList(
                LogicalOlapTableSink.class::isInstance);
        Assertions.assertEquals(1, sinks.size(), fixture.evidence());
        LogicalOlapTableSink<?> sink = sinks.get(0);
        Assertions.assertEquals(DMLCommandType.LOAD, sink.getDmlCommandType(), fixture.evidence());
        VariantType targetVariantType = targetVariantType(sink);

        List<TryParseToVariant> parses = new ArrayList<>();
        List<Cast> stringToVariantCasts = new ArrayList<>();
        for (Plan node : ((Plan) sink.child()).<Plan>collectToList(ignored -> true)) {
            for (Expression expression : node.getExpressions()) {
                parses.addAll(expression.<TryParseToVariant>collectToList(TryParseToVariant.class::isInstance));
                stringToVariantCasts.addAll(expression.<Cast>collectToList(candidate -> candidate instanceof Cast
                        && ((Cast) candidate).getDataType().isVariantType()
                        && ((Cast) candidate).child().getDataType().isStringLikeType()));
            }
        }

        if (shape == InputShape.EXPLICIT_CAST) {
            Assertions.assertTrue(parses.isEmpty(), fixture.evidence());
            Assertions.assertEquals(1, stringToVariantCasts.stream().filter(Cast::isExplicitType).count(),
                    fixture.evidence());
            return;
        }

        if (enableVariantV2) {
            Assertions.assertEquals(1, parses.size(), fixture.evidence());
            Assertions.assertEquals(targetVariantType, parses.get(0).getDataType(), fixture.evidence());
            Assertions.assertTrue(stringToVariantCasts.isEmpty(), fixture.evidence());
        } else {
            Assertions.assertTrue(parses.isEmpty(), fixture.evidence());
            Assertions.assertEquals(1, stringToVariantCasts.size(), fixture.evidence());
        }
    }

    private LoadFixture createStreamFixture(InputShape shape) throws Exception {
        NereidsStreamLoadTask task = new NereidsStreamLoadTask(new TUniqueId(1L, 2L), 3L,
                TFileType.FILE_STREAM, TFileFormatType.FORMAT_CSV_PLAIN, TFileCompressType.PLAIN);
        task.getColumnExprDescs().descs.addAll(columnDescs(shape).descs);
        return createStreamFamilyFixture("stream/" + shape, task, new TUniqueId(1L, 2L));
    }

    private LoadFixture createRoutineFixture(InputShape shape) throws Exception {
        NereidsRoutineLoadTaskInfo task = new NereidsRoutineLoadTaskInfo(1024L, new HashMap<>(), 10L,
                null, LoadTask.MergeType.APPEND, null, null, 0.0, columnDescs(shape), null, null,
                null, null, (byte) 0, (byte) 0, 1, false, TUniqueKeyUpdateMode.UPSERT,
                TPartialUpdateNewRowPolicy.APPEND, false);
        return createStreamFamilyFixture("routine/" + shape, task, new TUniqueId(4L, 5L));
    }

    private LoadFixture createStreamFamilyFixture(String entrance, NereidsLoadTaskInfo task, TUniqueId loadId)
            throws Exception {
        FixtureSource source = createFixtureSource(task, entrance);
        TBrokerFileStatus fileStatus = new TBrokerFileStatus();
        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(loadId, task.getTxnId(), source.targetTable,
                BrokerDesc.createForStreamLoad(), source.fileGroup, fileStatus, task.isStrictMode(),
                task.getFileType(), task.getHiddenColumns(), task.getUniqueKeyUpdateMode(),
                source.targetTable.getSequenceMapCol());
        return createLoadFixture(entrance, source, fileGroupInfo);
    }

    private LoadFixture createBrokerFixture(InputShape shape) throws Exception {
        String entrance = "broker/" + shape;
        prepareFixtureContext(entrance);
        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(DATABASE);
        OlapTable targetTable = (OlapTable) database.getTableOrAnalysisException(TABLE);
        NereidsImportColumnDescs descs = columnDescs(shape);
        NereidsDataDescription dataDescription = new NereidsDataDescription(TABLE, null,
                new ArrayList<>(ImmutableList.of("file:///tmp/variant-load.csv")), descs.getFileColNames(),
                null, "csv", false, descs.getColumnMappingList());
        NereidsBrokerFileGroup fileGroup = analyzeFileGroup(database, dataDescription);
        FixtureSource source = new FixtureSource(targetTable, fileGroup);
        NereidsFileGroupInfo fileGroupInfo = new NereidsFileGroupInfo(6L, 7L, targetTable,
                new BrokerDesc("test_broker", Collections.emptyMap()), fileGroup,
                ImmutableList.of(new TBrokerFileStatus()), 1, false, 1);
        return createLoadFixture(entrance, source, fileGroupInfo);
    }

    private FixtureSource createFixtureSource(NereidsLoadTaskInfo task, String entrance) throws Exception {
        prepareFixtureContext(entrance);
        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(DATABASE);
        OlapTable targetTable = (OlapTable) database.getTableOrAnalysisException(TABLE);
        NereidsDataDescription dataDescription = new NereidsDataDescription(TABLE, task);
        return new FixtureSource(targetTable, analyzeFileGroup(database, dataDescription));
    }

    private NereidsBrokerFileGroup analyzeFileGroup(Database database, NereidsDataDescription dataDescription)
            throws Exception {
        dataDescription.analyzeWithoutCheckPriv(database.getFullName());
        NereidsBrokerFileGroup fileGroup = new NereidsBrokerFileGroup(dataDescription);
        fileGroup.parse(database, dataDescription);
        return fileGroup;
    }

    private LoadFixture createLoadFixture(String entrance, FixtureSource source,
            NereidsFileGroupInfo fileGroupInfo) throws Exception {
        NereidsLoadScanProvider provider = new NereidsLoadScanProvider(fileGroupInfo, Collections.emptySet());
        NereidsParamCreateContext context = provider.createLoadContext();
        LogicalPlan plan = NereidsLoadUtils.createLoadPlan(fileGroupInfo, null, context, false,
                TPartialUpdateNewRowPolicy.APPEND);
        return new LoadFixture(entrance, plan);
    }

    private void prepareFixtureContext(String entrance) throws Exception {
        StatementScopeIdGenerator.clear();
        createStatementCtx("variant load parse " + entrance);
    }

    private NereidsImportColumnDescs columnDescs(InputShape shape) throws Exception {
        NereidsImportColumnDescs descs = new NereidsImportColumnDescs();
        descs.descs.add(new NereidsImportColumnDesc("k"));
        if (shape == InputShape.DIRECT) {
            descs.descs.add(new NereidsImportColumnDesc("v"));
        } else {
            descs.descs.add(new NereidsImportColumnDesc("raw_v"));
            Expression mapping = shape == InputShape.MAPPING
                    ? new UnboundSlot("raw_v")
                    : new Cast(new UnboundSlot("raw_v"), tableTargetVariantType(), true);
            descs.descs.add(new NereidsImportColumnDesc("v", mapping));
        }
        return descs;
    }

    private VariantType targetVariantType(LogicalOlapTableSink<?> sink) {
        List<VariantType> targetTypes = new ArrayList<>();
        sink.getTargetTableSlots().stream()
                .filter(slot -> slot.getDataType() instanceof VariantType)
                .forEach(slot -> targetTypes.add((VariantType) slot.getDataType()));
        Assertions.assertEquals(1, targetTypes.size(), sink.treeString());
        return targetTypes.get(0);
    }

    private VariantType tableTargetVariantType() throws Exception {
        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(DATABASE);
        OlapTable targetTable = (OlapTable) database.getTableOrAnalysisException(TABLE);
        return (VariantType) DataType.fromCatalogType(targetTable.getColumn("v").getType());
    }

    private static class FixtureSource {
        private final OlapTable targetTable;
        private final NereidsBrokerFileGroup fileGroup;

        private FixtureSource(OlapTable targetTable, NereidsBrokerFileGroup fileGroup) {
            this.targetTable = targetTable;
            this.fileGroup = fileGroup;
        }
    }

    private static class LoadFixture {
        private final String entrance;
        private final LogicalPlan plan;

        private LoadFixture(String entrance, LogicalPlan plan) {
            this.entrance = entrance;
            this.plan = plan;
        }

        private String evidence() {
            return entrance + "\n" + plan.treeString();
        }
    }

    @FunctionalInterface
    private interface LoadFixtureSupplier {
        LoadFixture get() throws Exception;
    }
}
