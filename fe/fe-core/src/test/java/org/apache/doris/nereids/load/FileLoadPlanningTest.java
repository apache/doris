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
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.load.NereidsLoadTaskInfo.NereidsImportColumnDescs;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTypeNodeType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class FileLoadPlanningTest extends TestWithFeService {
    private static final String DATABASE = "file_load_planning";
    private static final String TABLE = "file_load";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        createTable("CREATE TABLE " + TABLE + " (k INT, f FILE, a ARRAY<FILE>, "
                + "s STRUCT<payload:FILE,n:INT>, m MAP<STRING,ARRAY<FILE>>) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testStreamLoadTypedFilePlanAndDefaults() throws Exception {
        for (TFileFormatType format : ImmutableList.of(TFileFormatType.FORMAT_JSON, TFileFormatType.FORMAT_ORC,
                TFileFormatType.FORMAT_ARROW)) {
            assertStreamFamilyPlan(streamTask(format));
        }
    }

    @Test
    public void testTextFileLoadStrictModeKeepsTypedNullableSources() throws Exception {
        for (boolean nullable : new boolean[] {false, true}) {
            String table = "file_text_load_" + nullable;
            createTable("CREATE TABLE " + table + " (k INT, f FILE " + (nullable ? "NULL" : "NOT NULL")
                    + ") DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES('replication_num'='1')");
            for (TFileFormatType format : ImmutableList.of(TFileFormatType.FORMAT_JSON)) {
                for (boolean strict : new boolean[] {false, true}) {
                    TStreamLoadPutRequest request = new TStreamLoadPutRequest();
                    request.setLoadId(new TUniqueId(1L, 2L));
                    request.setTxnId(3L);
                    request.setFileType(TFileType.FILE_STREAM);
                    request.setFormatType(format);
                    request.setCompressType(TFileCompressType.PLAIN);
                    request.setStrictMode(strict);
                    assertStreamFamilyPlan(NereidsStreamLoadTask.fromTStreamLoadPutRequest(request), table);
                }
            }
        }
    }

    @Test
    public void testRoutineLoadTypedFilePlanAndDefaults() throws Exception {
        NereidsRoutineLoadTaskInfo task = new NereidsRoutineLoadTaskInfo(1024L, new HashMap<>(), 10L,
                null, LoadTask.MergeType.APPEND, null, null, 0.0, new NereidsImportColumnDescs(), null, null,
                null, null, (byte) 0, (byte) 0, 1, false, TUniqueKeyUpdateMode.UPSERT,
                TPartialUpdateNewRowPolicy.APPEND, false);
        Assertions.assertThrows(UserException.class, () -> assertStreamFamilyPlan(task));
    }

    @Test
    public void testBrokerLoadTypedFilePlan() throws Exception {
        for (String format : ImmutableList.of("json", "orc")) {
            prepareContext();
            Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(DATABASE);
            OlapTable table = (OlapTable) database.getTableOrAnalysisException(TABLE);
            NereidsDataDescription description = new NereidsDataDescription(TABLE, null,
                    new ArrayList<>(ImmutableList.of("file:///tmp/file-load." + format)), null,
                    null, format, false, ImmutableList.of());
            NereidsBrokerFileGroup group = analyzeFileGroup(database, description);
            NereidsFileGroupInfo info = new NereidsFileGroupInfo(6L, 7L, table,
                    new BrokerDesc("test_broker", Collections.emptyMap()), group,
                    ImmutableList.of(new TBrokerFileStatus()), 1, false, 1);
            createPlan(info);
        }
    }

    @Test
    public void testCsvRejectsDirectFileInput() {
        Assertions.assertThrows(UserException.class,
                () -> assertStreamFamilyPlan(streamTask(TFileFormatType.FORMAT_CSV_PLAIN)));
    }

    @Test
    public void testMappedStringIsNotAFileFormatInput() {
        NereidsStreamLoadTask task = streamTask(TFileFormatType.FORMAT_JSON);
        task.getColumnExprDescs().descs.addAll(ImmutableList.of(new NereidsImportColumnDesc("k"),
                new NereidsImportColumnDesc("raw_f"),
                new NereidsImportColumnDesc("f", new UnboundSlot("raw_f"))));
        UserException error = Assertions.assertThrows(UserException.class, () -> assertStreamFamilyPlan(task));
        Assertions.assertTrue(error.getMessage().contains("Cannot implicitly convert"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("FILE"), error.getMessage());
    }

    @Test
    public void testMappedStructRequiresExplicitSqlConversion() {
        NereidsStreamLoadTask task = streamTask(TFileFormatType.FORMAT_CSV_PLAIN);
        task.getColumnExprDescs().descs.addAll(ImmutableList.of(new NereidsImportColumnDesc("k"),
                new NereidsImportColumnDesc("raw_f"), new NereidsImportColumnDesc("f",
                        new UnboundFunction("named_struct", ImmutableList.of(new StringLiteral("uri"),
                                new UnboundSlot("raw_f"))))));
        Assertions.assertThrows(UserException.class, () -> assertStreamFamilyPlan(task));
    }

    private NereidsStreamLoadTask streamTask(TFileFormatType format) {
        return new NereidsStreamLoadTask(new TUniqueId(1L, 2L), 3L,
                TFileType.FILE_STREAM, format, TFileCompressType.PLAIN);
    }

    private void assertStreamFamilyPlan(NereidsLoadTaskInfo task) throws Exception {
        assertStreamFamilyPlan(task, TABLE);
    }

    private void assertStreamFamilyPlan(NereidsLoadTaskInfo task, String tableName) throws Exception {
        prepareContext();
        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(DATABASE);
        OlapTable table = (OlapTable) database.getTableOrAnalysisException(tableName);
        NereidsDataDescription description = new NereidsDataDescription(tableName, task);
        NereidsBrokerFileGroup group = analyzeFileGroup(database, description);
        TUniqueId loadId = new TUniqueId(1L, 2L);
        NereidsFileGroupInfo info = new NereidsFileGroupInfo(loadId, task.getTxnId(), table,
                BrokerDesc.createForStreamLoad(), group, new TBrokerFileStatus(), task.isStrictMode(),
                task.getFileType(), task.getHiddenColumns(), task.getUniqueKeyUpdateMode(), table.getSequenceMapCol());
        NereidsParamCreateContext context = new NereidsLoadScanProvider(info, Collections.emptySet())
                .createLoadContext();
        LogicalPlan plan = createPlan(info, context);
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor destination = descriptors.createTupleDescriptor();
        destination.setTable(table);
        NereidsLoadPlanInfoCollector collector = new NereidsLoadPlanInfoCollector(table, task, loadId,
                database.getId(), task.getUniqueKeyUpdateMode(), TPartialUpdateNewRowPolicy.APPEND,
                new HashSet<>(), context.exprMap);
        TFileScanRangeParams params = collector.collectLoadPlanInfo(plan, descriptors, destination)
                .toFileScanRangeParams(loadId, info);
        Assertions.assertEquals(task.isStrictMode(), params.isStrictMode());
        TupleDescriptor source = descriptors.getTupleDesc(new TupleId(params.getSrcTupleId()));
        Assertions.assertEquals(context.scanSlots.size(), source.getSlots().size());
        for (SlotDescriptor slot : source.getSlots()) {
            if (!slot.getType().typeContainsFile()) {
                continue;
            }
            Column targetColumn = table.getColumn(slot.getColumn().getName());
            Type target = targetColumn.getType();
            Assertions.assertTrue(slot.isNullable());
            Assertions.assertEquals(target.toThrift(), slot.getType().toThrift());
            Assertions.assertTrue(slot.getType().toThrift().getTypes().stream()
                    .anyMatch(node -> node.getType() == TTypeNodeType.FILE && node.getStructFieldsSize() == 6));
            TExpr defaultValue = params.getDefaultValueOfSrcSlot().get(slot.getId().asInt());
            Assertions.assertNotNull(defaultValue);
            if (targetColumn.isAllowNull()) {
                Assertions.assertEquals(1, defaultValue.getNodesSize());
                Assertions.assertEquals(TExprNodeType.NULL_LITERAL, defaultValue.getNodes().get(0).getNodeType());
                Assertions.assertEquals(target.toThrift(), defaultValue.getNodes().get(0).getType());
            } else {
                Assertions.assertNull(targetColumn.getDefaultValue());
                Assertions.assertEquals(0, defaultValue.getNodesSize());
            }
        }
    }

    private LogicalPlan createPlan(NereidsFileGroupInfo info) throws Exception {
        return createPlan(info, new NereidsLoadScanProvider(info, Collections.emptySet()).createLoadContext());
    }

    private LogicalPlan createPlan(NereidsFileGroupInfo info, NereidsParamCreateContext context) throws Exception {
        LogicalPlan plan = NereidsLoadUtils.createLoadPlan(info, null, context, false,
                TPartialUpdateNewRowPolicy.APPEND);
        LogicalOlapTableSink<?> sink = plan.<LogicalOlapTableSink<?>>collectToList(
                LogicalOlapTableSink.class::isInstance).get(0);
        Assertions.assertTrue(sink.getTargetTableSlots().stream().anyMatch(
                slot -> slot.getDataType().typeContainsFile()), plan.treeString());
        for (Plan node : plan.<Plan>collectToList(ignored -> true)) {
            for (Expression expression : node.getExpressions()) {
                Assertions.assertTrue(expression.collectToList(candidate -> candidate instanceof Cast
                        && ((Cast) candidate).child().getDataType().isStringLikeType()
                        && ((Cast) candidate).getDataType().typeContainsFile()).isEmpty(),
                        plan.treeString());
            }
        }
        context.scanSlots.forEach(slot -> {
            if (info.getTargetTable().getColumn(slot.getName()) != null
                    && !context.exprMap.containsKey(slot.getName())) {
                Type target = info.getTargetTable().getColumn(slot.getName()).getType();
                if (target.typeContainsFile()) {
                    Assertions.assertEquals(DataType.fromCatalogType(target), slot.getDataType());
                }
            }
        });
        return plan;
    }

    private NereidsBrokerFileGroup analyzeFileGroup(Database database, NereidsDataDescription description)
            throws Exception {
        description.analyzeWithoutCheckPriv(database.getFullName());
        NereidsBrokerFileGroup group = new NereidsBrokerFileGroup(description);
        group.parse(database, description);
        return group;
    }

    private void prepareContext() throws Exception {
        StatementScopeIdGenerator.clear();
        createStatementCtx("FILE load planning");
    }
}
