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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecMerge;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.commands.use.SwitchCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class IcebergDDLAndDMLPlanTest extends TestWithFeService {
    private String catalogName;
    private String dbName;
    private String tableName;
    private String warehouse;
    private Table mockedIcebergTable;
    private PartitionSpec basePartitionSpec;
    private Schema baseIcebergSchema;
    private boolean previousEnableNereidsDistributePlanner;
    private MockedStatic<IcebergUtils> icebergUtilsMock;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        previousEnableNereidsDistributePlanner =
                connectContext.getSessionVariable().isEnableNereidsDistributePlanner();
        connectContext.getSessionVariable().setEnableNereidsDistributePlanner(true);
        connectContext.getSessionVariable().setEnablePipelineXEngine("true");

        String suffix = java.util.UUID.randomUUID().toString().replace("-", "");
        catalogName = "iceberg_test_" + suffix;
        dbName = "iceberg_db_" + suffix;
        tableName = "iceberg_tbl_" + suffix;
        Path warehousePath = Files.createTempDirectory("iceberg_warehouse_");
        warehouse = "file://" + warehousePath.toAbsolutePath() + "/";

        String createCatalogSql = "create catalog " + catalogName
                + " properties('type'='iceberg',"
                + " 'iceberg.catalog.type'='hadoop',"
                + " 'warehouse'='" + warehouse + "')";
        createCatalog(createCatalogSql);

        IcebergExternalCatalog catalog = (IcebergExternalCatalog) Env.getCurrentEnv()
                .getCatalogMgr().getCatalog(catalogName);
        catalog.setInitializedForTest(true);

        IcebergExternalDatabase database = new IcebergExternalDatabase(
                catalog, Env.getCurrentEnv().getNextId(), dbName, dbName);
        catalog.addDatabaseForTest(database);

        Catalog icebergCatalog = catalog.getCatalog();
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces nsCatalog = (SupportsNamespaces) icebergCatalog;
            Namespace namespace = Namespace.of(dbName);
            if (!nsCatalog.namespaceExists(namespace)) {
                nsCatalog.createNamespace(namespace);
            }
        }
        Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "score", Types.IntegerType.get()),
                Types.NestedField.required(5, "amount", Types.DecimalType.of(10, 2)));
        this.baseIcebergSchema = icebergSchema;
        icebergCatalog.createTable(
                TableIdentifier.of(dbName, tableName),
                icebergSchema,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", "2"));

        List<Column> schema = ImmutableList.of(
                new Column("id", PrimitiveType.INT),
                new Column("name", PrimitiveType.STRING),
                new Column("age", PrimitiveType.INT),
                new Column("score", PrimitiveType.SMALLINT),
                new Column("amount", PrimitiveType.DECIMAL64, 0, 10, 2, false));

        IcebergExternalTable table = new IcebergExternalTable(
                Env.getCurrentEnv().getNextId(), tableName, tableName, catalog, database);
        IcebergExternalTable spyTable = Mockito.spy(table);
        Mockito.doNothing().when(spyTable).makeSureInitialized();
        Mockito.doAnswer(invocation -> {
            List<Column> fullSchema = new ArrayList<>(schema);
            if (ConnectContext.get() != null
                    && ConnectContext.get().needIcebergRowId()) {
                fullSchema.add(IcebergRowId.createHiddenColumn());
            }
            return fullSchema;
        }).when(spyTable).getFullSchema();
        Mockito.doReturn(ImmutableList.of()).when(spyTable)
                .getPartitionColumns(ArgumentMatchers.any());
        IcebergSnapshotCacheValue snapshotCacheValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(0L, 0L));
        Mockito.doReturn(new IcebergMvccSnapshot(snapshotCacheValue)).when(spyTable)
                .loadSnapshot(ArgumentMatchers.any(), ArgumentMatchers.any());
        Table mockedIcebergTable = Mockito.mock(Table.class);
        PartitionSpec mockedSpec = Mockito.mock(PartitionSpec.class);
        Mockito.doReturn(false).when(mockedSpec).isPartitioned();
        Mockito.doReturn(ImmutableMap.of("format-version", "2")).when(mockedIcebergTable).properties();
        Mockito.doReturn(mockedSpec).when(mockedIcebergTable).spec();
        Mockito.doReturn(ImmutableMap.<Integer, PartitionSpec>of()).when(mockedIcebergTable).specs();
        Mockito.doReturn(icebergSchema).when(mockedIcebergTable).schema();

        // Mock newScan() chain used by IcebergScanNode.createTableScan()
        TableScan mockedTableScan = Mockito.mock(TableScan.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.doReturn(mockedTableScan).when(mockedIcebergTable).newScan();
        Mockito.doReturn(mockedTableScan).when(mockedTableScan).metricsReporter(ArgumentMatchers.any());
        Mockito.doReturn(mockedTableScan).when(mockedTableScan).useSnapshot(ArgumentMatchers.anyLong());
        Mockito.doReturn(mockedTableScan).when(mockedTableScan).useRef(ArgumentMatchers.any());
        Mockito.doReturn(mockedTableScan).when(mockedTableScan).filter(ArgumentMatchers.<org.apache.iceberg.expressions.Expression>any());
        Mockito.doReturn(mockedTableScan).when(mockedTableScan).planWith(ArgumentMatchers.any());
        Mockito.doReturn(null).when(mockedTableScan).snapshot();
        Mockito.doReturn(CloseableIterable.withNoopClose(java.util.Collections.emptyList()))
                .when(mockedTableScan).planFiles();

        Mockito.doReturn(mockedIcebergTable).when(spyTable).getIcebergTable();
        this.mockedIcebergTable = mockedIcebergTable;
        this.basePartitionSpec = mockedSpec;
        database.addTableForTest(spyTable);

        icebergUtilsMock = Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS);
        icebergUtilsMock.when(() -> IcebergUtils.getIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                .thenAnswer(invocation -> {
                    ExternalTable externalTable = invocation.getArgument(0);
                    if (externalTable instanceof IcebergExternalTable
                            && tableName.equalsIgnoreCase(externalTable.getName())
                            && dbName.equalsIgnoreCase(externalTable.getDbName())) {
                        return mockedIcebergTable;
                    }
                    return invocation.callRealMethod();
                });
    }

    @Override
    protected void runAfterAll() throws Exception {
        if (icebergUtilsMock != null) {
            icebergUtilsMock.close();
            icebergUtilsMock = null;
        }
        connectContext.getSessionVariable()
                .setEnableNereidsDistributePlanner(previousEnableNereidsDistributePlanner);
        if (catalogName != null) {
            Env.getCurrentEnv().getCatalogMgr().dropCatalog(catalogName, true);
        }
    }

    @Test
    public void testIcebergDeletePlanAddsRowIdProject() throws Exception {
        useIceberg();
        String sql = "delete from " + tableName + " where id > 1";
        LogicalPlan deletePlan = parseStmt(sql);
        Assertions.assertTrue(deletePlan instanceof DeleteFromCommand);

        Plan explainPlan = ((DeleteFromCommand) deletePlan).getExplainPlan(connectContext);
        Assertions.assertTrue(explainPlan instanceof LogicalIcebergDeleteSink);

        Plan child = explainPlan.child(0);
        Assertions.assertTrue(child instanceof LogicalProject);
        List<NamedExpression> projects = ((LogicalProject<?>) child).getProjects();
        Assertions.assertEquals(2, projects.size());
        boolean hasOperation = false;
        boolean hasRowId = false;
        for (NamedExpression project : projects) {
            if (project instanceof UnboundAlias) {
                Optional<String> alias = ((UnboundAlias) project).getAlias();
                if (alias.isPresent()
                        && IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(alias.get())) {
                    hasOperation = true;
                }
                continue;
            }
            if (project instanceof Slot) {
                String name = ((Slot) project).getName();
                if (IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(name)) {
                    hasOperation = true;
                }
                if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(name)) {
                    hasRowId = true;
                }
            }
        }
        Assertions.assertTrue(hasOperation);
        Assertions.assertTrue(hasRowId);

        PhysicalPlan physicalPlan = planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);
        assertContainsPhysicalSink(physicalPlan, PhysicalIcebergDeleteSink.class);
    }

    @Test
    public void testIcebergUpdatePlans() throws Exception {
        useIceberg();
        String sql = "update " + tableName + " set name = 'new_name' where id = 1";
        LogicalPlan updatePlan = parseStmt(sql);
        Assertions.assertTrue(updatePlan instanceof UpdateCommand);

        Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
        Assertions.assertTrue(explainPlan instanceof LogicalIcebergMergeSink);

        Plan child = explainPlan.child(0);
        Assertions.assertTrue(child instanceof LogicalProject);
        List<NamedExpression> projects = ((LogicalProject<?>) child).getProjects();
        boolean hasOperation = false;
        boolean hasRowId = false;
        for (NamedExpression project : projects) {
            if (project instanceof UnboundAlias) {
                Optional<String> alias = ((UnboundAlias) project).getAlias();
                if (alias.isPresent()
                        && IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(alias.get())) {
                    hasOperation = true;
                }
                continue;
            }
            if (project instanceof Slot) {
                String name = ((Slot) project).getName();
                if (IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(name)) {
                    hasOperation = true;
                }
                if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(name)) {
                    hasRowId = true;
                }
            }
        }
        Assertions.assertTrue(hasOperation);
        Assertions.assertTrue(hasRowId);

        PhysicalPlan physicalPlan = planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);
        assertContainsPhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
    }

    @Test
    public void testIcebergMergeIntoExplainUsesMergePartitioning() throws Exception {
        useIceberg();
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "merge into " + tableName + " t "
                    + "using (select 1 as id, 'name1' as name, 10 as age, 1 as score, 1.23 as amount) s "
                    + "on t.id = s.id "
                    + "when matched then update set name = s.name "
                    + "when not matched then insert (id, name, age, score, amount) "
                    + "values (s.id, s.name, s.age, s.score, s.amount)";
            LogicalPlan mergePlan = parseStmt(sql);
            Assertions.assertTrue(mergePlan instanceof MergeIntoCommand);

            Plan explainPlan = ((MergeIntoCommand) mergePlan).getExplainPlan(connectContext);
            Assertions.assertTrue(explainPlan instanceof LogicalIcebergMergeSink);

            String explain = getExplainString((LogicalPlan) explainPlan,
                    ExplainCommand.ExplainLevel.DISTRIBUTED_PLAN, sql);
            String upper = explain.toUpperCase();
            Assertions.assertTrue(upper.contains("ICEBERG MERGE SINK"), explain);
            Assertions.assertTrue(upper.contains("MERGE_PARTITIONED"), explain);
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
        }
    }

    @Test
    public void testIcebergMergeIntoExchangeUsesMergePartitioningWhenEnabled() throws Exception {
        useIceberg();
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "merge into " + tableName + " t "
                    + "using (select 1 as id, 'name1' as name, 10 as age, 1 as score, 1.23 as amount) s "
                    + "on t.id = s.id "
                    + "when matched then update set name = s.name "
                    + "when not matched then insert (id, name, age, score, amount) "
                    + "values (s.id, s.name, s.age, s.score, s.amount)";
            LogicalPlan mergePlan = parseStmt(sql);
            Assertions.assertTrue(mergePlan instanceof MergeIntoCommand);

            Plan explainPlan = ((MergeIntoCommand) mergePlan).getExplainPlan(connectContext);
            PhysicalPlan physicalPlan =
                    planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

            PhysicalIcebergMergeSink<?> sink =
                    getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
            ExprId operationExprId = findOperationExprId(sink.child().getOutput());
            ExprId rowIdExprId = findRowIdExprId(sink.child().getOutput());
            Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                    "Missing merge partition exchange\n" + physicalPlan.treeString());
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
            Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecMerge,
                    "Missing merge distribution spec\n" + physicalPlan.treeString());

            DistributionSpecMerge spec = (DistributionSpecMerge) distribute.getDistributionSpec();
            Assertions.assertEquals(operationExprId, spec.getOperationExprId());
            Assertions.assertTrue(spec.isInsertRandom());
            Assertions.assertTrue(spec.getInsertPartitionExprIds().isEmpty());
            Assertions.assertEquals(1, spec.getDeletePartitionExprIds().size());
            Assertions.assertEquals(rowIdExprId, spec.getDeletePartitionExprIds().get(0));
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
        }
    }

    @Test
    public void testIcebergUpdateCastsConstantForSmallintAndDecimal() throws Exception {
        useIceberg();
        String sql = "update " + tableName + " set score = 1, amount = 1.23 where id = 1";
        LogicalPlan updatePlan = parseStmt(sql);
        Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
        PhysicalPlan physicalPlan = planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

        PhysicalIcebergMergeSink<?> sink =
                getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
        assertOutputCastedToColumnType(sink, "score");
        assertOutputCastedToColumnType(sink, "amount");
    }

    @Test
    public void testIcebergUpdateExchangeUsesRowIdOnlyWhenDisabled() throws Exception {
        useIceberg();
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = false;
        try {
            String sql = "update " + tableName + " set name = 'new_name' where id = 1";
            LogicalPlan updatePlan = parseStmt(sql);
            Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
            PhysicalPlan physicalPlan =
                    planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

            PhysicalIcebergMergeSink<?> sink =
                    getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
            ExprId rowIdExprId = findRowIdExprId(sink.child().getOutput());
            Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                    "Missing row_id exchange\n" + physicalPlan.treeString());
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
            Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecHash,
                    "Missing row_id hash distribution\n" + physicalPlan.treeString());
            DistributionSpecHash hash = (DistributionSpecHash) distribute.getDistributionSpec();
            Assertions.assertEquals(ImmutableList.of(rowIdExprId), hash.getOrderedShuffledColumns());
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
        }
    }

    @Test
    public void testIcebergUpdateExchangeUsesMergePartitioningWhenEnabled() throws Exception {
        useIceberg();
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "update " + tableName + " set name = 'new_name' where id = 1";
            LogicalPlan updatePlan = parseStmt(sql);
            Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
            PhysicalPlan physicalPlan =
                    planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

            PhysicalIcebergMergeSink<?> sink =
                    getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
            ExprId operationExprId = findOperationExprId(sink.child().getOutput());
            ExprId rowIdExprId = findRowIdExprId(sink.child().getOutput());
            Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                    "Missing merge partition exchange\n" + physicalPlan.treeString());
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
            Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecMerge,
                    "Missing merge distribution spec\n" + physicalPlan.treeString());

            DistributionSpecMerge spec = (DistributionSpecMerge) distribute.getDistributionSpec();
            Assertions.assertEquals(operationExprId, spec.getOperationExprId());
            Assertions.assertTrue(spec.isInsertRandom());
            Assertions.assertTrue(spec.getInsertPartitionExprIds().isEmpty());
            Assertions.assertEquals(1, spec.getDeletePartitionExprIds().size());
            Assertions.assertEquals(rowIdExprId, spec.getDeletePartitionExprIds().get(0));
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
        }
    }

    @Test
    public void testIcebergUpdateExchangeUsesPartitionColumnsWhenEnabled() throws Exception {
        useIceberg();
        IcebergExternalTable table = getIcebergTable();
        Column partitionColumn = new Column("age", PrimitiveType.INT);
        Mockito.doReturn(ImmutableList.of(partitionColumn)).when(table)
                .getPartitionColumns(ArgumentMatchers.any());
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "update " + tableName + " set name = 'new_name' where id = 1";
            LogicalPlan updatePlan = parseStmt(sql);
            Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
            PhysicalPlan physicalPlan =
                    planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

            PhysicalIcebergMergeSink<?> sink =
                    getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
            ExprId operationExprId = findOperationExprId(sink.child().getOutput());
            ExprId rowIdExprId = findRowIdExprId(sink.child().getOutput());
            ExprId partitionExprId = findExprIdByName(sink.child().getOutput(), "age");
            Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                    "Missing merge partition exchange\n" + physicalPlan.treeString());
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
            Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecMerge,
                    "Missing merge distribution spec\n" + physicalPlan.treeString());

            DistributionSpecMerge spec = (DistributionSpecMerge) distribute.getDistributionSpec();
            Assertions.assertEquals(operationExprId, spec.getOperationExprId());
            Assertions.assertFalse(spec.isInsertRandom());
            Assertions.assertEquals(1, spec.getInsertPartitionExprIds().size());
            Assertions.assertEquals(partitionExprId, spec.getInsertPartitionExprIds().get(0));
            Assertions.assertEquals(1, spec.getDeletePartitionExprIds().size());
            Assertions.assertEquals(rowIdExprId, spec.getDeletePartitionExprIds().get(0));
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
            Mockito.doReturn(ImmutableList.of()).when(table).getPartitionColumns(ArgumentMatchers.any());
        }
    }

    @Test
    public void testIcebergUpdatePartitionExpressionUsesPartitionColumnWhenEnabled() throws Exception {
        useIceberg();
        IcebergExternalTable table = getIcebergTable();
        Column partitionColumn = new Column("age", PrimitiveType.INT);
        Mockito.doReturn(ImmutableList.of(partitionColumn)).when(table)
                .getPartitionColumns(ArgumentMatchers.any());
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "update " + tableName + " set age = age + 1 where id = 1";
            LogicalPlan updatePlan = parseStmt(sql);
            Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
            PhysicalPlan physicalPlan =
                    planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

            PhysicalIcebergMergeSink<?> sink =
                    getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
            ExprId operationExprId = findOperationExprId(sink.child().getOutput());
            ExprId rowIdExprId = findRowIdExprId(sink.child().getOutput());
            Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                    "Missing merge partition exchange\n" + physicalPlan.treeString());
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
            Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecMerge,
                    "Missing merge distribution spec\n" + physicalPlan.treeString());

            DistributionSpecMerge spec = (DistributionSpecMerge) distribute.getDistributionSpec();
            Assertions.assertEquals(operationExprId, spec.getOperationExprId());
            Assertions.assertFalse(spec.isInsertRandom());
            ExprId expectedExprId = findPartitionExprIdByColumnOrder(
                    sink.getCols(), sink.child().getOutput(), "age");
            Assertions.assertEquals(ImmutableList.of(expectedExprId), spec.getInsertPartitionExprIds());
            Assertions.assertEquals(ImmutableList.of(rowIdExprId), spec.getDeletePartitionExprIds());
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
            Mockito.doReturn(ImmutableList.of()).when(table).getPartitionColumns(ArgumentMatchers.any());
        }
    }

    @Test
    public void testIcebergUpdateExchangeUsesPartitionSpecTransform() throws Exception {
        useIceberg();
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).bucket("id", 16).build();
        Mockito.doReturn(schema).when(mockedIcebergTable).schema();
        Mockito.doReturn(partitionSpec).when(mockedIcebergTable).spec();

        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "update " + tableName + " set name = 'new_name' where id = 1";
            LogicalPlan updatePlan = parseStmt(sql);
            Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
            PhysicalPlan physicalPlan =
                    planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

            PhysicalIcebergMergeSink<?> sink =
                    getSinglePhysicalSink(physicalPlan, PhysicalIcebergMergeSink.class);
            Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                    "Missing merge partition exchange\n" + physicalPlan.treeString());
            PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
            Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecMerge,
                    "Missing merge distribution spec\n" + physicalPlan.treeString());
            DistributionSpecMerge mergeSpec = (DistributionSpecMerge) distribute.getDistributionSpec();

            ExprId idExprId = findExprIdByName(sink.child().getOutput(), "id");
            Assertions.assertFalse(mergeSpec.isInsertRandom());
            Assertions.assertTrue(mergeSpec.getInsertPartitionExprIds().isEmpty());
            Assertions.assertEquals(1, mergeSpec.getInsertPartitionFields().size());
            DistributionSpecMerge.IcebergPartitionField field = mergeSpec.getInsertPartitionFields().get(0);
            Assertions.assertEquals(idExprId, field.getSourceExprId());
            Assertions.assertEquals("bucket[16]", field.getTransform());
            Assertions.assertEquals(Integer.valueOf(partitionSpec.specId()), mergeSpec.getPartitionSpecId());
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
            Mockito.doReturn(basePartitionSpec).when(mockedIcebergTable).spec();
            Mockito.doReturn(baseIcebergSchema).when(mockedIcebergTable).schema();
        }
    }

    @Test
    public void testIcebergDeleteExchangeUsesMergePartitioning() throws Exception {
        useIceberg();
        String sql = "delete from " + tableName + " where id > 1";
        LogicalPlan deletePlan = parseStmt(sql);
        Plan explainPlan = ((DeleteFromCommand) deletePlan).getExplainPlan(connectContext);
        PhysicalPlan physicalPlan = planPhysicalPlan((LogicalPlan) explainPlan, PhysicalProperties.GATHER, sql);

        PhysicalIcebergDeleteSink<?> sink = getSinglePhysicalSink(physicalPlan, PhysicalIcebergDeleteSink.class);
        ExprId rowIdExprId = findRowIdExprId(sink.child().getOutput());
        ExprId operationExprId = findOperationExprId(sink.child().getOutput());
        Assertions.assertTrue(sink.child() instanceof PhysicalDistribute,
                "Missing merge-partition exchange\n" + physicalPlan.treeString());
        PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) sink.child();
        Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecMerge,
                "Missing merge distribution spec\n" + physicalPlan.treeString());
        DistributionSpecMerge spec = (DistributionSpecMerge) distribute.getDistributionSpec();
        Assertions.assertEquals(operationExprId, spec.getOperationExprId());
        Assertions.assertEquals(ImmutableList.of(rowIdExprId), spec.getDeletePartitionExprIds());
        Assertions.assertTrue(spec.getInsertPartitionExprIds().isEmpty());
        Assertions.assertTrue(spec.getInsertPartitionFields().isEmpty());
        Assertions.assertTrue(spec.isInsertRandom());
        Assertions.assertNull(spec.getPartitionSpecId());
    }


    @Test
    public void testIcebergUpdateExplainHasExchange() throws Exception {
        useIceberg();
        String sql = "update " + tableName + " set name = 'new_name' where id = 1";
        LogicalPlan updatePlan = parseStmt(sql);
        Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
        String explain = getExplainString((LogicalPlan) explainPlan,
                ExplainCommand.ExplainLevel.DISTRIBUTED_PLAN, sql);
        String upper = explain.toUpperCase();
        Assertions.assertTrue(upper.contains("EXCHANGE"), explain);
        Assertions.assertTrue(upper.contains("ICEBERG MERGE SINK"), explain);
        Assertions.assertTrue(upper.contains(Column.ICEBERG_ROWID_COL.toUpperCase()), explain);
    }

    @Test
    public void testIcebergUpdateExplainHasMergePartitioningWhenEnabled() throws Exception {
        useIceberg();
        boolean previous = connectContext.getSessionVariable().enableIcebergMergePartitioning;
        connectContext.getSessionVariable().enableIcebergMergePartitioning = true;
        try {
            String sql = "update " + tableName + " set name = 'new_name' where id = 1";
            LogicalPlan updatePlan = parseStmt(sql);
            Plan explainPlan = ((UpdateCommand) updatePlan).getExplainPlan(connectContext);
            String explain = getExplainString((LogicalPlan) explainPlan,
                    ExplainCommand.ExplainLevel.DISTRIBUTED_PLAN, sql);
            Assertions.assertTrue(explain.toUpperCase().contains("MERGE_PARTITIONED"), explain);
        } finally {
            connectContext.getSessionVariable().enableIcebergMergePartitioning = previous;
        }
    }

    @Test
    public void testIcebergDeleteExplainHasExchange() throws Exception {
        useIceberg();
        String sql = "delete from " + tableName + " where id > 1";
        LogicalPlan deletePlan = parseStmt(sql);
        Plan explainPlan = ((DeleteFromCommand) deletePlan).getExplainPlan(connectContext);
        String explain = getExplainString((LogicalPlan) explainPlan,
                ExplainCommand.ExplainLevel.DISTRIBUTED_PLAN, sql);
        String upper = explain.toUpperCase();
        Assertions.assertTrue(upper.contains("EXCHANGE"), explain);
        Assertions.assertTrue(upper.contains("MERGE_PARTITIONED"), explain);
        Assertions.assertTrue(upper.contains("ICEBERG DELETE SINK"), explain);
        Assertions.assertTrue(upper.contains(Column.ICEBERG_ROWID_COL.toUpperCase()), explain);
    }

    private void switchCatalog(String catalogName) throws Exception {
        SwitchCommand switchCommand = (SwitchCommand) parseStmt("switch " + catalogName + ";");
        Env.getCurrentEnv().changeCatalog(connectContext, switchCommand.getCatalogName());
    }

    private void useIceberg() throws Exception {
        switchCatalog(catalogName);
        useDatabase(dbName);
    }

    private IcebergExternalTable getIcebergTable() {
        List<String> nameParts = ImmutableList.of(catalogName, dbName, tableName);
        return (IcebergExternalTable) RelationUtil.getTable(nameParts, Env.getCurrentEnv(), Optional.empty());
    }

    private PhysicalPlan planPhysicalPlan(LogicalPlan plan, PhysicalProperties physicalProperties, String sql) {
        connectContext.setThreadLocalInfo();
        ensureQueryId();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, statementContext);
        adapter.setViewDdlSqls(statementContext.getViewDdlSqls());
        statementContext.setParsedStatement(adapter);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        long previousTargetTableId = connectContext.getIcebergRowIdTargetTableId();
        DeleteCommandContext deleteContext = null;
        long targetTableId = -1;
        if (plan instanceof LogicalIcebergDeleteSink) {
            deleteContext = ((LogicalIcebergDeleteSink<?>) plan).getDeleteContext();
            targetTableId = ((LogicalIcebergDeleteSink<?>) plan).getTargetTable().getId();
        } else if (plan instanceof LogicalIcebergMergeSink) {
            deleteContext = ((LogicalIcebergMergeSink<?>) plan).getDeleteContext();
            targetTableId = ((LogicalIcebergMergeSink<?>) plan).getTargetTable().getId();
        }
        if (deleteContext != null
                && deleteContext.getDeleteFileType() == DeleteCommandContext.DeleteFileType.POSITION_DELETE
                && previousTargetTableId < 0) {
            connectContext.setIcebergRowIdTargetTableId(targetTableId);
        }
        try {
            planner.plan(adapter, connectContext.getSessionVariable().toThrift());
            PhysicalPlan physicalPlan = planner.getPhysicalPlan();
            ExplainOptions explainOptions = new ExplainOptions(ExplainCommand.ExplainLevel.OPTIMIZED_PLAN, false);
            System.out.println("Physical plan for: " + sql + "\n" + planner.getExplainString(explainOptions));
            return physicalPlan;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to plan statement: " + sql, exception);
        } finally {
            connectContext.setIcebergRowIdTargetTableId(previousTargetTableId);
        }
    }

    private String getExplainString(LogicalPlan plan, ExplainCommand.ExplainLevel level, String sql) {
        connectContext.setThreadLocalInfo();
        ensureQueryId();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, statementContext);
        adapter.setViewDdlSqls(statementContext.getViewDdlSqls());
        statementContext.setParsedStatement(adapter);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        long previousTargetTableId = connectContext.getIcebergRowIdTargetTableId();
        DeleteCommandContext deleteContext = null;
        long targetTableId = -1;
        if (plan instanceof LogicalIcebergDeleteSink) {
            deleteContext = ((LogicalIcebergDeleteSink<?>) plan).getDeleteContext();
            targetTableId = ((LogicalIcebergDeleteSink<?>) plan).getTargetTable().getId();
        } else if (plan instanceof LogicalIcebergMergeSink) {
            deleteContext = ((LogicalIcebergMergeSink<?>) plan).getDeleteContext();
            targetTableId = ((LogicalIcebergMergeSink<?>) plan).getTargetTable().getId();
        }
        if (deleteContext != null
                && deleteContext.getDeleteFileType() == DeleteCommandContext.DeleteFileType.POSITION_DELETE
                && previousTargetTableId < 0) {
            connectContext.setIcebergRowIdTargetTableId(targetTableId);
        }
        try {
            planner.plan(adapter, connectContext.getSessionVariable().toThrift());
            ExplainOptions explainOptions = new ExplainOptions(level, false);
            return planner.getExplainString(explainOptions);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to plan statement: " + sql, exception);
        } finally {
            connectContext.setIcebergRowIdTargetTableId(previousTargetTableId);
        }
    }

    private static void assertContainsPhysicalSink(PhysicalPlan plan, Class<?> sinkClass) {
        Set<?> sinks = plan.collect(sinkClass::isInstance);
        Assertions.assertFalse(sinks.isEmpty());
    }

    private void ensureQueryId() {
        if (connectContext.queryId() == null) {
            UUID uuid = UUID.randomUUID();
            connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        }
    }

    private static ExprId findRowIdExprId(List<Slot> slots) {
        for (Slot slot : slots) {
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(slot.getName())) {
                return slot.getExprId();
            }
        }
        Assertions.fail("Missing row_id slot in output");
        return null;
    }

    private static ExprId findOperationExprId(List<Slot> slots) {
        for (Slot slot : slots) {
            if (IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(slot.getName())) {
                return slot.getExprId();
            }
        }
        Assertions.fail("Missing operation slot in output");
        return null;
    }

    private static ExprId findExprIdByName(List<Slot> slots, String name) {
        for (Slot slot : slots) {
            if (name.equalsIgnoreCase(slot.getName())) {
                return slot.getExprId();
            }
        }
        Assertions.fail("Missing slot in output: " + name);
        return null;
    }

    private static ExprId findPartitionExprIdByColumnOrder(List<Column> columns, List<Slot> slots,
                                                           String columnName) {
        List<Column> visibleColumns = new ArrayList<>();
        for (Column column : columns) {
            if (column.isVisible()) {
                visibleColumns.add(column);
            }
        }
        List<Slot> dataSlots = getDataSlots(slots);
        Assertions.assertEquals(visibleColumns.size(), dataSlots.size());
        int index = -1;
        for (int i = 0; i < visibleColumns.size(); i++) {
            if (columnName.equalsIgnoreCase(visibleColumns.get(i).getName())) {
                index = i;
                break;
            }
        }
        Assertions.assertTrue(index >= 0, "Missing column in visible columns: " + columnName);
        return dataSlots.get(index).getExprId();
    }

    private static List<Slot> getDataSlots(List<Slot> slots) {
        List<Slot> dataSlots = new ArrayList<>();
        for (Slot slot : slots) {
            String name = slot.getName();
            if (IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(name)) {
                continue;
            }
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(name)) {
                continue;
            }
            dataSlots.add(slot);
        }
        return dataSlots;
    }

    private static <T> T getSinglePhysicalSink(PhysicalPlan plan, Class<T> sinkClass) {
        Set<?> sinks = plan.collect(sinkClass::isInstance);
        Assertions.assertEquals(1, sinks.size());
        return sinkClass.cast(sinks.iterator().next());
    }

    private static void assertOutputCastedToColumnType(PhysicalIcebergMergeSink<?> sink, String columnName) {
        Column column = findColumnByName(sink.getCols(), columnName);
        NamedExpression expr = findOutputExprByName(sink.getOutputExprs(), columnName);
        Expression child = expr;
        if (expr instanceof Alias) {
            child = ((Alias) expr).child();
        }
        DataType expected = DataType.fromCatalogType(column.getType());
        Assertions.assertEquals(expected, child.getDataType(),
                "Output expression type mismatch for column: " + columnName);
    }

    private static Column findColumnByName(List<Column> columns, String name) {
        for (Column column : columns) {
            if (name.equalsIgnoreCase(column.getName())) {
                return column;
            }
        }
        Assertions.fail("Missing column: " + name);
        return null;
    }

    private static NamedExpression findOutputExprByName(List<NamedExpression> exprs, String name) {
        for (NamedExpression expr : exprs) {
            if (name.equalsIgnoreCase(expr.getName())) {
                return expr;
            }
        }
        Assertions.fail("Missing output expression: " + name);
        return null;
    }
}
