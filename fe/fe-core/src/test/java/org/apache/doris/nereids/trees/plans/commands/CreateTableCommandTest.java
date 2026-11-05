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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class CreateTableCommandTest {

    @Test
    void ctasBareVariantCopiesThePersistedLayoutIncludingLegacyNestedGroup() {
        for (boolean escaped : new boolean[] {false, true}) {
            VariantType sourceType = variantSourceType(escaped);
            Column source = new Column("v", sourceType.toCatalogDataType());
            SlotReference slot = variantSourceSlot(source, List.of());
            for (boolean escapeDerived : new boolean[] {false, true}) {
                DataType actual = CreateTableCommand.ctasColumnType(slot, escapeDerived);
                Assertions.assertEquals(ColumnToThrift.toThrift(source),
                        ColumnToThrift.toThrift(new Column("v", actual.toCatalogDataType())));
            }
        }
    }

    @Test
    void ctasExtractedVariantUsesDerivedLayoutDespiteItsOriginalColumn() {
        Column root = new Column("v", variantSourceType(false).toCatalogDataType());
        SlotReference extracted = variantSourceSlot(root, List.of("obj"));
        VariantType actual = (VariantType) CreateTableCommand.ctasColumnType(extracted, true);
        Assertions.assertTrue(actual.getEscapeDotsInKeys());
        Assertions.assertFalse(actual.getEnableNestedGroup());
        Assertions.assertEquals(new VariantType(0, true).toSql(), actual.toSql());
        Assertions.assertEquals(VariantType.INSTANCE.toSql(),
                CreateTableCommand.ctasColumnType(extracted, false).toSql());
        Assertions.assertSame(VariantType.INSTANCE, extracted.getDataType());
    }

    @Test
    void ctasNeutralExpressionAndSetOperationOutputsChooseOnlyTheNewStorageFormat() {
        SlotReference left = new SlotReference("left_v", VariantType.INSTANCE, true);
        SlotReference right = new SlotReference("right_v", VariantType.INSTANCE, true);
        Slot expression = new Alias(new Coalesce(left, right), "coalesced_v").toSlot();
        // UNION and recursive CTE outputs likewise carry a common type without a source Column.
        List<Slot> outputs = List.of(expression,
                new SlotReference("union_v", VariantType.INSTANCE, true),
                new SlotReference("recursive_v", VariantType.INSTANCE, true));
        for (Slot output : outputs) {
            VariantType actual = (VariantType) CreateTableCommand.ctasColumnType(output, true);
            Assertions.assertTrue(actual.getEscapeDotsInKeys());
            Assertions.assertFalse(actual.getEnableNestedGroup());
            Assertions.assertEquals(new VariantType(0, true).toSql(), actual.toSql());
            Assertions.assertEquals(VariantType.INSTANCE.toSql(),
                    CreateTableCommand.ctasColumnType(output, false).toSql());
            Assertions.assertEquals(VariantType.INSTANCE, output.getDataType());
        }
    }

    @Test
    void ctasSourceFormatIncludesMvColumnsAndReleasesTheTableLock() {
        Column legacyBase = new Column("base_v", new VariantType(0).toCatalogDataType());
        Column escapedMv = new Column("mv_v", new VariantType(0, true).toCatalogDataType());
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(Map.of(1L, List.of(legacyBase), 2L, List.of(escapedMv)))
                .when(table).getIndexIdToSchema(false);

        Assertions.assertTrue(CreateTableCommand.hasEscapedVariantSource(table));
        InOrder order = Mockito.inOrder(table);
        order.verify(table).readLock();
        order.verify(table).getIndexIdToSchema(false);
        order.verify(table).readUnlock();

        Mockito.doReturn(Map.of(1L, List.of(legacyBase), 2L, List.of(legacyBase)))
                .when(table).getIndexIdToSchema(false);
        Assertions.assertFalse(CreateTableCommand.hasEscapedVariantSource(table));
        Mockito.verify(table, Mockito.times(2)).readUnlock();
        Assertions.assertFalse(CreateTableCommand.hasEscapedVariantSource(Mockito.mock(TableIf.class)));
    }

    @Test
    void ctasSourceFormatUsesBaseAndMvMetadataWhenNoPhysicalPartitionsExist() {
        for (boolean escapedInBase : new boolean[] {true, false}) {
            Column base = new Column("v", new VariantType(0, escapedInBase).toCatalogDataType());
            OlapTable table = Mockito.spy(new OlapTable(1L, "source", List.of(base), KeysType.DUP_KEYS,
                    new PartitionInfo(PartitionType.RANGE), new RandomDistributionInfo(1)));
            table.setBaseIndexId(10L);
            table.setIndexMeta(10L, "source", List.of(base), 0, 0, (short) 0,
                    TStorageType.COLUMN, KeysType.DUP_KEYS);
            if (!escapedInBase) {
                Column mv = new Column("mv_v", new VariantType(0, true).toCatalogDataType());
                table.setIndexMeta(11L, "mv", List.of(mv), 0, 0, (short) 0,
                        TStorageType.COLUMN, KeysType.DUP_KEYS);
            }
            Assertions.assertEquals(0, table.getPartitionNum());
            Assertions.assertTrue(table.getSchemaAllIndexes(false).isEmpty());

            Assertions.assertTrue(CreateTableCommand.hasEscapedVariantSource(table));
            InOrder order = Mockito.inOrder(table);
            order.verify(table).readLock();
            order.verify(table).getIndexIdToSchema(false);
            order.verify(table).readUnlock();
        }
    }

    @Test
    void ctasSourceFormatReleasesItsLockWhenSchemaReadFails() {
        OlapTable table = Mockito.mock(OlapTable.class);
        IllegalStateException failure = new IllegalStateException("schema unavailable");
        Mockito.when(table.getIndexIdToSchema(false)).thenThrow(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> CreateTableCommand.hasEscapedVariantSource(table)));
        InOrder order = Mockito.inOrder(table);
        order.verify(table).readLock();
        order.verify(table).getIndexIdToSchema(false);
        order.verify(table).readUnlock();
    }

    private static VariantType variantSourceType(boolean escaped) {
        return new VariantType(List.of(new VariantField("obj.a", StringType.INSTANCE, "source template",
                PatternType.MATCH_NAME.name())), escaped ? 7 : 0, escaped, escaped ? 101 : 0,
                escaped ? 8 : 0, false, 0L, 64, !escaped, escaped);
    }

    private static SlotReference variantSourceSlot(Column source, List<String> path) {
        SlotReference seed = new SlotReference("v", VariantType.INSTANCE, true);
        return new SlotReference(seed.getExprId(), "v", VariantType.INSTANCE, true,
                List.of(), null, source, null, source, path);
    }

    @Test
    void ctasIfNotExistsReturnsBeforeUnsupportedSinkValidation() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        Mockito.when(createTableInfo.isIfNotExists()).thenReturn(true);
        Mockito.when(env.createTable(createTableInfo)).thenReturn(true);
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);

            org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                    () -> command.run(context, executor));

            sinkMock.verifyNoInteractions();
            Mockito.verify(command, Mockito.never()).validateCreateTableProperties(context);
            Mockito.verify(env).createTable(createTableInfo);
        }
    }

    @Test
    void ctasValidatesSinkBeforePublishingMetadata() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);
        Mockito.doNothing().when(command).validateCreateTableProperties(context);
        Mockito.doReturn(false).when(command).targetTableExists(context);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            sinkMock.when(() -> UnboundTableSinkCreator.createUnboundTableSink(
                    tableNameParts, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), query))
                    .thenThrow(new UserException("unsupported sink"));

            org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> command.run(context, executor));

            Mockito.verify(env, Mockito.never()).createTable(createTableInfo);
            Mockito.verify(env, Mockito.never()).dropTable(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
        }
    }

    @Test
    void existingCtasTargetPrecedesUnsupportedSinkValidation() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        Mockito.when(createTableInfo.getTableName()).thenReturn("target");
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);
        Mockito.doNothing().when(command).validateCreateTableProperties(context);
        Mockito.doReturn(true).when(command).targetTableExists(context);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> command.run(context, executor));

            org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains("already exists"));
            sinkMock.verifyNoInteractions();
            Mockito.verify(env, Mockito.never()).createTable(createTableInfo);
            Mockito.verify(env, Mockito.never()).dropTable(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
        }
    }

    @Test
    void ctasConfigurationPreflightPrecedesTargetLookup() throws Exception {
        LogicalPlan query = Mockito.mock(LogicalPlan.class);
        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Env env = Mockito.mock(Env.class);
        List<String> tableNameParts = ImmutableList.of("catalog", "database", "target");

        Mockito.when(createTableInfo.getTableNameParts()).thenReturn(tableNameParts);
        Mockito.when(createTableInfo.getTableName()).thenReturn("target");
        CreateTableCommand command = Mockito.spy(
                new CreateTableCommand(Optional.of(query), createTableInfo));
        Mockito.doNothing().when(command).validateCreateTableAsSelect(context, query);
        Mockito.doThrow(new UserException("unsupported catalog configuration"))
                .when(command).validateCreateTableProperties(context);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<UnboundTableSinkCreator> sinkMock =
                        Mockito.mockStatic(UnboundTableSinkCreator.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> command.run(context, executor));

            org.junit.jupiter.api.Assertions.assertTrue(
                    exception.getMessage().contains("unsupported catalog configuration"));
            Mockito.verify(command, Mockito.never()).targetTableExists(context);
            sinkMock.verifyNoInteractions();
            Mockito.verify(env, Mockito.never()).createTable(createTableInfo);
            Mockito.verify(env, Mockito.never()).dropTable(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyBoolean());
        }
    }
}
