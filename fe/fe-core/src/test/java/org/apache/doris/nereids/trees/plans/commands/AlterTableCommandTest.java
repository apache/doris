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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnsOp;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableCommandTest {
    private static Column normalVariantColumn(String name) {
        return new Column(name, Type.VARIANT);
    }

    private static Column docModeVariantColumn(String name) {
        org.apache.doris.catalog.VariantType docModeVariant = new org.apache.doris.catalog.VariantType(
                new ArrayList<>(), 0, false, 10000, 0, true, 0L, 64, false);
        return new Column(name, docModeVariant);
    }

    private static ColumnDefinition intColumnDefinition(String name) {
        return new ColumnDefinition(name, IntegerType.INSTANCE, false);
    }

    private static void invokeRewriteForOlapTable(AlterTableCommand command, OlapTable table) throws Exception {
        invokePrivate(command, "rewriteAlterOpForOlapTable",
                new Class<?>[] {org.apache.doris.qe.ConnectContext.class, OlapTable.class},
                new Object[] {null, table});
    }

    private static void invokeValidateVariantAlter(AlterTableCommand command, OlapTable table) throws Exception {
        invokePrivate(command, "validateAlterVariantColumnsForFlexiblePartialUpdate",
                new Class<?>[] {OlapTable.class}, new Object[] {table});
    }

    private static void invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object[] args)
            throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        try {
            method.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    @Test
    void testEnableFeatureOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new EnableFeatureOp("BATCH_DELETE"));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` ENABLE FEATURE \"BATCH_DELETE\"", alterTableCommand.toSql());

        ops.clear();
        ops.add(new EnableFeatureOp("UPDATE_FLEXIBLE_COLUMNS"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` ENABLE FEATURE \"UPDATE_FLEXIBLE_COLUMNS\"",
                alterTableCommand.toSql());

        ops.clear();
        Map<String, String> properties = new HashMap<>();
        properties.put("function_column.sequence_type", "int");
        ops.add(new EnableFeatureOp("SEQUENCE_LOAD", properties));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(
                "ALTER TABLE `internal`.`db`.`test` ENABLE FEATURE \"SEQUENCE_LOAD\" WITH PROPERTIES (\"function_column.sequence_type\" = \"int\")",
                alterTableCommand.toSql());
    }

    @Test
    void testRewriteEnableFlexiblePartialUpdateOnVariantTable() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.isUniqKeyMergeOnWriteWithClusterKeys()).thenReturn(false);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(false);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(true);
        Mockito.when(table.getVisibleIndex()).thenReturn(Lists.newArrayList(
                new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL)));
        Mockito.when(table.getBaseSchema(true)).thenReturn(Lists.newArrayList(
                new Column("k", PrimitiveType.INT), normalVariantColumn("v")));

        List<AlterTableOp> ops = new ArrayList<>();
        EnableFeatureOp enableFlexibleUpdate = new EnableFeatureOp("UPDATE_FLEXIBLE_COLUMNS");
        enableFlexibleUpdate.validate(null);
        ops.add(enableFlexibleUpdate);
        AlterTableCommand alterTableCommand = new AlterTableCommand(null, ops);

        invokeRewriteForOlapTable(alterTableCommand, table);

        Assertions.assertEquals(1, alterTableCommand.getOps().size());
        Assertions.assertTrue(alterTableCommand.getOps().get(0) instanceof AddColumnOp);
        AddColumnOp addColumnOp = (AddColumnOp) alterTableCommand.getOps().get(0);
        Assertions.assertTrue(addColumnOp.getColumn().isSkipBitmapColumn());
        Assertions.assertEquals(Column.SKIP_BITMAP_COL, addColumnOp.getColumn().getName());
        Assertions.assertEquals("AFTER `v`", addColumnOp.getColPos().toSql());
    }

    @Test
    void testEnableFlexiblePartialUpdateRequiresLightSchemaChange() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.isUniqKeyMergeOnWriteWithClusterKeys()).thenReturn(false);
        Mockito.when(table.hasSkipBitmapColumn()).thenReturn(false);
        Mockito.when(table.getEnableLightSchemaChange()).thenReturn(false);

        EnableFeatureOp enableFlexibleUpdate = new EnableFeatureOp("UPDATE_FLEXIBLE_COLUMNS");
        enableFlexibleUpdate.validate(null);
        AlterTableCommand alterTableCommand = new AlterTableCommand(null,
                Lists.newArrayList(enableFlexibleUpdate));

        UserException exception = Assertions.assertThrows(UserException.class,
                () -> invokeRewriteForOlapTable(alterTableCommand, table));
        Assertions.assertTrue(exception.getMessage().contains("light_schema_change"));
    }

    @Test
    void testEnableFlexiblePartialUpdateRejectsClusterKeyTable() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.when(table.isUniqKeyMergeOnWriteWithClusterKeys()).thenReturn(true);

        EnableFeatureOp enableFlexibleUpdate = new EnableFeatureOp("UPDATE_FLEXIBLE_COLUMNS");
        enableFlexibleUpdate.validate(null);
        AlterTableCommand alterTableCommand = new AlterTableCommand(null,
                Lists.newArrayList(enableFlexibleUpdate));

        UserException exception = Assertions.assertThrows(UserException.class,
                () -> invokeRewriteForOlapTable(alterTableCommand, table));
        Assertions.assertTrue(exception.getMessage().contains("cluster keys"));
    }

    @Test
    void testValidateVariantAlterOnlyWhenEnablingFlexiblePartialUpdate() throws Exception {
        OlapTable tableWithoutFlexibleUpdate = Mockito.mock(OlapTable.class);
        Mockito.when(tableWithoutFlexibleUpdate.hasSkipBitmapColumn()).thenReturn(false);
        OlapTable tableWithFlexibleUpdate = Mockito.mock(OlapTable.class);
        Mockito.when(tableWithFlexibleUpdate.hasSkipBitmapColumn()).thenReturn(true);

        AddColumnOp addNormalVariant = new AddColumnOp(intColumnDefinition("normal_v"), null, null, null);
        addNormalVariant.setColumn(normalVariantColumn("normal_v"));
        AlterTableCommand normalVariantCommand = new AlterTableCommand(null, Lists.newArrayList(addNormalVariant));
        Assertions.assertDoesNotThrow(() -> invokeValidateVariantAlter(normalVariantCommand, tableWithFlexibleUpdate));

        AddColumnOp addDocModeVariant = new AddColumnOp(intColumnDefinition("doc_v"), null, null, null);
        addDocModeVariant.setColumn(docModeVariantColumn("doc_v"));
        AlterTableCommand addDocOnlyCommand = new AlterTableCommand(null, Lists.newArrayList(addDocModeVariant));
        Assertions.assertDoesNotThrow(() -> invokeValidateVariantAlter(addDocOnlyCommand, tableWithoutFlexibleUpdate));
        Assertions.assertThrows(UserException.class,
                () -> invokeValidateVariantAlter(addDocOnlyCommand, tableWithFlexibleUpdate));

        EnableFeatureOp enableFlexibleUpdate = new EnableFeatureOp("UPDATE_FLEXIBLE_COLUMNS");
        enableFlexibleUpdate.validate(null);
        AlterTableCommand addDocWithEnableCommand = new AlterTableCommand(null,
                Lists.newArrayList(addDocModeVariant, enableFlexibleUpdate));
        Assertions.assertThrows(UserException.class,
                () -> invokeValidateVariantAlter(addDocWithEnableCommand, tableWithoutFlexibleUpdate));

        AddColumnsOp addColumnsOp = new AddColumnsOp(null, null, Lists.newArrayList(
                new Column("plain", PrimitiveType.INT), docModeVariantColumn("doc_v2")));
        AlterTableCommand addColumnsCommand = new AlterTableCommand(null, Lists.newArrayList(
                addColumnsOp, enableFlexibleUpdate));
        Assertions.assertThrows(UserException.class,
                () -> invokeValidateVariantAlter(addColumnsCommand, tableWithoutFlexibleUpdate));

        ModifyColumnOp modifyColumnOp = new ModifyColumnOp(intColumnDefinition("doc_v3"),
                new ColumnPosition("plain"), null, null);
        modifyColumnOp.setColumn(docModeVariantColumn("doc_v3"));
        AlterTableCommand modifyCommand = new AlterTableCommand(null, Lists.newArrayList(
                modifyColumnOp, enableFlexibleUpdate));
        Assertions.assertThrows(UserException.class,
                () -> invokeValidateVariantAlter(modifyCommand, tableWithoutFlexibleUpdate));
    }

    @Test
    void testAddPartitionFieldOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new AddPartitionFieldOp("bucket", 16, "id", null));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` ADD PARTITION KEY bucket(16, id)", alterTableCommand.toSql());

        ops.clear();
        ops.add(new AddPartitionFieldOp("year", null, "ts", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` ADD PARTITION KEY year(ts)",
                alterTableCommand.toSql());

        ops.clear();
        ops.add(new AddPartitionFieldOp(null, null, "category", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` ADD PARTITION KEY category", alterTableCommand.toSql());

        // Test with custom partition field name
        ops.clear();
        ops.add(new AddPartitionFieldOp("day", null, "ts", "ts_day"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` ADD PARTITION KEY day(ts) AS ts_day",
                alterTableCommand.toSql());
    }

    @Test
    void testDropPartitionFieldOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new DropPartitionFieldOp("id_bucket_16"));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` DROP PARTITION KEY id_bucket_16",
                alterTableCommand.toSql());

        ops.clear();
        ops.add(new DropPartitionFieldOp("ts_year"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` DROP PARTITION KEY ts_year",
                alterTableCommand.toSql());

        ops.clear();
        ops.add(new DropPartitionFieldOp("category"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` DROP PARTITION KEY category", alterTableCommand.toSql());
    }

    @Test
    void testMultiplePartitionFieldOps() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new AddPartitionFieldOp("day", null, "ts", null));
        ops.add(new AddPartitionFieldOp("bucket", 8, "id", null));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        String sql = alterTableCommand.toSql();
        Assertions.assertTrue(sql.contains("ADD PARTITION KEY day(ts)"));
        Assertions.assertTrue(sql.contains("ADD PARTITION KEY bucket(8, id)"));
    }

    @Test
    void testReplacePartitionFieldOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new ReplacePartitionFieldOp("ts_year", null, null, null,
                "month", null, "ts", null));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` REPLACE PARTITION KEY ts_year WITH month(ts)",
                alterTableCommand.toSql());

        ops.clear();
        ops.add(new ReplacePartitionFieldOp("id_bucket_10", null, null, null,
                "bucket", 16, "id", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(
                "ALTER TABLE `internal`.`db`.`test` REPLACE PARTITION KEY id_bucket_10 WITH bucket(16, id)",
                alterTableCommand.toSql());

        ops.clear();
        ops.add(new ReplacePartitionFieldOp("category", null, null, null,
                "bucket", 8, "id", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals("ALTER TABLE `internal`.`db`.`test` REPLACE PARTITION KEY category WITH bucket(8, id)",
                alterTableCommand.toSql());

        // Test with custom partition field name
        ops.clear();
        ops.add(new ReplacePartitionFieldOp("ts_year", null, null, null,
                "day", null, "ts", "day_of_ts"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(
                "ALTER TABLE `internal`.`db`.`test` REPLACE PARTITION KEY ts_year WITH day(ts) AS day_of_ts",
                alterTableCommand.toSql());

        // Test with old partition expression
        ops.clear();
        ops.add(new ReplacePartitionFieldOp(null, "bucket", 16, "id",
                "truncate", 5, "code", "code_trunc"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(
                "ALTER TABLE `internal`.`db`.`test` REPLACE PARTITION KEY bucket(16, id) WITH truncate(5, code) AS code_trunc",
                alterTableCommand.toSql());
    }
}
