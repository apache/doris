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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableCommandTest {
    private final NereidsParser parser = new NereidsParser();

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

    @Test
    void testRejectNestedColumnPathForNonIcebergTable() {
        TableIf table = Mockito.mock(TableIf.class);
        for (String sql : Arrays.asList(
                "ALTER TABLE t ADD COLUMN s.c STRING NULL",
                "ALTER TABLE t MODIFY COLUMN s.a BIGINT",
                "ALTER TABLE t MODIFY COLUMN s.a COMMENT 'nested comment'",
                "ALTER TABLE t DROP COLUMN s.c",
                "ALTER TABLE t RENAME COLUMN s.c TO c2")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> AlterTableCommand.checkNestedColumnPathSupported(table, parseAlter(sql).getOps()));
            Assertions.assertTrue(exception.getMessage()
                    .contains("Nested column path is only supported for Iceberg tables"));
        }
    }

    @Test
    void testAllowNestedColumnPathForIcebergTable() throws AnalysisException {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        AlterTableCommand.checkNestedColumnPathSupported(table,
                parseAlter("ALTER TABLE t ADD COLUMN s.c STRING NULL").getOps());
    }

    @Test
    void testRejectDefaultMetadataForNestedIcebergModify() throws AnalysisException {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        for (String sql : Arrays.asList(
                "ALTER TABLE t MODIFY COLUMN s.a BIGINT DEFAULT 7",
                "ALTER TABLE t MODIFY COLUMN s.a BIGINT DEFAULT NULL",
                "ALTER TABLE t MODIFY COLUMN s.ts DATETIME DEFAULT CURRENT_TIMESTAMP "
                        + "ON UPDATE CURRENT_TIMESTAMP")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> AlterTableCommand.checkNestedColumnPathSupported(table, parseAlter(sql).getOps()));
            Assertions.assertTrue(exception.getMessage()
                    .contains("DEFAULT and ON UPDATE are not supported for nested Iceberg MODIFY COLUMN"));
        }

        AlterTableCommand.checkNestedColumnPathSupported(table,
                parseAlter("ALTER TABLE t MODIFY COLUMN s.a BIGINT").getOps());
        AlterTableCommand.checkNestedColumnPathSupported(table,
                parseAlter("ALTER TABLE t ADD COLUMN s.b BIGINT NULL DEFAULT 7").getOps());
    }

    @Test
    void testRejectRollupForNestedIcebergColumnOperations() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        for (String sql : Arrays.asList(
                "ALTER TABLE t ADD COLUMN s.c STRING NULL TO r1",
                "ALTER TABLE t ADD COLUMN s.c STRING NULL IN r1",
                "ALTER TABLE t DROP COLUMN s.c FROM r1",
                "ALTER TABLE t MODIFY COLUMN s.c STRING FROM r1")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> AlterTableCommand.checkNestedColumnPathSupported(table, parseAlter(sql).getOps()));
            Assertions.assertTrue(exception.getMessage()
                    .contains("Rollup is not supported for nested Iceberg column operation"));
        }
    }

    @Test
    void testRejectKeyForNestedIcebergAddAndModify() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        for (String sql : Arrays.asList(
                "ALTER TABLE t ADD COLUMN s.c INT KEY NULL",
                "ALTER TABLE t MODIFY COLUMN s.c BIGINT KEY")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> AlterTableCommand.checkNestedColumnPathSupported(table, parseAlter(sql).getOps()));
            Assertions.assertTrue(exception.getMessage()
                    .contains("KEY is not supported for nested Iceberg ADD/MODIFY COLUMN"));
        }
    }

    @Test
    void testRejectGeneratedColumnForNestedIcebergAddAndModify() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        for (String sql : Arrays.asList(
                "ALTER TABLE t ADD COLUMN s.c INT AS (id + 1) NULL",
                "ALTER TABLE t MODIFY COLUMN s.c BIGINT AS (id + 1)")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> AlterTableCommand.checkNestedColumnPathSupported(table, parseAlter(sql).getOps()));
            Assertions.assertTrue(exception.getMessage()
                    .contains("Generated columns are not supported for nested Iceberg ADD/MODIFY COLUMN"));
        }
    }

    @Test
    void testModifyColumnTracksExplicitNullability() {
        ModifyColumnOp omitted = (ModifyColumnOp) parseAlter(
                "ALTER TABLE t MODIFY COLUMN s.a BIGINT").getOps().get(0);
        ModifyColumnOp nullable = (ModifyColumnOp) parseAlter(
                "ALTER TABLE t MODIFY COLUMN s.a BIGINT NULL").getOps().get(0);
        ModifyColumnOp notNullable = (ModifyColumnOp) parseAlter(
                "ALTER TABLE t MODIFY COLUMN s.a BIGINT NOT NULL").getOps().get(0);

        Assertions.assertFalse(omitted.getColumnDef()
                .translateToCatalogStyleForSchemaChange().isNullableSpecified());
        Assertions.assertTrue(nullable.getColumnDef()
                .translateToCatalogStyleForSchemaChange().isNullableSpecified());
        Assertions.assertTrue(notNullable.getColumnDef()
                .translateToCatalogStyleForSchemaChange().isNullableSpecified());
    }

    private AlterTableCommand parseAlter(String sql) {
        Plan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(AlterTableCommand.class, plan);
        return (AlterTableCommand) plan;
    }
}
