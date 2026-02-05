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

import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.EnableFeatureOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableCommandTest {
    @Test
    void testEnableFeatureOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new EnableFeatureOp("BATCH_DELETE"));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` ENABLE FEATURE \"BATCH_DELETE\"");

        ops.clear();
        ops.add(new EnableFeatureOp("UPDATE_FLEXIBLE_COLUMNS"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` ENABLE FEATURE \"UPDATE_FLEXIBLE_COLUMNS\"");

        ops.clear();
        Map<String, String> properties = new HashMap<>();
        properties.put("function_column.sequence_type", "int");
        ops.add(new EnableFeatureOp("SEQUENCE_LOAD", properties));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` ENABLE FEATURE \"SEQUENCE_LOAD\" WITH PROPERTIES (\"function_column.sequence_type\" = \"int\")");
    }

    @Test
    void testAddPartitionFieldOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new AddPartitionFieldOp("bucket", 16, "id", null));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` ADD PARTITION KEY bucket(16, id)");

        ops.clear();
        ops.add(new AddPartitionFieldOp("year", null, "ts", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` ADD PARTITION KEY year(ts)");

        ops.clear();
        ops.add(new AddPartitionFieldOp(null, null, "category", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` ADD PARTITION KEY category");

        // Test with custom partition field name
        ops.clear();
        ops.add(new AddPartitionFieldOp("day", null, "ts", "ts_day"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` ADD PARTITION KEY day(ts) AS ts_day");
    }

    @Test
    void testDropPartitionFieldOp() {
        List<AlterTableOp> ops = new ArrayList<>();
        ops.add(new DropPartitionFieldOp("id_bucket_16"));
        AlterTableCommand alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` DROP PARTITION KEY id_bucket_16");

        ops.clear();
        ops.add(new DropPartitionFieldOp("ts_year"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` DROP PARTITION KEY ts_year");

        ops.clear();
        ops.add(new DropPartitionFieldOp("category"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(), "ALTER TABLE `db`.`test` DROP PARTITION KEY category");
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
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` REPLACE PARTITION KEY ts_year WITH month(ts)");

        ops.clear();
        ops.add(new ReplacePartitionFieldOp("id_bucket_10", null, null, null,
                "bucket", 16, "id", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` REPLACE PARTITION KEY id_bucket_10 WITH bucket(16, id)");

        ops.clear();
        ops.add(new ReplacePartitionFieldOp("category", null, null, null,
                "bucket", 8, "id", null));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` REPLACE PARTITION KEY category WITH bucket(8, id)");

        // Test with custom partition field name
        ops.clear();
        ops.add(new ReplacePartitionFieldOp("ts_year", null, null, null,
                "day", null, "ts", "day_of_ts"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` REPLACE PARTITION KEY ts_year WITH day(ts) AS day_of_ts");

        // Test with old partition expression
        ops.clear();
        ops.add(new ReplacePartitionFieldOp(null, "bucket", 16, "id",
                "truncate", 5, "code", "code_trunc"));
        alterTableCommand = new AlterTableCommand(new TableNameInfo("db", "test"), ops);
        Assertions.assertEquals(alterTableCommand.toSql(),
                "ALTER TABLE `db`.`test` REPLACE PARTITION KEY bucket(16, id) WITH truncate(5, code) AS code_trunc");
    }
}
