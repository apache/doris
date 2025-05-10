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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MaterializedIndexMetaTest extends SqlTestBase {

    private static final String fileName = "./MaterializedIndexMetaSerializeTest";
    private static final Path path = Paths.get(fileName);

    @Override
    protected void runBeforeAll() throws Exception {
        Files.deleteIfExists(path);
        super.runBeforeAll();
        createTables("CREATE TABLE IF NOT EXISTS basic_test_table (\n"
                + "    `k1` TINYINT COMMENT \"abc\",\n"
                + "    `k2` SMALLINT COMMENT \"debug\",\n"
                + "    `k3` INT,\n"
                + "    `k4` BIGINT COMMENT \"**\",\n"
                + "    `k5` LARGEINT,\n"
                + "    `k6` DOUBLE,\n"
                + "    `k7` FLOAT,\n"
                + "    `k8` DATE,\n"
                + "    `k9` DATETIME,\n"
                + "    `k10` VARCHAR(255),\n"
                + "    `k11` DECIMAL(27,9),\n"
                + "    `k12` INT,\n"
                + "    `v1` INT COMMENT \"sum_aggregate\",\n"
                + ")\n"
                + "ENGINE=OLAP\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\"\n"
                + ");");
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        Files.deleteIfExists(path);
    }

    @Test
    public void testSerializeMaterializedIndexMeta()
            throws IOException, AnalysisException {
        // 1. Write objects to file
        Files.createFile(path);
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        List<Column> schema = Lists.newArrayList();
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k1", Type.TINYINT, true, null, true, "1", "abc"));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k2", Type.SMALLINT, true, null, true, "1", "debug"));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k3", Type.INT, true, null, true, "1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k4", Type.BIGINT, true, null, true, "1", "**"));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k5", Type.LARGEINT, true, null, true, null, ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k6", Type.DOUBLE, true, null, true, "1.1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k7", Type.FLOAT, true, null, true, "1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k8", Type.DATE, true, null, true, "1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k9", Type.DATETIME, true, null, true, "1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k10", Type.VARCHAR, true, null, true, "1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k11", Type.DECIMALV2, true, null, true, "1", ""));
        schema.add(new Column(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "k12", Type.INT, true, null, true, "1", ""));
        schema.add(new Column("mva_SUM__CAST(`v1` AS bigint)", Type.INT, false, AggregateType.SUM, true, "1", ""));
        schema.add(new Column("mva_BITMAP_UNION__to_bitmap_with_check(CAST(`k1` AS bigint))", Type.BITMAP, false, AggregateType.BITMAP_UNION, false, "1", ""));
        short shortKeyColumnCount = 1;
        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(1, schema, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.DUP_KEYS, new OriginStatement(
                "create materialized view test_mv as select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, sum(v1), "
                        + "bitmap_union(to_bitmap(k1)) from basic_test_table group by k1, k2, k3, k4, k5, "
                        + "k6, k7, k8, k9, k10, k11, k12",
                0), null, "test");
        indexMeta.write(out);
        out.flush();
        out.close();


        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));
        MaterializedIndexMeta readIndexMeta = MaterializedIndexMeta.read(in);
        readIndexMeta.parseStmt(null);
        Assertions.assertEquals(1, readIndexMeta.getIndexId());
        List<Column> resultColumns = readIndexMeta.getSchema();
        for (int i = 0; i < resultColumns.size(); i++) {
            Column column = resultColumns.get(i);
            if (column.getName().equals("mva_BITMAP_UNION__to_bitmap_with_check(CAST(`k1` AS bigint))")) {
                Assertions.assertTrue(column.getDefineExpr() instanceof FunctionCallExpr);
                Assertions.assertEquals(Type.BITMAP, column.getType());
                Assertions.assertEquals(AggregateType.BITMAP_UNION, column.getAggregationType());
                Assertions.assertEquals("to_bitmap_with_check",
                        ((FunctionCallExpr) column.getDefineExpr()).getFnName().getFunction());
            } else if (column.getName().equals("mva_SUM__CAST(`v1` AS bigint)")) {
                Assertions.assertEquals("v1", ((SlotRef) column.getDefineExpr().getChild(0)).getColumn().getName());
            } else {
                Assertions.assertEquals(column.getName(),
                        CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX
                                + ((SlotRef) column.getDefineExpr()).getColumn().getName());
            }
        }

        // 3.close
        in.close();
    }
}
