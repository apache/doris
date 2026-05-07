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

import org.apache.doris.binlog.BinlogTestUtils;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class OlapTableRowBinlogSchemaTest {

    private static OlapTable newTestTable(BinlogConfig binlogConfig) {
        long baseIndexId = 1L;
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column value = new Column("v1", PrimitiveType.INT);
        value.setIsKey(false);
        List<Column> baseSchema = Lists.newArrayList(key, value);

        // Construct a minimal olap table for row binlog schema generation.
        OlapTable table = new OlapTable(1L, "tbl", baseSchema, KeysType.PRIMARY_KEYS, null, null);
        table.setBaseIndexId(baseIndexId);
        MaterializedIndexMeta baseIndexMeta = new MaterializedIndexMeta(baseIndexId, baseSchema, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.PRIMARY_KEYS, null);
        table.addIndexIdToMetaForUnitTest(baseIndexId, baseIndexMeta);
        table.addIndexNameToIdForUnitTest("base", baseIndexId);
        table.setBinlogConfig(binlogConfig);

        if (binlogConfig.isEnableForStreaming()) {
            // Mock row binlog meta by using generated schema to make getRowBinlogMeta() work in pure unit test.
            long rowBinlogIndexId = 2L;
            List<Column> rowBinlogSchema = table.generateTableRowBinlogSchema();
            MaterializedIndexMeta rowBinlogMeta = new MaterializedIndexMeta(rowBinlogIndexId, rowBinlogSchema, 1, 1,
                    (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null, null, null, null);
            rowBinlogMeta.initSchemaColumnUniqueId();
            table.setRowBinlogMeta(rowBinlogMeta, "row_binlog");
        }
        return table;
    }

    @Test
    public void testRowBinlogSchemaOnEnable() {
        OlapTable tableWithoutBefore = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(true, false));
        Assertions.assertTrue(tableWithoutBefore.needRowBinlog());
        List<String> tableWithoutBeforeColumns =
                tableWithoutBefore.getRowBinlogMeta().getSchema(true).stream().map(Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertFalse(tableWithoutBeforeColumns.contains(Column.generateBeforeColName("v1")));
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.BINLOG_LSN_COL), 2);
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.BINLOG_OPERATION_COL), 3);
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.BINLOG_TIMESTAMP_COL), 4);
        Assertions.assertEquals(tableWithoutBeforeColumns.size(), 5);

        OlapTable tableWithBefore = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(true, true));
        Assertions.assertTrue(tableWithBefore.needRowBinlog());
        List<String> tableWithBeforeColumns =
                tableWithBefore.getRowBinlogMeta().getSchema(true).stream().map(Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertTrue(tableWithBeforeColumns.contains(Column.generateBeforeColName("v1")));
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.BINLOG_LSN_COL), 3);
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.BINLOG_OPERATION_COL), 4);
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.BINLOG_TIMESTAMP_COL), 5);
        Assertions.assertEquals(tableWithBeforeColumns.size(), 6);
    }

    @Test
    public void testRowBinlogSchemaOnDisable() {
        OlapTable table = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(false, false));
        Assertions.assertFalse(table.needRowBinlog());
        Assertions.assertTrue(table.getBaseIndexMeta().getRowBinlogIndexId() <= 0);
    }
}
