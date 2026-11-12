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

public class OlapTableChangelogSchemaTest {

    private static OlapTable newTestTable(ChangelogConfig changelogConfig) {
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column value = new Column("v1", Type.INT, false, null, false, "7", "");
        value.setIsKey(false);
        return newTestTable(changelogConfig, Lists.newArrayList(key, value));
    }

    private static OlapTable newTestTable(ChangelogConfig changelogConfig, List<Column> baseSchema) {
        long baseIndexId = 1L;

        // Construct a minimal olap table for changelog schema generation.
        OlapTable table = new OlapTable(1L, "tbl", baseSchema, KeysType.PRIMARY_KEYS, null, null);
        table.setBaseIndexId(baseIndexId);
        MaterializedIndexMeta baseIndexMeta = new MaterializedIndexMeta(baseIndexId, baseSchema, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.PRIMARY_KEYS, null);
        table.addIndexIdToMetaForUnitTest(baseIndexId, baseIndexMeta);
        table.addIndexNameToIdForUnitTest("base", baseIndexId);

        if (changelogConfig.isEnable()) {
            table.setChangelogConfig(changelogConfig);
            // Mock changelog meta by using generated schema to make getChangelogIndexMeta() work in pure unit test.
            long changelogIndexId = 2L;
            List<Column> changelogSchema = table.generateTableChangelogSchema();
            MaterializedIndexMeta changelogIndexMeta = new MaterializedIndexMeta(changelogIndexId, changelogSchema, 1, 1,
                    (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null, null, null, null);
            changelogIndexMeta.initSchemaColumnUniqueId();
            table.setChangelogIndexMeta(changelogIndexMeta, "changelog");
        }
        return table;
    }

    @Test
    public void testChangelogSchemaOnEnable() {
        OlapTable tableWithoutBefore = newTestTable(BinlogTestUtils.newTestChangelogConfig(true, false));
        Assertions.assertTrue(tableWithoutBefore.hasChangelog());
        Assertions.assertFalse(tableWithoutBefore.getBaseSchema(true).get(1).isAllowNull());
        Assertions.assertEquals("7", tableWithoutBefore.getBaseSchema(true).get(1).getDefaultValue());
        List<String> tableWithoutBeforeColumns =
                tableWithoutBefore.getChangelogIndexMeta().getSchema(true).stream().map(Column::getName)
                        .collect(Collectors.toList());
        Assertions.assertFalse(tableWithoutBeforeColumns.contains(Column.generateBeforeColName("v1")));
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.CHANGELOG_TSO_COL), 2);
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.CHANGELOG_LSN_COL), 3);
        Assertions.assertEquals(tableWithoutBeforeColumns.indexOf(Column.CHANGELOG_OP_COL), 4);
        Assertions.assertEquals(tableWithoutBeforeColumns.size(), 5);
        Column afterColumnWithoutBefore = tableWithoutBefore.getChangelogIndexMeta().getSchema(true).stream()
                .filter(column -> column.getName().equals("v1"))
                .findFirst()
                .orElseThrow();
        Assertions.assertTrue(afterColumnWithoutBefore.isAllowNull());
        Assertions.assertNull(afterColumnWithoutBefore.getDefaultValue());
        Assertions.assertNull(afterColumnWithoutBefore.getDefaultValueExprDef());
        Assertions.assertNull(afterColumnWithoutBefore.getRealDefaultValue());

        OlapTable tableWithBefore = newTestTable(BinlogTestUtils.newTestChangelogConfig(true, true));
        Assertions.assertTrue(tableWithBefore.hasChangelog());
        List<Column> changelogSchemaWithBefore = tableWithBefore.getChangelogIndexMeta().getSchema(true);
        List<String> tableWithBeforeColumns =
                changelogSchemaWithBefore.stream().map(Column::getName).collect(Collectors.toList());
        Assertions.assertTrue(tableWithBeforeColumns.contains(Column.generateBeforeColName("v1")));
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.CHANGELOG_TSO_COL), 3);
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.CHANGELOG_LSN_COL), 4);
        Assertions.assertEquals(tableWithBeforeColumns.indexOf(Column.CHANGELOG_OP_COL), 5);
        Assertions.assertEquals(tableWithBeforeColumns.size(), 6);
        Column afterColumnWithBefore = changelogSchemaWithBefore.stream()
                .filter(column -> column.getName().equals("v1"))
                .findFirst()
                .orElseThrow();
        Assertions.assertTrue(afterColumnWithBefore.isAllowNull());
        Assertions.assertNull(afterColumnWithBefore.getDefaultValue());
        Column beforeColumn = changelogSchemaWithBefore.stream()
                .filter(column -> column.getName().equals(Column.generateBeforeColName("v1")))
                .findFirst()
                .orElseThrow();
        Assertions.assertTrue(beforeColumn.isAllowNull());
        Assertions.assertNull(beforeColumn.getDefaultValue());
        Assertions.assertNull(beforeColumn.getDefaultValueExprDef());
        Assertions.assertNull(beforeColumn.getRealDefaultValue());
    }

    @Test
    public void testChangelogSchemaOnDisable() {
        OlapTable table = newTestTable(BinlogTestUtils.newTestChangelogConfig(false, false));
        Assertions.assertFalse(table.hasChangelog());
        Assertions.assertTrue(table.getBaseIndexMeta().getChangelogIndexId() <= 0);
    }

    @Test
    public void testChangelogSchemaIncludesHiddenKeyColumns() {
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column hiddenKey = new Column("__DORIS_TEST_HIDDEN_KEY__", PrimitiveType.BIGINT);
        hiddenKey.setIsKey(true);
        hiddenKey.setIsVisible(false);
        Column value = new Column("v1", PrimitiveType.INT);
        value.setIsKey(false);
        Column hiddenValue = new Column("__DORIS_TEST_HIDDEN_VALUE__", PrimitiveType.INT);
        hiddenValue.setIsKey(false);
        hiddenValue.setIsVisible(false);

        OlapTable table = newTestTable(BinlogTestUtils.newTestChangelogConfig(true, true),
                Lists.newArrayList(key, hiddenKey, value, hiddenValue));

        List<String> columnNames = table.getChangelogIndexMeta().getSchema(true).stream().map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals("k1", columnNames.get(0));
        Assertions.assertEquals("__DORIS_TEST_HIDDEN_KEY__", columnNames.get(1));
        Assertions.assertEquals("v1", columnNames.get(2));
        Assertions.assertFalse(columnNames.contains("__DORIS_TEST_HIDDEN_VALUE__"));
        Assertions.assertTrue(columnNames.contains(Column.generateBeforeColName("v1")));
        Assertions.assertFalse(columnNames.contains(Column.generateBeforeColName("__DORIS_TEST_HIDDEN_KEY__")));
        Assertions.assertEquals(4, columnNames.indexOf(Column.CHANGELOG_TSO_COL));
        Assertions.assertEquals(5, columnNames.indexOf(Column.CHANGELOG_LSN_COL));
    }

    @Test
    public void testChangelogSchemaSkipsHiddenNonKeyColumnBeforeVisibleColumn() {
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column hiddenValue = new Column("__DORIS_TEST_HIDDEN_VALUE__", PrimitiveType.INT);
        hiddenValue.setIsKey(false);
        hiddenValue.setIsVisible(false);
        Column value = new Column("v1", PrimitiveType.INT);
        value.setIsKey(false);

        OlapTable table = newTestTable(BinlogTestUtils.newTestChangelogConfig(true, false),
                Lists.newArrayList(key, hiddenValue, value));
        List<String> columnNames = table.getChangelogIndexMeta().getSchema(true).stream().map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals("k1", columnNames.get(0));
        Assertions.assertEquals("v1", columnNames.get(1));
        Assertions.assertFalse(columnNames.contains("__DORIS_TEST_HIDDEN_VALUE__"));
        Assertions.assertEquals(2, columnNames.indexOf(Column.CHANGELOG_TSO_COL));
        Assertions.assertEquals(3, columnNames.indexOf(Column.CHANGELOG_LSN_COL));
    }
}
