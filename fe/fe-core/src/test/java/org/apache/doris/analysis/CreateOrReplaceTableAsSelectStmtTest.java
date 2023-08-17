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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * test for CreateOrReplaceTableAsSelectStmt.
 */
class CreateOrReplaceTableAsSelectStmtTest extends TestWithFeService {
    private final String createTable = "CREATE TABLE";
    private final String createOrReplaceTable = "CREATE OR REPLACE TABLE";
    private final String sourceTable = "source_table";
    private final String db = "test";
    private LinkedHashMap<String, String> tableProperties;
    private String distributionBucket;
    private List<ColumnDef> columnDefs;

    protected String columnName(DataType dataType) {
        return "c_" + dataType.simpleString();
    }

    protected String toCreateSql(String tableName, String[] keys, String defaultDistributionKey,
                                 List<ColumnDef> columnDefs, Map<String, String> prop) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(createTable);
        stringBuilder.append(" `").append(tableName).append("` (\n");
        int end = columnDefs.size();
        int i = 0;
        String distributionKey = defaultDistributionKey;
        for (ColumnDef cd : columnDefs) {
            i++;
            stringBuilder.append("  ").append(cd.toColumn().toSql(false, true));
            if (i != end) {
                stringBuilder.append(",");
            }
            if (distributionKey == null) {
                distributionKey = cd.getName();
            }
            stringBuilder.append("\n");
        }
        stringBuilder.append(") ENGINE=OLAP\n");

        String key = "`" + distributionKey + "`";
        StringBuilder keyBuilder = new StringBuilder();
        if (keys != null && keys.length > 0) {
            for (int idx = 0, keysLen = keys.length; idx < keysLen; idx++) {
                keyBuilder.append("`").append(keys[idx]).append("`");
                if (idx == keysLen - 1) {
                    continue;
                }
                keyBuilder.append(", ");
            }
            key = keyBuilder.toString();
        }
        stringBuilder.append("DUPLICATE KEY(").append(key).append(")\n");
        stringBuilder.append("COMMENT 'OLAP'\n");
        stringBuilder.append("DISTRIBUTED BY HASH(`").append(distributionKey).append("`) BUCKETS ")
                .append(distributionBucket).append("\n");

        stringBuilder.append("PROPERTIES (\n");
        end = prop.size();
        i = 0;
        for (Map.Entry<String, String> entry : prop.entrySet()) {
            i++;
            stringBuilder.append("\"").append(entry.getKey())
                    .append("\" = \"").append(entry.getValue()).append("\"");
            if (i != end) {
                stringBuilder.append(",");
            }
            stringBuilder.append("\n");
        }
        stringBuilder.append(");");
        return stringBuilder.toString();
    }

    protected String toCreateOrReplaceSql(String tableName,
                                          List<String> names, Map<String, String> prop) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(createOrReplaceTable).append(" `").append(tableName).append("`\n");

        stringBuilder.append("PROPERTIES(");
        int end = prop.size();
        int k = 0;
        for (Map.Entry<String, String> entry : prop.entrySet()) {
            k++;
            stringBuilder.append("\"").append(entry.getKey())
                    .append("\" = \"").append(entry.getValue()).append("\"");
            if (k != end) {
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(")\n");

        stringBuilder.append(" as select ");
        for (int i = 0, len = names.size(); i < len; i++) {
            stringBuilder.append(names.get(i));
            if (i != len - 1) {
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(" from `").append(sourceTable).append("`;");
        return stringBuilder.toString();
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(db);
        useDatabase(db);

        TypeDef[] typeDefs = {
                new TypeDef(ScalarType.createType(PrimitiveType.INT)),
                new TypeDef(ScalarType.createType(PrimitiveType.STRING)),
                new TypeDef(ScalarType.createType(PrimitiveType.FLOAT)),
                new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)),
                new TypeDef(ScalarType.createType(PrimitiveType.BOOLEAN)),
                new TypeDef(ScalarType.createType(PrimitiveType.DATETIMEV2)),
                new TypeDef(ScalarType.createType(PrimitiveType.DATEV2)),
                new TypeDef(ScalarType.createDecimalType(10, 2)),
                new TypeDef(ScalarType.createType(PrimitiveType.DOUBLE)),
                new TypeDef(ScalarType.createType(PrimitiveType.JSONB)),
                new TypeDef(ScalarType.createType(PrimitiveType.LARGEINT)),
                new TypeDef(ScalarType.createVarcharType(200))
        };
        columnDefs = new ArrayList<>(typeDefs.length);
        for (int i = 0, len = typeDefs.length; i < len; i++) {
            ColumnDef columnDef = new ColumnDef("col" + (i + 1), typeDefs[i]);
            if (i == 0) {
                columnDef.setIsKey(true);
            }
            columnDefs.add(columnDef);
        }

        tableProperties = new LinkedHashMap<>();
        tableProperties.put("replication_allocation", "tag.location.default: 1");
        tableProperties.put("is_being_synced", "false");
        tableProperties.put("storage_format", "V2");
        tableProperties.put("light_schema_change", "true");
        tableProperties.put("disable_auto_compaction", "false");
        tableProperties.put("enable_single_replica_compaction", "false");

        distributionBucket = "10";

        createTable(toCreateSql(sourceTable, null, null, columnDefs, tableProperties));
    }

    private void execCortas(String sql, String table, String expectExplainRes) throws Exception {
        createOrReplaceTableAsSelect(sql);
        Assertions.assertEquals(expectExplainRes, showCreateTableByName(table).getResultRows().get(0).get(1));
    }

    @Test
    public void testCorrectnessWithDecimalAndFunction() throws Exception {
        Map<String, String> prop = new LinkedHashMap<>();
        prop.put("replication_num", "1");

        String dstTbl = "select_decimal_table";

        execCortas(toCreateOrReplaceSql(dstTbl, Arrays.asList("*"), prop),
                dstTbl, toCreateSql(dstTbl, null, null, columnDefs, tableProperties));

        ColumnDef decimalP38S2 = new ColumnDef("col7", new TypeDef(ScalarType.createDecimalType(38, 2)), true);
        ColumnDef decimalP27S9 = new ColumnDef("col7", new TypeDef(ScalarType.createDecimalType(27, 9)), true);
        ColumnDef countColumnDef = new ColumnDef("cnt", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT)), true);
        countColumnDef.setIsKey(true);
        decimalP38S2.setIsKey(true);
        decimalP27S9.setIsKey(true);
        ColumnDef decimalV2 = null;
        for (ColumnDef cd : columnDefs) {
            if (cd.getType().isDecimalV2() || cd.getType().isDecimalV3()) {
                decimalV2 = cd;
                break;
            }
        }
        String[] keys = new String[] {decimalP38S2.getName(), countColumnDef.getName()};
        if (Config.enable_decimal_conversion) {
            execCortas(toCreateOrReplaceSql(dstTbl,
                            Arrays.asList("sum(" + decimalV2.getName() + ") as " + decimalP38S2.getName(),
                                    "count(*) as " + countColumnDef.getName()), prop),
                    dstTbl,
                    toCreateSql(dstTbl, keys, decimalP38S2.getName(), Arrays.asList(decimalP38S2, countColumnDef),
                            tableProperties));
        } else {
            execCortas(toCreateOrReplaceSql(dstTbl,
                            Arrays.asList("sum(" + decimalV2.getName() + ") as " + decimalP38S2.getName(),
                                    "count(*) as " + countColumnDef.getName()), prop),
                    dstTbl,
                    toCreateSql(dstTbl, keys, decimalP38S2.getName(), Arrays.asList(decimalP27S9, countColumnDef),
                            tableProperties));
        }
    }
}
