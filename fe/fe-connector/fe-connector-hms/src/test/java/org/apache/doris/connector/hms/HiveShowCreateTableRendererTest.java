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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Byte-parity tests for {@link HiveShowCreateTableRenderer} — the connector-side port of legacy
 * {@code HiveMetaStoreClientHelper.showCreateTable} that native hive SHOW CREATE TABLE relies on.
 *
 * <p>The load-bearing details the acceptance suites (test_hive_show_create_table / _ddl_text_format /
 * _meta_cache / test_multi_delimit_serde / test_hive_ddl) discriminate on: the TWO quoting conventions
 * (SERDEPROPERTIES {@code 'k' = 'v'} with spaces vs TBLPROPERTIES {@code 'k'='v'} without), the 2-space data /
 * 1-space partition column indents, the null-comment guard (an empty {@code COMMENT ''} would break the
 * meta-cache column substring), and lifting the {@code comment} table param to a top-level COMMENT clause.</p>
 */
public class HiveShowCreateTableRendererTest {

    private static ConnectorColumn col(String name, String typeName, String comment) {
        return new ConnectorColumn(name, ConnectorType.of(typeName), comment, true, null);
    }

    /** A partitioned TEXT/LazySimpleSerDe table with a commented + an uncommented column and a comment param. */
    private static HmsTableInfo textTable() {
        Map<String, String> serdeParams = new LinkedHashMap<>();
        serdeParams.put("field.delim", "|");
        Map<String, String> tableParams = new LinkedHashMap<>();
        tableParams.put("comment", "my table");
        tableParams.put("doris.file_format", "text");
        return HmsTableInfo.builder()
                .dbName("db").tableName("t")
                .columns(Arrays.asList(col("id", "INT", null), col("name", "STRING", "the name")))
                .partitionKeys(Collections.singletonList(col("dt", "DATEV2", null)))
                .serializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                .sdParameters(serdeParams)
                .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
                .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .location("hdfs://ns/wh/t")
                .parameters(tableParams)
                .build();
    }

    @Test
    public void testColumnBlockIndentTypesAndNullCommentGuard() {
        // 2-space data-column indent, mapped hive type strings, and the null-comment guard: `id` has NO comment
        // token (a spurious COMMENT '' would break test_hive_meta_cache's exact ``k3` string)` substring),
        // `name` carries its comment, and the block closes with `)\n`.
        String ddl = HiveShowCreateTableRenderer.render(textTable());
        Assertions.assertTrue(ddl.contains("CREATE TABLE `t`(\n  `id` int,\n  `name` string COMMENT 'the name')\n"),
                ddl);
    }

    @Test
    public void testTableCommentLiftedAndPartitionOneSpaceIndent() {
        String ddl = HiveShowCreateTableRenderer.render(textTable());
        // The `comment` table param becomes a top-level COMMENT clause (not a TBLPROPERTY).
        Assertions.assertTrue(ddl.contains("COMMENT 'my table'\n"), ddl);
        // Partition columns use a ONE-space indent (data columns use two), matching legacy.
        Assertions.assertTrue(ddl.contains("PARTITIONED BY (\n `dt` date)\n"), ddl);
    }

    @Test
    public void testSerdeAndFormatBlocks() {
        String ddl = HiveShowCreateTableRenderer.render(textTable());
        Assertions.assertTrue(ddl.contains(
                "ROW FORMAT SERDE\n  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n"), ddl);
        Assertions.assertTrue(ddl.contains(
                "STORED AS INPUTFORMAT\n  'org.apache.hadoop.mapred.TextInputFormat'\n"
                        + "OUTPUTFORMAT\n  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"), ddl);
        Assertions.assertTrue(ddl.contains("LOCATION\n  'hdfs://ns/wh/t'\n"), ddl);
    }

    @Test
    public void testTwoQuotingConventions() {
        // The discriminator the suites rely on: SERDEPROPERTIES has SPACES around '=', TBLPROPERTIES has NONE.
        String ddl = HiveShowCreateTableRenderer.render(textTable());
        Assertions.assertTrue(ddl.contains("WITH SERDEPROPERTIES (\n  'field.delim' = '|')\n"), ddl);
        Assertions.assertTrue(ddl.contains("'doris.file_format'='text'"), ddl);
    }

    @Test
    public void testCommentParamNotLeakedIntoTblproperties() {
        // The comment was lifted to COMMENT '...'; it must NOT also appear as a TBLPROPERTY ('comment'='my table').
        String ddl = HiveShowCreateTableRenderer.render(textTable());
        Assertions.assertFalse(ddl.contains("'comment'='my table'"), ddl);
    }
}
