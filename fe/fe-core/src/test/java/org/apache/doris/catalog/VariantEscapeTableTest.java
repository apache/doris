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

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariantEscapeTableTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("variant_escape_table");
        connectContext.setDatabase("variant_escape_table");
        connectContext.getSessionVariable().defaultVariantEnableNestedGroup = false;
    }

    private OlapTable table(String name) throws Exception {
        return (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("variant_escape_table")
                .getTableOrMetaException(name);
    }

    @Test
    public void testPropertyPersistenceAndFalseOmission() throws Exception {
        createTable("CREATE TABLE escaped(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='true')");
        createTable("CREATE TABLE legacy(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='false')");
        Assertions.assertTrue(table("escaped").getVariantEscapeDotsInKeys());
        Assertions.assertFalse(table("legacy").getVariantEscapeDotsInKeys());
        Assertions.assertFalse(table("legacy").getTableProperty().getProperties()
                .containsKey(PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS));
        TableProperty restored = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(table("escaped").getTableProperty()), TableProperty.class);
        restored.gsonPostProcess();
        Assertions.assertTrue(restored.getVariantEscapeDotsInKeys());
        Assertions.assertFalse(table("escaped").getColumn("v").getType().toSql()
                .contains("variant_escape_dots_in_keys"));
        Assertions.assertTrue(new OlapTableWrapper(table("escaped"), java.util.Map.of()).getVariantEscapeDotsInKeys());
    }

    @Test
    public void testColumnSyntaxAndAlterAreRejected() throws Exception {
        for (String value : new String[] {"true", "false"}) {
            Exception error = Assertions.assertThrows(Exception.class, () -> new NereidsParser().parseSingle(
                    "SELECT CAST('{}' AS VARIANT<PROPERTIES('variant_escape_dots_in_keys'='" + value + "')>)"));
            Assertions.assertTrue(error.getMessage().contains("is a table property"), error.getMessage());
        }
        for (String original : new String[] {"true", "false"}) {
            String name = "alter_" + original;
            createTable("CREATE TABLE " + name + "(k INT, v VARIANT) DUPLICATE KEY(k) "
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                    + "'variant_escape_dots_in_keys'='" + original + "')");
            for (String target : new String[] {"true", "false"}) {
                Exception error = Assertions.assertThrows(Exception.class, () -> alterTableSync(
                        "ALTER TABLE " + name + " SET ('variant_escape_dots_in_keys'='" + target + "')"));
                Assertions.assertTrue(error.getMessage().contains("only be specified when creating"),
                        error.getMessage());
            }
        }
    }

    @Test
    public void testNestedGroupFinalTypeIsForbidden() throws Exception {
        connectContext.getSessionVariable().defaultVariantEnableNestedGroup = true;
        try {
            Exception error = Assertions.assertThrows(Exception.class, () -> createTable(
                    "CREATE TABLE nested_default(k INT, v VARIANT<PROPERTIES('variant_max_subcolumns_count'='1')>) "
                    + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true')"));
            Assertions.assertTrue(error.getMessage().contains("Escaped tables do not support NestedGroup"),
                    error.getMessage());
        } finally {
            connectContext.getSessionVariable().defaultVariantEnableNestedGroup = false;
        }
    }

    @Test
    public void testCtasBoundSourcesAndExplicitPolicy() throws Exception {
        createTable("CREATE TABLE source(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='true')");
        createTable("CREATE TABLE inherited DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1') AS WITH c AS (SELECT k FROM source) SELECT k FROM c");
        Assertions.assertTrue(table("inherited").getVariantEscapeDotsInKeys());
        alterTableSync("ALTER TABLE inherited ADD COLUMN v VARIANT");
        Assertions.assertTrue(table("inherited").getVariantEscapeDotsInKeys());
        createTable("CREATE TABLE overridden DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='false') AS SELECT k,v FROM source");
        Assertions.assertFalse(table("overridden").getVariantEscapeDotsInKeys());
        createTable("CREATE TABLE subquery_source DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1') AS SELECT CAST(1 AS INT) k, (SELECT max(k) FROM source) x");
        Assertions.assertTrue(table("subquery_source").getVariantEscapeDotsInKeys());
    }
}
