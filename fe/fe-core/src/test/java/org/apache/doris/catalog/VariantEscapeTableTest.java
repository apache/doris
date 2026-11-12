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
import org.apache.doris.nereids.trees.plans.commands.CreateTableLikeCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
    public void testCopyTabletDdl() throws Exception {
        checkCopyTabletDdl(false);
    }

    @Test
    public void testCopyTabletLikeDdl() throws Exception {
        checkCopyTabletDdl(true);
    }

    private void checkCopyTabletDdl(boolean forLike) throws Exception {
        for (boolean escaped : new boolean[] {true, false}) {
            String sourceName = "copy_tablet_" + forLike + "_" + escaped;
            String targetName = sourceName + "_restored";
            createTable("CREATE TABLE " + sourceName + "(k INT, v VARIANT) DUPLICATE KEY(k) "
                    + "DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1',"
                    + "'variant_escape_dots_in_keys'='" + escaped + "')");
            List<String> statements = new ArrayList<>();
            if (forLike) {
                CreateTableLikeCommand command = (CreateTableLikeCommand) new NereidsParser().parseSingle(
                        "CREATE TABLE " + targetName + " LIKE " + sourceName);
                Env.getCreateTableLikeStmt(command.getInfo(), null, table(sourceName), statements, null, null,
                        false, true, false, 42L, false, false);
            } else {
                // ADMIN COPY TABLET uses this entry point with the snapshot version.
                Env.getDdlStmt(table(sourceName), statements, null, null, false, true, 42L);
            }
            Assertions.assertEquals(1, statements.size());
            String ddl = statements.get(0);
            Assertions.assertTrue(ddl.contains("DISTRIBUTED BY HASH(k) BUCKETS 1"), ddl);
            Assertions.assertTrue(ddl.contains("\"replication_num\" = \"1\""), ddl);
            Assertions.assertTrue(ddl.contains("\"version_info\" = \"42\""), ddl);
            Assertions.assertEquals(escaped, ddl.contains("\"variant_escape_dots_in_keys\" = \"true\""), ddl);
            Assertions.assertFalse(ddl.contains("\"variant_escape_dots_in_keys\" = \"false\""), ddl);

            createTable(ddl.replace("`" + sourceName + "`", "`" + targetName + "`"));
            Assertions.assertEquals(escaped, table(targetName).getVariantEscapeDotsInKeys());
            Assertions.assertEquals(42L, table(targetName).getPartitions().iterator().next().getVisibleVersion());
        }
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
        Exception explicit = Assertions.assertThrows(Exception.class, () -> createTable(
                "CREATE TABLE nested_explicit(k INT, v VARIANT<PROPERTIES('variant_enable_nested_group'='true')>) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true')"));
        Assertions.assertTrue(explicit.getMessage().contains("Escaped tables do not support NestedGroup"));
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

    @Test
    public void testAsyncMvInheritanceAndExplicitPolicy() throws Exception {
        createTable("CREATE TABLE async_source(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='true')");
        for (String policy : new String[] {"inherited", "true"}) {
            String explicit = policy.equals("inherited") ? ""
                    : ",'variant_escape_dots_in_keys'='" + policy + "'";
            createMvByNereids("CREATE MATERIALIZED VIEW async_" + policy
                    + " BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES('replication_num'='1','storage_medium'='HDD'" + explicit + ") "
                    + "AS WITH c AS (SELECT k,v FROM async_source) SELECT k,v FROM c");
            Assertions.assertEquals(!policy.equals("false"), table("async_" + policy).getVariantEscapeDotsInKeys());
        }
        Exception mismatch = Assertions.assertThrows(Exception.class, () -> createMvByNereids(
                "CREATE MATERIALIZED VIEW async_false BUILD DEFERRED REFRESH COMPLETE ON MANUAL "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='false') AS SELECT k,v FROM async_source"));
        Assertions.assertTrue(mismatch.getMessage().contains("async MV must use the same value"));
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbOrMetaException("variant_escape_table")
                .getTableNullable("async_false"));
    }

}
