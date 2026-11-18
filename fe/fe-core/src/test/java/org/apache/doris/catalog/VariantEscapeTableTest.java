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
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TSlotDescriptor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class VariantEscapeTableTest extends TestWithFeService {
    private boolean previousVariantV2;
    private boolean previousVariantEscape;

    @org.junit.jupiter.api.BeforeEach
    public void enableVariantV2() {
        previousVariantV2 = org.apache.doris.common.Config.enable_variant_v2;
        previousVariantEscape = org.apache.doris.common.Config.enable_variant_escape_dots_in_keys;
        org.apache.doris.common.Config.enable_variant_escape_dots_in_keys = true;
        org.apache.doris.common.Config.enable_variant_v2 = true;
    }

    @org.junit.jupiter.api.AfterEach
    public void restoreVariantV2() {
        org.apache.doris.common.Config.enable_variant_v2 = previousVariantV2;
        org.apache.doris.common.Config.enable_variant_escape_dots_in_keys = previousVariantEscape;
    }

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
    public void testRollingUpgradeGateRejectsCreationAndLoads() throws Exception {
        createTable("CREATE TABLE upgrade_source(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='true')");
        org.apache.doris.common.Config.enable_variant_escape_dots_in_keys = false;
        Exception create = Assertions.assertThrows(Exception.class, () -> createTable(
                "CREATE TABLE upgrade_disabled(k INT, v VARIANT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                        + "'variant_escape_dots_in_keys'='true')"));
        Assertions.assertTrue(create.getMessage().contains("disabled during rolling upgrades"), create.getMessage());
        Exception like = Assertions.assertThrows(Exception.class,
                () -> createLike("CREATE TABLE upgrade_like LIKE upgrade_source"));
        Assertions.assertTrue(like.getMessage().contains("disabled during rolling upgrades"), like.getMessage());
        org.apache.doris.planner.OlapTableSink sink = new org.apache.doris.planner.OlapTableSink(
                table("upgrade_source"), null, java.util.Collections.emptyList(), false);
        Exception load = Assertions.assertThrows(Exception.class,
                () -> sink.init(new org.apache.doris.thrift.TUniqueId(1, 2), 1, 1, 60, 1, false, false, 60));
        Assertions.assertTrue(load.getMessage().contains("disabled during rolling upgrades"), load.getMessage());
        createTable("CREATE TABLE upgrade_legacy(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testNonOlapCreateRejectsExplicitEncoding() throws Exception {
        for (String value : new String[] {"true", "false"}) {
            String ddl = "CREATE TABLE external_encoding_" + value + "(k INT) ENGINE=ELASTICSEARCH "
                    + "PROPERTIES('hosts'='http://127.0.0.1:9200','index'='test',"
                    + "'variant_escape_dots_in_keys'='" + value + "')";
            Exception error = Assertions.assertThrows(Exception.class, () -> createTable(ddl));
            Assertions.assertTrue(error.getMessage().contains("requires an OLAP table"), error.getMessage());
            String ctas = "CREATE TABLE external_ctas_" + value + " ENGINE=ELASTICSEARCH "
                    + "PROPERTIES('hosts'='http://127.0.0.1:9200','index'='test',"
                    + "'variant_escape_dots_in_keys'='" + value + "') AS SELECT 1 AS k";
            Exception ctasError = Assertions.assertThrows(Exception.class, () -> createTable(ctas));
            Assertions.assertTrue(ctasError.getMessage().contains("requires an OLAP table"), ctasError.getMessage());
        }
        createTable("CREATE TABLE external_no_encoding(k INT) ENGINE=ELASTICSEARCH "
                + "PROPERTIES('hosts'='http://127.0.0.1:9200','index'='test')");
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
            // A re-created table must keep its source encoding, so a legacy table's DDL pins false.
            Assertions.assertEquals(!escaped, ddl.contains("\"variant_escape_dots_in_keys\" = \"false\""), ddl);

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
    }

    @Test
    public void testActivationSwitchSelectsCreationDefault() throws Exception {
        createTable("CREATE TABLE default_escaped(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        Assertions.assertTrue(table("default_escaped").getVariantEscapeDotsInKeys());
        createTable("CREATE TABLE default_explicit_false(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='false')");
        Assertions.assertFalse(table("default_explicit_false").getVariantEscapeDotsInKeys());
        createTable("CREATE TABLE default_non_variant(k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        Assertions.assertFalse(table("default_non_variant").getVariantEscapeDotsInKeys());

        // LIKE re-creates an existing table, so it keeps that table's encoding instead of the default.
        createLike("CREATE TABLE default_like_false LIKE default_explicit_false");
        Assertions.assertFalse(table("default_like_false").getVariantEscapeDotsInKeys());
        createLike("CREATE TABLE default_like_escaped LIKE default_escaped");
        Assertions.assertTrue(table("default_like_escaped").getVariantEscapeDotsInKeys());

        org.apache.doris.common.Config.enable_variant_escape_dots_in_keys = false;
        createTable("CREATE TABLE default_off(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        Assertions.assertFalse(table("default_off").getVariantEscapeDotsInKeys());
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
    public void testCtasDoesNotInheritStorageTemplates() throws Exception {
        createTable("CREATE TABLE template_legacy(k INT, v VARIANT<MATCH_NAME '50%':STRING,"
                + "MATCH_NAME 'a%2Eb':STRING, PROPERTIES('variant_max_subcolumns_count'='3',"
                + "'variant_enable_nested_group'='false')>) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='false')");
        createTable("CREATE TABLE template_escaped(k INT, v VARIANT<MATCH_NAME '50%25':STRING,"
                + "MATCH_NAME 'a%2Eb':STRING, PROPERTIES('variant_enable_nested_group'='false')>) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true')");
        createTable("CREATE TABLE template_escaped_int(k INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true')");
        for (String policy : new String[] {"inherited", "true", "false"}) {
            String explicit = policy.equals("inherited") ? ""
                    : ",'variant_escape_dots_in_keys'='" + policy + "'";
            String name = "template_ctas_" + policy;
            createTable("CREATE TABLE " + name + " DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES('replication_num'='1'" + explicit + ") AS SELECT l.k,l.v "
                    + "FROM template_legacy l JOIN template_escaped_int e ON l.k=e.k");
            Assertions.assertEquals(!policy.equals("false"), table(name).getVariantEscapeDotsInKeys());
            Assertions.assertTrue(((VariantType) table(name).getColumn("v").getType()).getPredefinedFields().isEmpty());
            Assertions.assertEquals(3, table(name).getColumn("v").getVariantMaxSubcolumnsCount());
        }
        createTable("CREATE TABLE template_ctas_to_legacy DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='false') "
                + "AS SELECT k,v FROM template_escaped");
        Assertions.assertFalse(table("template_ctas_to_legacy").getVariantEscapeDotsInKeys());
        Assertions.assertTrue(((VariantType) table("template_ctas_to_legacy").getColumn("v").getType())
                .getPredefinedFields().isEmpty());
        Assertions.assertEquals(2, ((VariantType) table("template_legacy").getColumn("v").getType())
                .getPredefinedFields().size());
        Assertions.assertEquals(2, ((VariantType) table("template_escaped").getColumn("v").getType())
                .getPredefinedFields().size());
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

    @Test
    public void testScanSlotPathsUseStorageSpelling() throws Exception {
        for (boolean escaped : new boolean[] {true, false}) {
            String name = "slot_paths_" + escaped;
            createTable("CREATE TABLE " + name + "(k INT, v VARIANT) DUPLICATE KEY(k) "
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                    + "'variant_escape_dots_in_keys'='" + escaped + "')");
            Planner planner = getSqlStmtExecutor(
                    "SELECT v['a.b'], v['a']['b'], v['50%'] FROM " + name).planner();
            Set<String> paths = new TreeSet<>();
            for (TSlotDescriptor thrift : planner.getDescTable().toThrift().getSlotDescriptors()) {
                if (thrift.isSetColumnPaths() && !thrift.getColumnPaths().isEmpty()) {
                    paths.add(thrift.getColumnPaths() + "=" + thrift.getColName());
                }
            }
            // Escaped tables send storage components, so a literal dotted key and a nested path stay distinct.
            Set<String> expected = escaped
                    ? ImmutableSet.of("[a%2Eb]=v.a%2Eb", "[a, b]=v.a.b", "[50%25]=v.50%25")
                    : ImmutableSet.of("[a.b]=v.a.b", "[a, b]=v.a.b", "[50%]=v.50%");
            Assertions.assertEquals(new TreeSet<>(expected), paths, name);
        }
    }

    @Test
    public void testV1CreationAndInheritanceFailClosed() throws Exception {
        createTable("CREATE TABLE v2_source(k INT, padding INT, v VARIANT) DUPLICATE KEY(k,padding) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='true')");
        org.apache.doris.common.Config.enable_variant_v2 = false;
        // The activation switch selects the creation default, so an omitted property requires V2.
        assertV2Required(() -> createTable("CREATE TABLE v1_default(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')"));
        createTable("CREATE TABLE v1_false(k INT, v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='false')");
        Assertions.assertFalse(table("v1_false").getVariantEscapeDotsInKeys());
        assertV2Required(() -> createTable("CREATE TABLE v1_true(k INT,v VARIANT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1',"
                + "'variant_escape_dots_in_keys'='true')"));
        assertV2Required(() -> createLike("CREATE TABLE v1_like LIKE v2_source"));
        assertV2Required(() -> createTable("CREATE TABLE v1_ctas DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1') AS SELECT k,v FROM v2_source"));
        assertV2Required(() -> createTable("CREATE TABLE v1_ctas_explicit DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true') "
                + "AS SELECT k,v FROM v1_false"));
        assertV2Required(() -> createMvByNereids("CREATE MATERIALIZED VIEW v1_async "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1') AS SELECT k,v FROM v2_source"));
        assertV2Required(() -> createMvByNereids("CREATE MATERIALIZED VIEW v1_async_explicit "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true') "
                + "AS SELECT k FROM v1_false"));
        assertV2Required(() -> createMv("CREATE MATERIALIZED VIEW v1_sync "
                + "AS SELECT k AS kk, element_at(v,'obj') AS sub FROM v2_source"));
        assertV2Required(() -> alterTableSync("ALTER TABLE v2_source ADD ROLLUP v1_rollup(k,v)"));
        alterTableSync("ALTER TABLE v2_source ADD COLUMN added VARIANT");
        Assertions.assertTrue(table("v2_source").getVariantEscapeDotsInKeys());
        alterTableSync("ALTER TABLE v2_source DROP COLUMN added");
        alterTableSync("ALTER TABLE v2_source ADD COLUMN ordinary INT DEFAULT '0'");
        alterTableSync("ALTER TABLE v2_source DROP COLUMN ordinary");
        Assertions.assertTrue(table("v2_source").getVariantEscapeDotsInKeys());
        createLike("CREATE TABLE v1_like_false LIKE v1_false");
        createTable("CREATE TABLE v1_ctas_false DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='false') "
                + "AS SELECT k,v FROM v1_false");
        createMvByNereids("CREATE MATERIALIZED VIEW v1_async_false "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1') AS SELECT k,v FROM v1_false");
        for (String name : new String[] {"v1_like_false", "v1_ctas_false", "v1_async_false"}) {
            Assertions.assertFalse(table(name).getVariantEscapeDotsInKeys());
        }
        for (String name : new String[] {"v1_true", "v1_like", "v1_ctas", "v1_async", "v1_async_explicit"}) {
            Assertions.assertNull(Env.getCurrentInternalCatalog().getDbOrMetaException("variant_escape_table")
                    .getTableNullable(name));
        }
    }

    @Test
    public void testSyncMvAndRollupCannotSetTableEncoding() throws Exception {
        createTable("CREATE TABLE sync_source(k INT,padding INT,v VARIANT) DUPLICATE KEY(k,padding) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        Exception error = Assertions.assertThrows(Exception.class, () -> alterTableSync(
                "ALTER TABLE sync_source ADD ROLLUP sync_rollup(k,v) "
                + "PROPERTIES('variant_escape_dots_in_keys'='true')"));
        Assertions.assertTrue(error.getMessage().contains("only be specified when creating"), error.getMessage());
        Exception sync = Assertions.assertThrows(Exception.class, () -> createMv(
                "CREATE MATERIALIZED VIEW sync_explicit PROPERTIES('variant_escape_dots_in_keys'='true') "
                + "AS SELECT k AS kk, element_at(v,'obj') AS sub FROM sync_source"));
        Assertions.assertTrue(sync.getMessage().contains("only be specified when creating"), sync.getMessage());
    }

    private void createLike(String sql) throws Exception {
        CreateTableLikeCommand command = (CreateTableLikeCommand) new NereidsParser().parseSingle(sql);
        command.run(connectContext, new org.apache.doris.qe.StmtExecutor(connectContext, sql));
    }

    private void assertV2Required(org.junit.jupiter.api.function.Executable action) {
        org.apache.doris.nereids.StatementContext previous = connectContext.getStatementContext();
        connectContext.setStatementContext(new org.apache.doris.nereids.StatementContext(connectContext, null));
        try {
            Exception error = Assertions.assertThrows(Exception.class, action);
            Assertions.assertTrue(error.getMessage().contains("requires enable_variant_v2=true"), error.getMessage());
        } finally {
            connectContext.setStatementContext(previous);
        }
    }
}
