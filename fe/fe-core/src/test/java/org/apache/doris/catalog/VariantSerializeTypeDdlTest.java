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

import org.apache.doris.common.Config;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.thrift.TVariantSerializeType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The serialize type is internal: DDL creates V2 variant columns and cannot state it, and only metadata older than
 * the field is V1. MODIFY COLUMN keeps what a column is stored with; a created column does not inherit it.
 */
public class VariantSerializeTypeDdlTest extends TestWithFeService {
    private static final String COUNT_ZERO = "VARIANT<PROPERTIES(\"variant_max_subcolumns_count\" = \"0\")>";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("variant_serialize_type");
        connectContext.setDatabase("variant_serialize_type");
    }

    // DDL cannot create a V1 column, so the metadata is rewritten to what an old image reads as.
    private OlapTable createTableWithOldColumn(String name) throws Exception {
        createTable("CREATE TABLE variant_serialize_type." + name + " (k INT, v " + COUNT_ZERO + ") "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("variant_serialize_type").getTableOrMetaException(name);
        Column column = table.getColumn("v");
        Assertions.assertEquals(TVariantSerializeType.V2, column.getVariantSerializeType());
        column.setType(((VariantType) column.getType()).withSerializeType(TVariantSerializeType.V1));
        return table;
    }

    @Test
    public void testPrintedDdlReplaysOldColumnAsV2() throws Exception {
        OlapTable table = createTableWithOldColumn("t_replay");
        List<String> createTableStmt = Lists.newArrayList();
        Env.getDdlStmt(table, createTableStmt, null, null, false, true, -1L);
        // the DDL shows the stored serialize type in a comment, which the parser skips
        Assertions.assertTrue(createTableStmt.get(0).contains("> /* variant_serialize_type = v1 */ NULL"),
                createTableStmt.get(0));
        List<ColumnDefinition> columns = ((CreateTableCommand) new NereidsParser().parseSingle(createTableStmt.get(0)))
                .getCreateTableInfo().getColumnDefinitions();
        Assertions.assertEquals(TVariantSerializeType.V2,
                ((org.apache.doris.nereids.types.VariantType) columns.get(1).getType()).getVariantSerializeType());
    }

    @Test
    public void testPrintedDdlShowsSerializeTypeOfNewColumn() throws Exception {
        createTable("CREATE TABLE variant_serialize_type.t_print_v2 (k INT, v " + COUNT_ZERO + ") "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("variant_serialize_type").getTableOrMetaException("t_print_v2");
        List<String> createTableStmt = Lists.newArrayList();
        Env.getDdlStmt(table, createTableStmt, null, null, false, true, -1L);
        Assertions.assertTrue(createTableStmt.get(0).contains("> /* variant_serialize_type = v2 */ NULL"),
                createTableStmt.get(0));
    }

    @Test
    public void testModifyColumnKeepsStoredSerializeType() throws Exception {
        OlapTable table = createTableWithOldColumn("t_modify");
        // the restated definition parses as V2
        alterTableSync("ALTER TABLE t_modify MODIFY COLUMN v " + COUNT_ZERO + " COMMENT 'kept'");
        Assertions.assertEquals("kept", table.getColumn("v").getComment());
        Assertions.assertEquals(TVariantSerializeType.V1, table.getColumn("v").getVariantSerializeType());
    }

    @Test
    public void testModifyColumnKeepsTypedPathsToSparse() throws Exception {
        // variant_enable_typed_paths_to_sparse = true comes only from the internal session variable, which the
        // parser reads with debug points enabled, and a restated definition cannot state it
        boolean originalEnableDebugPoints = Config.enable_debug_points;
        try {
            Config.enable_debug_points = true;
            connectContext.getSessionVariable().defaultEnableTypedPathsToSparse = true;
            createTable("CREATE TABLE variant_serialize_type.t_typed_sparse (k INT, v VARIANT) "
                    + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        } finally {
            Config.enable_debug_points = originalEnableDebugPoints;
            connectContext.getSessionVariable().defaultEnableTypedPathsToSparse = false;
        }
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("variant_serialize_type").getTableOrMetaException("t_typed_sparse");
        Assertions.assertTrue(table.getColumn("v").getVariantEnableTypedPathsToSparse());
        alterTableSync("ALTER TABLE t_typed_sparse MODIFY COLUMN v VARIANT COMMENT 'kept'");
        Assertions.assertEquals("kept", table.getColumn("v").getComment());
        Assertions.assertTrue(table.getColumn("v").getVariantEnableTypedPathsToSparse());
    }

    // The variant column that CTAS creates from the query.
    private org.apache.doris.nereids.types.VariantType ctasVariantType(String sql) {
        CreateTableCommand command = (CreateTableCommand) new NereidsParser().parseSingle(sql);
        connectContext.setStatementContext(new StatementContext());
        command.validateCreateTableAsSelect(connectContext, command.getCtasQuery().get());
        List<ColumnDefinition> columns = command.getCreateTableInfo().getColumnDefinitions();
        return (org.apache.doris.nereids.types.VariantType) columns.get(1).getType();
    }

    @Test
    public void testCtasOfOldColumnCreatesV2() throws Exception {
        createTableWithOldColumn("t_ctas_source");
        org.apache.doris.nereids.types.VariantType type = ctasVariantType(
                "CREATE TABLE t_ctas DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1') "
                        + "AS SELECT * FROM t_ctas_source");
        Assertions.assertEquals(TVariantSerializeType.V2, type.getVariantSerializeType());
        // the new column keeps the count, so its 0 extracts no dynamic path
        Assertions.assertEquals(0, type.getVariantMaxSubcolumnsCount());
    }

    @Test
    public void testCtasOfVariantExpressionKeepsCountZero() {
        // the type of a VARIANT expression has a count of 0 under V1, and the new column is V2 with that count
        org.apache.doris.nereids.types.VariantType type = ctasVariantType(
                "CREATE TABLE t_ctas_expr DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1') "
                        + "AS SELECT 1 AS k, parse_to_variant('{\"a\": 1}') AS v");
        Assertions.assertEquals(TVariantSerializeType.V2, type.getVariantSerializeType());
        Assertions.assertEquals(0, type.getVariantMaxSubcolumnsCount());
    }

    @Test
    public void testCtasOfNewColumnKeepsCountZero() throws Exception {
        createTable("CREATE TABLE variant_serialize_type.t_ctas_new_source (k INT, v " + COUNT_ZERO + ") "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        // a count of 0 that a new column states means no dynamic path, and the copy keeps it
        org.apache.doris.nereids.types.VariantType type = ctasVariantType(
                "CREATE TABLE t_ctas_new DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1') "
                        + "AS SELECT * FROM t_ctas_new_source");
        Assertions.assertEquals(TVariantSerializeType.V2, type.getVariantSerializeType());
        Assertions.assertEquals(0, type.getVariantMaxSubcolumnsCount());
    }

    private MTMV createMvOfOldColumn(String name) throws Exception {
        createTableWithOldColumn(name + "_source");
        createMvByNereids("CREATE MATERIALIZED VIEW " + name + " BUILD DEFERRED REFRESH COMPLETE ON MANUAL "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1') "
                + "AS SELECT k, v, v['a'] AS va, parse_to_variant(CAST(v AS STRING)) AS pv FROM " + name + "_source");
        return (MTMV) Env.getCurrentInternalCatalog().getDbOrMetaException("variant_serialize_type")
                .getTableOrMetaException(name);
    }

    @Test
    public void testMvStoresComputedVariantAsV2() throws Exception {
        MTMV mv = createMvOfOldColumn("mv_columns");
        // a VARIANT column of a table keeps its type
        Assertions.assertEquals(TVariantSerializeType.V1, mv.getColumn("v").getVariantSerializeType());
        // a VARIANT the query computes is a new column, as in CTAS, and its count of 0 extracts no dynamic path
        for (String name : new String[] {"va", "pv"}) {
            Assertions.assertEquals(TVariantSerializeType.V2, mv.getColumn(name).getVariantSerializeType());
            Assertions.assertEquals(0, ((VariantType) mv.getColumn(name).getType()).getVariantMaxSubcolumnsCount());
        }
    }

    @Test
    public void testRefreshAcceptsMvThatStoresComputedVariantAsV1() throws Exception {
        MTMV mv = createMvOfOldColumn("mv_refresh");
        // an older version stored the VARIANT the query computes as V1, and this version analyses it as V2
        for (String name : new String[] {"va", "pv"}) {
            Column column = mv.getColumn(name);
            column.setType(((VariantType) column.getType()).withSerializeType(TVariantSerializeType.V1));
        }
        try {
            Assertions.assertDoesNotThrow(() -> MTMVPlanUtil.ensureMTMVQueryUsable(mv,
                    MTMVPlanUtil.createMTMVContext(mv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE)));
        } finally {
            // the context of the MV replaced the one of this thread, which the other tests analyse with
            connectContext.setThreadLocalInfo();
        }
    }
}
