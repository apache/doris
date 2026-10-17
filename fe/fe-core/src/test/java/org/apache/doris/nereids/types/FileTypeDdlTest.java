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

package org.apache.doris.nereids.types;

import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableLikeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.DefaultValue;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

public class FileTypeDdlTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("file_agg_state_ddl");
        connectContext.setDatabase("file_agg_state_ddl");
        createTable("CREATE TABLE ddl_base (id INT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    private ColumnDefinition column(String sql, boolean key, AggregateType agg, Optional<DefaultValue> value) {
        DataType type = Assertions.assertDoesNotThrow(() -> DataType.convertFromString(sql));
        return new ColumnDefinition("f", type, key, agg, true, value, "");
    }

    private void validate(ColumnDefinition column, boolean olap, KeysType keys) {
        column.validate(olap, Collections.emptySet(), Collections.emptySet(), false, keys);
    }

    @Test
    public void testInternalTableRejectsFileValueColumns() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>",
                "ARRAY<STRUCT<f:FILE>>", "STRUCT<f:MAP<STRING,ARRAY<FILE>>>")) {
            for (KeysType keys : Arrays.asList(KeysType.DUP_KEYS, KeysType.UNIQUE_KEYS, KeysType.AGG_KEYS)) {
                AggregateType agg = keys == KeysType.AGG_KEYS ? AggregateType.REPLACE : null;
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> validate(column(sql, false, agg, Optional.empty()), true, keys));
                Assertions.assertEquals("FILE columns are not supported in internal tables", error.getMessage());
            }
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> validate(column("FILE", false, AggregateType.REPLACE_IF_NOT_NULL, Optional.empty()),
                        true, KeysType.AGG_KEYS));
    }

    @Test
    public void testCreateTableAndCtasRejectFileColumns() {
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            for (String ddl : Arrays.asList(
                    "CREATE TABLE ddl_file (id INT, f " + sqlType + ") DUPLICATE KEY(id) "
                            + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')",
                    "CREATE TABLE ddl_ctas DISTRIBUTED BY HASH(id) BUCKETS 1 "
                            + "PROPERTIES('replication_num'='1') AS SELECT 1 AS id, CAST(NULL AS "
                            + sqlType + ") AS f")) {
                Exception error = Assertions.assertThrows(Exception.class, () -> createTable(ddl));
                Assertions.assertTrue(error.getMessage().contains(
                        "FILE columns are not supported in internal tables"), error.getMessage());
            }
        }
    }

    @Test
    public void testCreateTableLikeRejectsExistingFileSchema() throws Exception {
        createTable("CREATE TABLE ddl_legacy (id INT, f STRING) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrAnalysisException("file_agg_state_ddl")
                .getTableOrAnalysisException("ddl_legacy");
        // Simulate a FILE schema restored from metadata created before this DDL restriction.
        table.getColumn("f").setType(Type.FILE);
        String sql = "CREATE TABLE ddl_copy LIKE ddl_legacy";
        CreateTableLikeCommand command = (CreateTableLikeCommand) new NereidsParser().parseSingle(sql);
        Exception error = Assertions.assertThrows(Exception.class,
                () -> command.run(connectContext, new StmtExecutor(connectContext, sql)));
        Assertions.assertTrue(error.getMessage().contains(
                "FILE columns are not supported in internal tables"), error.getMessage());
    }

    @Test
    public void testMaterializedViewRejectsFileColumns() throws Exception {
        createTable("CREATE TABLE ddl_mv_source (id INT, f STRING, extra INT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable source = (OlapTable) Env.getCurrentInternalCatalog().getDbOrAnalysisException("file_agg_state_ddl")
                .getTableOrAnalysisException("ddl_mv_source");
        for (String sqlType : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            // Simulate a pre-existing FILE table; MV creation must not bypass the new schema restriction.
            source.getColumn("f").setType(DataType.convertFromString(sqlType).toCatalogDataType());
            String syncSql = "CREATE MATERIALIZED VIEW ddl_sync_file AS SELECT id, f FROM ddl_mv_source ORDER BY id";
            createStatementCtx(syncSql);
            CreateMaterializedViewCommand sync = (CreateMaterializedViewCommand)
                    new NereidsParser().parseSingle(syncSql);
            sync.validate(connectContext);
            Exception syncError = Assertions.assertThrows(Exception.class,
                    () -> Deencapsulation.invoke(new MaterializedViewHandler(), "checkAndPrepareMaterializedView",
                            sync, source, Collections.emptyMap()));
            Assertions.assertTrue(syncError.getMessage().contains(
                    "FILE columns are not supported in internal tables"), syncError.getMessage());

            String asyncSql = "CREATE MATERIALIZED VIEW ddl_async_file BUILD DEFERRED REFRESH COMPLETE ON MANUAL "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1') "
                    + "AS SELECT id, f FROM ddl_mv_source";
            createStatementCtx(asyncSql);
            CreateMTMVCommand async = (CreateMTMVCommand) new NereidsParser().parseSingle(asyncSql);
            Exception asyncError = Assertions.assertThrows(Exception.class,
                    () -> async.getCreateMTMVInfo().analyze(connectContext));
            Assertions.assertTrue(asyncError.getMessage().contains(
                    "FILE columns are not supported in internal tables"), asyncError.getMessage());
        }
    }

    @Test
    public void testRejectKeyExternalAndNonReplaceAggregation() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            ColumnDefinition key = column(sql, true, null, Optional.empty());
            Assertions.assertThrows(Exception.class, () -> validate(key, true, KeysType.DUP_KEYS));
            ColumnDefinition external = column(sql, false, null, Optional.empty());
            Assertions.assertThrows(Exception.class, () -> validate(external, false, KeysType.DUP_KEYS));
        }
        ColumnDefinition sum = column("FILE", false, AggregateType.SUM, Optional.empty());
        Assertions.assertThrows(Exception.class, () -> validate(sum, true, KeysType.AGG_KEYS));
    }

    @Test
    public void testKeyEligibilityPreservesNestedFileRestrictions() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Assertions.assertFalse(ColumnDefinition.isEligibleKeyType(DataType.convertFromString(sql)));
        }
        Assertions.assertTrue(ColumnDefinition.isEligibleKeyType(new AggStateType("sum",
                Collections.singletonList(IntegerType.INSTANCE), Collections.singletonList(true), true)));
        Assertions.assertTrue(ColumnDefinition.isEligibleKeyType(IntegerType.INSTANCE));
    }

    @Test
    public void testExternalTableRejectsFileAtNestedLeaf() {
        for (String sql : Arrays.asList("ARRAY<STRUCT<f:FILE>>", "STRUCT<f:MAP<STRING,ARRAY<FILE>>>")) {
            Exception error = Assertions.assertThrows(Exception.class,
                    () -> validate(column(sql, false, null, Optional.empty()), false, KeysType.DUP_KEYS));
            Assertions.assertEquals("FILE columns are not supported in external tables", error.getMessage());
        }
    }

    @Test
    public void testDefaultAndMapKeyRestrictions() {
        ColumnDefinition nullable = column("FILE", false, null, Optional.of(DefaultValue.NULL_DEFAULT_VALUE));
        Assertions.assertThrows(AnalysisException.class, () -> validate(nullable, true, KeysType.DUP_KEYS));
        ColumnDefinition nonNullDefault = column("FILE", false, null, Optional.of(new DefaultValue("{}")));
        Assertions.assertThrows(Exception.class, () -> validate(nonNullDefault, true, KeysType.DUP_KEYS));
        for (String sql : Arrays.asList("MAP<FILE,INT>", "MAP<STRUCT<f:FILE>,INT>")) {
            DataType type = Assertions.assertDoesNotThrow(() -> DataType.convertFromString(sql));
            AnalysisException error = Assertions.assertThrows(AnalysisException.class, type::validateDataType);
            Assertions.assertTrue(error.getMessage().startsWith("MAP key type must be a primitive type"),
                    error.getMessage());
        }
    }

    @Test
    public void testFileAggStateTypesAndAlterColumnValidation() {
        TableNameInfo tableName = new TableNameInfo("file_agg_state_ddl", "ddl_base");
        for (String leaf : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            String sql = "AGG_STATE<any_value(" + leaf + ")>";
            DataType state = DataType.convertFromString(sql);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class, state::validateDataType);
            Assertions.assertTrue(error.getMessage().contains("AGG_STATE does not support FILE"), error.getMessage());
            ColumnDefinition added = column(sql, false, null, Optional.empty());
            AnalysisException alterError = Assertions.assertThrows(AnalysisException.class,
                    () -> AddColumnOp.validateColumnDef(tableName, added, null, null));
            Assertions.assertTrue(alterError.getMessage().contains("AGG_STATE does not support FILE"),
                    alterError.getMessage());
        }
        Assertions.assertDoesNotThrow(() -> DataType.convertFromString("AGG_STATE<sum(INT)>").validateDataType());
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> AddColumnOp.validateColumnDef(tableName, column(sql, false, null, Optional.empty()),
                            null, null));
            Assertions.assertEquals("FILE columns are not supported in internal tables", error.getMessage());
        }
    }

    @Test
    public void testNullCastRejectsFileAggStateTargets() {
        for (String leaf : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            String sql = "AGG_STATE<any_value(" + leaf + ")>";
            DataType state = DataType.convertFromString(sql);
            for (boolean strict : Arrays.asList(false, true)) {
                Assertions.assertFalse(CheckCast.check(NullType.INSTANCE, state, strict));
            }
            Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze("select cast(null as " + sql + ")"));
        }
        Assertions.assertTrue(CheckCast.check(NullType.INSTANCE,
                DataType.convertFromString("AGG_STATE<sum(INT)>"), true));
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext)
                .analyze("select cast(null as AGG_STATE<sum(INT)>)"));
    }

    @Test
    public void testSchemaChangePreservesFileIdentityAndAllowsNullableWidening() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            org.apache.doris.catalog.Type type = DataType.convertFromString(sql).toCatalogDataType();
            Column required = new Column("f", type, false);
            Column nullable = new Column("f", type, true);
            Assertions.assertDoesNotThrow(() -> required.checkSchemaChangeAllowed(nullable), sql);
            Assertions.assertThrows(Exception.class, () -> nullable.checkSchemaChangeAllowed(required), sql);
            Column string = new Column("f", org.apache.doris.catalog.Type.STRING, true);
            Assertions.assertThrows(Exception.class, () -> nullable.checkSchemaChangeAllowed(string), sql);
            Assertions.assertThrows(Exception.class, () -> string.checkSchemaChangeAllowed(nullable), sql);
        }
        Column file = new Column("f", org.apache.doris.catalog.Type.FILE, true);
        for (String sql : Arrays.asList("JSON", "STRUCT<uri:STRING>")) {
            Column other = new Column("f", DataType.convertFromString(sql).toCatalogDataType(), true);
            Assertions.assertThrows(Exception.class, () -> file.checkSchemaChangeAllowed(other));
            Assertions.assertThrows(Exception.class, () -> other.checkSchemaChangeAllowed(file));
        }
        for (String sql : Arrays.asList("ARRAY<STRING>", "STRUCT<f:STRING>", "MAP<STRING,STRING>")) {
            String fileSql = sql.replace("ARRAY<STRING>", "ARRAY<FILE>")
                    .replace("STRUCT<f:STRING>", "STRUCT<f:FILE>").replace("MAP<STRING,STRING>", "MAP<STRING,FILE>");
            Column nestedFile = new Column("f", DataType.convertFromString(fileSql).toCatalogDataType(), true);
            Column nestedString = new Column("f", DataType.convertFromString(sql).toCatalogDataType(), true);
            Assertions.assertThrows(Exception.class, () -> nestedFile.checkSchemaChangeAllowed(nestedString));
            Assertions.assertThrows(Exception.class, () -> nestedString.checkSchemaChangeAllowed(nestedFile));
        }
    }

    @Test
    public void testPartitionRejectsFileThroughComplexTypeRule() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>",
                "ARRAY<STRUCT<f:FILE>>", "ARRAY<INT>", "STRUCT<f:INT>", "MAP<STRING,INT>")) {
            ColumnDefinition column = column(sql, true, null, Optional.empty());
            Assertions.assertTrue(column.getType().isComplexType(), sql);
            Assertions.assertTrue(column.getType().toCatalogDataType().isComplexType(), sql);
            for (boolean external : Arrays.asList(false, true)) {
                PartitionTableInfo partition = new PartitionTableInfo(false, PartitionType.RANGE.name(),
                        null, Collections.singletonList(new UnboundSlot("f")));
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> partition.validatePartitionInfo(Collections.singletonMap("f", column),
                                Collections.emptyMap(), connectContext, false, external));
                Assertions.assertTrue(error.getMessage().startsWith("Complex type column can't be partition column"),
                        error.getMessage());
            }
        }
    }

    @Test
    public void testRejectDistributionClusterAndIndexes() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            ColumnDefinition column = column(sql, false, null, Optional.empty());
            Assertions.assertThrows(Exception.class, () -> column.validate(true, Collections.emptySet(),
                    Collections.singleton("f"), false, KeysType.DUP_KEYS));
            DistributionDescriptor distribution = new DistributionDescriptor(true, false, 1,
                    Collections.singletonList("f"));
            Assertions.assertThrows(Exception.class,
                    () -> distribution.validate(Collections.singletonMap("f", column), KeysType.DUP_KEYS));
            Assertions.assertThrows(Exception.class,
                    () -> HashDistributionInfo.checkDistributionColumnType("f", column.getType().toCatalogDataType()));
            Assertions.assertFalse(IndexDefinition.isSupportIdxType(column.getType()), sql);
            Assertions.assertFalse(new Column("f", column.getType().toCatalogDataType()).isSupportBloomFilter(), sql);
            for (String indexType : Arrays.asList("INVERTED", "BLOOMFILTER", "NGRAM_BF")) {
                IndexDefinition index = new IndexDefinition("idx", false, Collections.singletonList("f"),
                        indexType, new HashMap<>(), "");
                Assertions.assertThrows(Exception.class, () -> index.checkColumn(column, KeysType.DUP_KEYS,
                        false, TInvertedIndexFileStorageFormat.V2));
                Assertions.assertThrows(Exception.class, () -> index.checkColumn(
                        new Column("f", column.getType().toCatalogDataType()), KeysType.DUP_KEYS,
                        false, TInvertedIndexFileStorageFormat.V2));
            }
        }
    }
}
