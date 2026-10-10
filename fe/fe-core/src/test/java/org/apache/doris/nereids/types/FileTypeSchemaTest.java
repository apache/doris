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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.MTMVAnalyzeQueryInfo;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FileTypeSchemaTest extends TestWithFeService {
    private static final String DB = "file_type_schema";
    private static final String PAYLOAD = "f FILE, files ARRAY<FILE>, s STRUCT<f:FILE>, m MAP<STRING,FILE>";
    private boolean savedTableStream;

    @Override
    protected void runBeforeAll() throws Exception {
        savedTableStream = Config.enable_table_stream;
        Config.enable_table_stream = true;
        createDatabase(DB);
        connectContext.setDatabase(DB);
        createTable("CREATE TABLE file_base (id INT NOT NULL, " + PAYLOAD + ", extra INT) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE file_mow (id INT NOT NULL, " + PAYLOAD + ") "
                + "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'binlog.enable'='true', "
                + "'binlog.format'='ROW', 'binlog.need_historical_value'='true')");
    }

    @Override
    protected void runAfterAll() {
        Config.enable_table_stream = savedTableStream;
    }

    private LogicalPlan parse(String sql) {
        connectContext.setStatementContext(new StatementContext(connectContext, new OriginStatement(sql, 0)));
        return new NereidsParser().parseSingle(sql);
    }

    @Override
    public void createTable(String sql) throws Exception {
        ((CreateTableCommand) parse(sql)).run(connectContext, null);
    }

    private OlapTable table(String name) throws Exception {
        return (OlapTable) Env.getCurrentInternalCatalog().getDbOrDdlException(DB).getTableOrMetaException(name);
    }

    @Test
    public void testGeneratedGettersAndScalarKeys() throws Exception {
        createTable("CREATE TABLE file_generated ("
                + "bytes BIGINT GENERATED ALWAYS AS (ELEMENT_AT(f, 'size')), id INT NOT NULL, f FILE, "
                + "uri VARCHAR(65533) GENERATED ALWAYS AS (ELEMENT_AT(f, 'uri')), "
                + "off BIGINT GENERATED ALWAYS AS (ELEMENT_AT(f, 'offset')), "
                + "mime VARCHAR(1024) GENERATED ALWAYS AS (ELEMENT_AT(f, 'content_type')), "
                + "checksum VARCHAR(1024) GENERATED ALWAYS AS (ELEMENT_AT(f, 'checksum')), "
                + "INDEX bytes_idx(bytes) USING INVERTED) DUPLICATE KEY(bytes,id) "
                + "PARTITION BY RANGE(bytes) (PARTITION p1 VALUES LESS THAN ('100')) "
                + "DISTRIBUTED BY HASH(bytes) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable generated = table("file_generated");
        for (String name : Arrays.asList("bytes", "uri", "off", "mime", "checksum")) {
            Column column = generated.getColumn(name);
            Assertions.assertFalse(column.getType().typeContainsFile());
            Expr expression = column.getGeneratedColumnInfo().getExpr();
            List<FunctionCallExpr> functions = new ArrayList<>();
            expression.collect(FunctionCallExpr.class, functions);
            Assertions.assertTrue(functions.stream().anyMatch(fn -> fn.getFnName().getFunction().equals("element_at")),
                    name + " must lower to FILE field access");
        }
        Assertions.assertTrue(generated.getColumn("bytes").isKey());
        Assertions.assertEquals(Type.BIGINT, generated.getColumn("bytes").getType());
        Assertions.assertEquals(new HashSet<>(Arrays.asList("bytes", "uri", "off", "mime", "checksum")),
                generated.getColumn("f").getGeneratedColumnsThatReferToThis());
    }

    private List<Column> syncColumns(String query) throws Exception {
        CreateMaterializedViewCommand command = (CreateMaterializedViewCommand)
                parse("CREATE MATERIALIZED VIEW file_sync AS " + query);
        command.validate(connectContext);
        return Deencapsulation.invoke(new MaterializedViewHandler(), "checkAndPrepareMaterializedView",
                command, table("file_base"), Collections.emptyMap());
    }

    private void assertFilePayload(List<Column> columns) {
        for (String name : Arrays.asList("f", "files", "s", "m")) {
            Column column = columns.stream().filter(c -> c.getName().equals(name)).findFirst().orElseThrow();
            Assertions.assertTrue(column.getType().typeContainsFile(), name);
            Assertions.assertFalse(column.isKey(), name);
        }
        Assertions.assertTrue(columns.stream().filter(Column::isKey)
                .noneMatch(c -> c.getType().typeContainsFile()));
    }

    @Test
    public void testSyncMaterializedViewPayload() throws Exception {
        assertFilePayload(syncColumns("SELECT id, f, files, s, m FROM file_base ORDER BY id"));
        assertFilePayload(syncColumns("SELECT id, f, files, s, m FROM file_base"));
        List<Column> fields = syncColumns("SELECT ELEMENT_AT(f, 'size') AS bytes, id FROM file_base ORDER BY bytes, id");
        Assertions.assertTrue(fields.get(0).isKey());
        Assertions.assertEquals(Type.BIGINT, fields.get(0).getType());
        for (String value : Arrays.asList("f", "files", "s", "m")) {
            Assertions.assertThrows(Exception.class,
                    () -> syncColumns("SELECT id, " + value + " FROM file_base ORDER BY id, " + value));
            Assertions.assertThrows(Exception.class,
                    () -> syncColumns("SELECT " + value + ", COUNT(*) FROM file_base GROUP BY " + value));
        }
    }

    private CreateMTMVInfo analyzeMv(String name, String refresh, String clauses, String query) throws Exception {
        CreateMTMVCommand command = (CreateMTMVCommand) parse("CREATE MATERIALIZED VIEW " + name
                + " BUILD DEFERRED REFRESH " + refresh + " ON MANUAL " + clauses
                + " PROPERTIES('replication_num'='1') AS " + query);
        command.getCreateMTMVInfo().analyze(connectContext);
        return command.getCreateMTMVInfo();
    }

    @Test
    public void testAsyncMaterializedViewPayload() throws Exception {
        CreateMTMVInfo info = analyzeMv("file_async", "COMPLETE", "DISTRIBUTED BY HASH(id) BUCKETS 1",
                "SELECT id, f, files, s, m, ELEMENT_AT(f, 'size') AS bytes FROM file_base");
        assertFilePayload(info.getColumns());
        Assertions.assertEquals(Collections.singletonList("id"), info.getColumns().stream().filter(Column::isKey)
                .map(Column::getName).collect(Collectors.toList()));
        for (String refresh : Arrays.asList("COMPLETE", "INCREMENTAL")) {
            for (String value : Arrays.asList("f", "files", "s", "m")) {
                Assertions.assertThrows(Exception.class, () -> analyzeMv("file_bad", refresh,
                        "KEY(id," + value + ") DISTRIBUTED BY RANDOM BUCKETS 1",
                        "SELECT id, " + value + " FROM file_mow"));
                Assertions.assertThrows(Exception.class, () -> analyzeMv("file_bad", refresh,
                        "DISTRIBUTED BY HASH(" + value + ") BUCKETS 1",
                        "SELECT id, " + value + " FROM file_mow"));
            }
        }
    }

    @Test
    public void testIvmPreservesFilePayloadAndScalarIdentity() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW file_ivm BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES('replication_num'='1') "
                + "AS SELECT id, f, files, s, m, ELEMENT_AT(f, 'size') AS bytes FROM file_mow";
        ((CreateMTMVCommand) parse(sql)).run(connectContext, null);
        MTMV mv = (MTMV) table("file_ivm");
        Assertions.assertTrue(mv.isIvm());
        assertFilePayload(mv.getBaseSchema());
        parse(mv.getQuerySql());
        MTMVAnalyzeQueryInfo analyzed = MTMVPlanUtil.analyzeQueryWithSql(mv, connectContext,
                Optional.of(IvmRewriteContext.normalize(mv)));
        Plan normalized = analyzed.getIvmNormalizedPlan();
        Assertions.assertNotNull(normalized);
        for (String name : Arrays.asList("f", "files", "s", "m")) {
            Assertions.assertEquals(table("file_mow").getColumn(name).getType(), normalized.getOutput().stream()
                    .filter(slot -> slot.getName().equals(name)).findFirst().orElseThrow()
                    .getDataType().toCatalogDataType());
        }
        Set<Expression> identities = new HashSet<>();
        normalized.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                expression.foreach(expr -> {
                    if (expr instanceof Alias && ((Alias) expr).getName().equals(Column.IVM_ROW_ID_COL)) {
                        identities.add(((Alias) expr).child());
                    }
                });
            }
        });
        Assertions.assertFalse(identities.isEmpty());
        for (Expression identity : identities) {
            Assertions.assertTrue(identity.getInputSlots().stream().noneMatch(slot ->
                    slot.getDataType().toCatalogDataType().typeContainsFile()));
        }
    }

    @Test
    public void testGeneratedGetterAsClusterKey() throws Exception {
        createTable("CREATE TABLE file_generated_cluster (id INT NOT NULL, f FILE, "
                + "bytes BIGINT GENERATED ALWAYS AS (ELEMENT_AT(f, 'size'))) UNIQUE KEY(id) ORDER BY(bytes) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true')");
        Assertions.assertTrue(table("file_generated_cluster").getColumn("bytes").isClusterKey());
        Assertions.assertFalse(table("file_generated_cluster").getColumn("f").isClusterKey());
    }

    @Test
    public void testAtomicFileSchemaChanges() throws Exception {
        createTable("CREATE TABLE file_sc (id INT, f FILE NOT NULL, extra INT) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','light_schema_change'='true')");
        alterTableSync("ALTER TABLE file_sc ADD COLUMN added FILE NULL DEFAULT NULL");
        Assertions.assertEquals(Type.FILE, table("file_sc").getColumn("added").getType());
        alterTableSync("ALTER TABLE file_sc RENAME COLUMN added TO renamed");
        Assertions.assertEquals(Type.FILE, table("file_sc").getColumn("renamed").getType());
        Assertions.assertNull(table("file_sc").getColumn("added"));
        alterTableSync("ALTER TABLE file_sc DROP COLUMN renamed");
        Assertions.assertNull(table("file_sc").getColumn("renamed"));
        for (String target : Arrays.asList("STRING", "JSON", "STRUCT<uri:STRING>")) {
            Assertions.assertThrows(Exception.class,
                    () -> alterTableSync("ALTER TABLE file_sc MODIFY COLUMN f " + target + " NULL"));
            Assertions.assertEquals(Type.FILE, table("file_sc").getColumn("f").getType());
        }
        alterTableSync("ALTER TABLE file_sc MODIFY COLUMN f FILE NULL");
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            for (AlterJobV2 job : Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values()) {
                while (!job.getJobState().isFinalState()) {
                    Thread.sleep(100);
                }
                Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
            }
            while (table("file_sc").getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(100);
            }
        });
        Assertions.assertTrue(table("file_sc").getColumn("f").isAllowNull());
        Assertions.assertThrows(Exception.class,
                () -> alterTableSync("ALTER TABLE file_sc MODIFY COLUMN f FILE NOT NULL"));
    }
}
