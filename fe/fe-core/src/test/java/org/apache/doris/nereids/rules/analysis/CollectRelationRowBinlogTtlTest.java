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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

class CollectRelationRowBinlogTtlTest extends TestWithFeService {
    private boolean originalEnableFeatureBinlog;
    private boolean originalEnableTableStream;

    @Override
    protected void runBeforeAll() throws Exception {
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        originalEnableTableStream = Config.enable_table_stream;
        Config.enable_feature_binlog = true;
        Config.enable_table_stream = true;
        createDatabase("collect_row_ttl");
        connectContext.setDatabase("collect_row_ttl");
        createTable("create table binlog_base (k int) distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1', 'binlog.enable'='true', 'binlog.format'='ROW')");
        createTable("create table plain_base (k int) distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1')");
        createTable("create stream binlog_stream on table binlog_base");
    }

    @Override
    protected void runAfterAll() {
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
        Config.enable_table_stream = originalEnableTableStream;
    }

    @Test
    void testIncrementalIvmRegistersBeforeStreamScansExist() {
        MTMV mv = Mockito.mock(MTMV.class);
        for (IvmRewriteContext rewrite : new IvmRewriteContext[] {
                IvmRewriteContext.incremental(mv),
                IvmRewriteContext.incrementalExplain(mv, true),
                IvmRewriteContext.incrementalDryRun(mv, Optional.empty())}) {
            StatementContext statement = collect("select * from binlog_base", Optional.of(rewrite));
            Assertions.assertTrue(statement.isRowBinlogReferenceTsoRequired());
        }
        Assertions.assertFalse(collect("select * from plain_base", Optional.of(IvmRewriteContext.incremental(mv)))
                .isRowBinlogReferenceTsoRequired());
    }

    @Test
    void testFullIvmSnapshotRegistersButResetDoesNot() {
        MTMV mv = Mockito.mock(MTMV.class);
        Assertions.assertTrue(collect("select * from binlog_base", Optional.of(IvmRewriteContext.full(
                mv, Collections.emptyMap(), StreamReadMode.SNAPSHOT)))
                .isRowBinlogReferenceTsoRequired());
        Assertions.assertFalse(collect("select * from binlog_base", Optional.of(IvmRewriteContext.full(mv)))
                .isRowBinlogReferenceTsoRequired());
        Assertions.assertFalse(collect("select * from binlog_base", Optional.of(IvmRewriteContext.normalize(mv)))
                .isRowBinlogReferenceTsoRequired());
    }

    @Test
    void testCollectorUsesPlannerStatementForDirectBinlogAndStreams() {
        Assertions.assertTrue(collect("select * from binlog_base@incr('incrementType'='DETAIL')", Optional.empty())
                .isRowBinlogReferenceTsoRequired());
        Assertions.assertTrue(collect("select * from binlog_stream", Optional.empty())
                .isRowBinlogReferenceTsoRequired());
        Assertions.assertTrue(collect("select * from binlog_stream@snapshot()", Optional.empty())
                .isRowBinlogReferenceTsoRequired());
        Assertions.assertFalse(collect("select * from binlog_base", Optional.empty())
                .isRowBinlogReferenceTsoRequired());
    }

    private StatementContext collect(String sql, Optional<IvmRewriteContext> rewrite) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        StatementContext plannerStatement = new StatementContext(connectContext, new OriginStatement(sql, 0));
        plannerStatement.setIvmRewriteContext(rewrite);
        StatementContext previousStatement = connectContext.getStatementContext();
        StatementContext connectionStatement = new StatementContext(connectContext, new OriginStatement(sql, 0));
        // The string StmtExecutor entry point can retain a different statement from the parser's context.
        connectContext.setStatementContext(connectionStatement);
        try {
            CascadesContext context = CascadesContext.initContext(plannerStatement, plan, PhysicalProperties.ANY);
            context.newTableCollector(true).collect();
            Assertions.assertFalse(plannerStatement.getOneLevelTables().isEmpty());
            Assertions.assertFalse(connectionStatement.isRowBinlogReferenceTsoRequired());
            Assertions.assertTrue(connectionStatement.getOneLevelTables().isEmpty());
            return plannerStatement;
        } finally {
            connectContext.setStatementContext(previousStatement);
        }
    }
}
