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

package org.apache.doris.nereids.parser;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.TreeMap;

public class AuditEncryptionSessionVariableTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInsertMaskingPreservesThreadAndSession(boolean withContext) {
        ConnectContext.remove();
        ConnectContext ctx = new ConnectContext();
        ctx.setDatabase("unchanged_database");
        StatementContext statement = new StatementContext(ctx, null);
        ctx.setStatementContext(statement);
        SessionVariable session = ctx.getSessionVariable();
        session.setQueryTimeoutS(1800);
        session.setInsertTimeoutS(14400);
        if (withContext) {
            ctx.setThreadLocalInfo();
        }
        try {
            String hint = "/*+ SET_VAR(query_timeout=1, insert_timeout=1) */";
            String source = "S3('uri'='s3://bucket/data.parquet', 'format'='parquet',"
                    + " 's3.secret_key'='test-secret')";
            String[] statements = {
                    "INSERT INTO t SELECT " + hint + " 1",
                    "INSERT INTO t SELECT " + hint + " * FROM " + source,
                    "WITH src AS (SELECT " + hint + " * FROM " + source + ") INSERT INTO t SELECT * FROM src",
                    "INSERT INTO S3('file_path'='s3://bucket/out_', 'format'='csv',"
                            + " 's3.secret_key'='target-secret') SELECT " + hint + " * FROM " + source
            };
            for (String sql : statements) {
                TreeMap<Pair<Integer, Integer>, String> replacements = new TreeMap<>(new Pair.PairComparator<>());
                new NereidsParser().parseForEncryption(sql, replacements);
                String masked = BaseViewInfo.rewriteSql(replacements, sql);
                Assertions.assertTrue(masked.contains(hint));
                Assertions.assertFalse(masked.contains("test-secret"));
                Assertions.assertFalse(masked.contains("target-secret"));
                Assertions.assertEquals(1800, session.getQueryTimeoutS());
                Assertions.assertEquals(14400, session.getInsertTimeoutS());
                Assertions.assertFalse(session.getIsSingleSetVar());
                Assertions.assertTrue(session.getSessionOriginValue().isEmpty());
                Assertions.assertEquals("unchanged_database", ctx.getDatabase());
                Assertions.assertSame(statement, ctx.getStatementContext());
                Assertions.assertSame(withContext ? ctx : null, ConnectContext.get());
            }
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testAuditParsingDoesNotApplySetVar() {
        ConnectContext ctx = new ConnectContext();
        ctx.setDatabase("test");
        ctx.setStatementContext(new StatementContext(ctx, null));
        ctx.setThreadLocalInfo();
        try {
            SessionVariable session = ctx.getSessionVariable();
            session.setQueryTimeoutS(1800);
            session.setInsertTimeoutS(14400);
            String hint = "/*+ SET_VAR(query_timeout=1, insert_timeout=1) */";
            String[] sources = {
                    "numbers(\"number\"=\"1\")",
                    "S3('uri'='s3://bucket/data.parquet', 'format'='parquet', 's3.secret_key'='test-secret')"
            };
            for (String source : sources) {
                String sql = "INSERT INTO t SELECT " + hint + " * FROM " + source;
                TreeMap<Pair<Integer, Integer>, String> replacements = new TreeMap<>(new Pair.PairComparator<>());
                new NereidsParser().parseForEncryption(sql, replacements);
                Assertions.assertEquals(1800, session.getQueryTimeoutS());
                Assertions.assertEquals(14400, session.getInsertTimeoutS());
                Assertions.assertFalse(session.getIsSingleSetVar());
                Assertions.assertTrue(session.getSessionOriginValue().isEmpty());
                String masked = BaseViewInfo.rewriteSql(replacements, sql);
                Assertions.assertTrue(masked.contains(hint));
                Assertions.assertFalse(masked.contains("test-secret"));
            }
        } finally {
            ConnectContext.remove();
        }
    }
}
