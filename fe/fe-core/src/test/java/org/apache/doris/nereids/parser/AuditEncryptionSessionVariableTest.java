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

import java.util.TreeMap;

public class AuditEncryptionSessionVariableTest {
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
