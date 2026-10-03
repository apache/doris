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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

class BatchPointQueryRewriteTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("batch_point_query_test");
        useDatabase("batch_point_query_test");
        createTable("CREATE TABLE address_rows (address VARCHAR(128) NOT NULL, out_edges_json STRING, "
                + "in_edges_json STRING, degree BIGINT) UNIQUE KEY(address) "
                + "DISTRIBUTED BY HASH(address) BUCKETS 128 PROPERTIES(\"replication_num\"=\"1\","
                + "\"enable_unique_key_merge_on_write\"=\"true\",\"store_row_column\"=\"true\")");
        createTable("CREATE TABLE integer_rows (id INT NOT NULL, value STRING) UNIQUE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES(\"replication_num\"=\"1\","
                + "\"enable_unique_key_merge_on_write\"=\"true\",\"store_row_column\"=\"true\")");
    }

    private boolean rewrite(String sql, boolean enabled, MysqlCommand command) throws Exception {
        VariableMgr.setVar(connectContext.getSessionVariable(),
                new SetVar(SessionVariable.ENABLE_BATCH_POINT_QUERY, new StringLiteral(Boolean.toString(enabled))));
        connectContext.setCommand(command);
        return PlanChecker.from(connectContext).analyze(sql).rewrite().getCascadesContext()
                .getStatementContext().isShortCircuitQuery();
    }

    @Test
    void testLiteralInOnHashKey() throws Exception {
        Assertions.assertTrue(rewrite("SELECT address,out_edges_json,in_edges_json,degree "
                + "FROM address_rows WHERE address IN ('A','B','C')", true, MysqlCommand.COM_QUERY));
        Assertions.assertFalse(rewrite("SELECT * FROM address_rows WHERE address IN ('A','B')",
                false, MysqlCommand.COM_QUERY));
    }

    @Test
    void testHundredKeyBoundary() throws Exception {
        String values = IntStream.range(0, 100).mapToObj(i -> "'address_" + i + "'")
                .collect(Collectors.joining(","));
        Assertions.assertTrue(rewrite("SELECT * FROM address_rows WHERE address IN (" + values + ")",
                true, MysqlCommand.COM_QUERY));
        Assertions.assertFalse(rewrite("SELECT * FROM address_rows WHERE address IN (" + values + ",'one_more')",
                true, MysqlCommand.COM_QUERY));
    }

    @Test
    void testUnsupportedShapesKeepNormalScan() throws Exception {
        Assertions.assertFalse(rewrite("SELECT * FROM address_rows WHERE address IN ('A','B') AND degree>1",
                true, MysqlCommand.COM_QUERY));
        Assertions.assertFalse(rewrite("SELECT lower(address) FROM address_rows WHERE address IN ('A','B')",
                true, MysqlCommand.COM_QUERY));
        Assertions.assertFalse(rewrite("SELECT * FROM address_rows WHERE address IN ('A','B')",
                true, MysqlCommand.COM_STMT_EXECUTE));
        Assertions.assertFalse(rewrite("SELECT * FROM integer_rows WHERE id IN (1,2)",
                true, MysqlCommand.COM_QUERY));
    }

    @Test
    void testEqualityStillUsesOriginalPointQuery() throws Exception {
        Assertions.assertTrue(rewrite("SELECT * FROM address_rows WHERE address='A'",
                false, MysqlCommand.COM_QUERY));
    }

    @Test
    void testSamplingAndSkippedVersionsKeepNormalScan() throws Exception {
        Assertions.assertFalse(rewrite("SELECT * FROM address_rows TABLESAMPLE(50 PERCENT) "
                + "WHERE address IN ('A','B')", true, MysqlCommand.COM_QUERY));
        connectContext.getSessionVariable().skipMissingVersion = true;
        try {
            Assertions.assertFalse(rewrite("SELECT * FROM address_rows WHERE address IN ('A','B')",
                    true, MysqlCommand.COM_QUERY));
        } finally {
            connectContext.getSessionVariable().skipMissingVersion = false;
        }
    }
}
