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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.SimpleHttpServer;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SQLDialectUtilsTest {

    int port;
    SimpleHttpServer server;

    @Before
    public void setUp() throws Exception {
        port = TestWithFeService.findValidPort();
        server = new SimpleHttpServer(port);
        server.start("/api/v1/convert");
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testSqlConvert() throws IOException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        ConnectContext ctx = TestWithFeService.createDefaultCtx();
        // 1. not COM_QUERY
        String res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_STMT_RESET);
        Assert.assertEquals(originSql, res);
        // 2. config sql_convertor_service not set
        res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_QUERY);
        Assert.assertEquals(originSql, res);
        // 3. session var sql_dialect not set
        Config.sql_convertor_service = "http://127.0.0.1:" + port + "/api/v1/convert";
        res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_QUERY);
        Assert.assertEquals(originSql, res);
        // 4. not support dialect
        ctx.getSessionVariable().setSqlDialect("sqlserver");
        res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_QUERY);
        Assert.assertEquals(originSql, res);
        // 5. test presto
        ctx.getSessionVariable().setSqlDialect("presto");
        server.setResponse("{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_QUERY);
        Assert.assertEquals(expectedSql, res);
        // 6. test response version error
        server.setResponse("{\"version\": \"v2\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_QUERY);
        Assert.assertEquals(originSql, res);
        // 7. test response code error
        server.setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 400, \"message\": \"\"}");
        res = SQLDialectUtils.convertStmtWithDialect(originSql, ctx, MysqlCommand.COM_QUERY);
        Assert.assertEquals(originSql, res);
    }
}
