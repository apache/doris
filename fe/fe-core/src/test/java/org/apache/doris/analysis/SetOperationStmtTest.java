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

package org.apache.doris.analysis;

import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;

public class SetOperationStmtTest {
    private Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws Exception {
        String sql = "select k1,k2 from t where k1='a' union select k1,k2 from t where k1='b';";
        SqlScanner input = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(input);
        SetOperationStmt stmt = (SetOperationStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(SetOperationStmt.Operation.UNION, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' intersect select k1,k2 from t where k1='b';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(SetOperationStmt.Operation.INTERSECT, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' except select k1,k2 from t where k1='b';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(SetOperationStmt.Operation.EXCEPT, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' minus select k1,k2 from t where k1='b';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(SetOperationStmt.Operation.EXCEPT, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' union select k1,k2 from t where k1='b' intersect select k1,k2 from t "
                + "where k1='c' except select k1,k2 from t where k1='d';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(SetOperationStmt.Operation.UNION, stmt.getOperands().get(1).getOperation());
        Assert.assertEquals(SetOperationStmt.Operation.INTERSECT, stmt.getOperands().get(2).getOperation());
        Assert.assertEquals(SetOperationStmt.Operation.EXCEPT, stmt.getOperands().get(3).getOperation());
        Assert.assertEquals(4, stmt.getOperands().size());


    }

}
