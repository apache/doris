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

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackendStmtTest {

    private static Analyzer analyzer;

    @BeforeClass
    public static void setUp() throws Exception {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    public BackendClause createStmt(int type) {
        BackendClause stmt = null;
        switch (type) {
            case 1:
                // missing ip
                stmt = new AddBackendClause(Lists.newArrayList(":12346"));
                break;
            case 2:
                // invalid ip
                stmt = new AddBackendClause(Lists.newArrayList("asdasd:12345"));
                break;
            case 3:
                // invalid port
                stmt = new AddBackendClause(Lists.newArrayList("10.1.2.3:123467"));
                break;
            case 4:
                // normal add
                stmt = new AddBackendClause(Lists.newArrayList("192.168.1.1:12345"));
                break;
            case 5:
                // normal remove
                stmt = new DropBackendClause(Lists.newArrayList("192.168.1.2:12345"));
                break;
            default:
                break;
        }
        return stmt;
    }

    @Test(expected = AnalysisException.class)
    public void initBackendsTest1() throws Exception {
        BackendClause stmt = createStmt(1);
        stmt.analyze(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void initBackendsTest3() throws Exception {
        BackendClause stmt = createStmt(3);
        stmt.analyze(analyzer);
    }

    @Test
    public void initBackendsTest4() throws Exception {
        BackendClause stmt = createStmt(4);
        stmt.analyze(analyzer);
        Assert.assertEquals("ADD BACKEND \"192.168.1.1:12345\"", stmt.toSql());
    }

    @Test
    public void initBackendsTest5() throws Exception {
        BackendClause stmt = createStmt(5);
        stmt.analyze(analyzer);
        Assert.assertEquals("DROP BACKEND \"192.168.1.2:12345\"", stmt.toSql());
    }
}
