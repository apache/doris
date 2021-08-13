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

import mockit.Mocked;
import org.apache.doris.common.*;
import org.apache.doris.mysql.privilege.*;
import org.apache.doris.qe.*;
import org.junit.*;

public class ShowTableIdStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException, AnalysisException  {
        ShowTableIdStmt stmt = new ShowTableIdStmt(123456);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW TABLE 123456", stmt.toString());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("DbName", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("TableName", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("DbId", stmt.getMetaData().getColumn(2).getName());
    }
}