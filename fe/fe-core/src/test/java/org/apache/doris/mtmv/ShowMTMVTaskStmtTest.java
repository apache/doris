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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ShowMTMVTaskStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Test;

public class ShowMTMVTaskStmtTest {
    @Test
    public void testNormal() throws UserException, AnalysisException {
        final Analyzer analyzer =  AccessTestUtil.fetchBlockAnalyzer();
        ShowMTMVTaskStmt stmt = new ShowMTMVTaskStmt();
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getTaskId());
        Assert.assertNull(stmt.getDbName());
        Assert.assertNull(stmt.getMVName());
        Assert.assertEquals(14, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("SHOW MTMV TASK", stmt.toSql());

        stmt = new ShowMTMVTaskStmt("task1");
        stmt.analyze(analyzer);
        Assert.assertNotNull(stmt.getTaskId());
        Assert.assertNull(stmt.getDbName());
        Assert.assertNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV TASK FOR task1", stmt.toSql());

        stmt = new ShowMTMVTaskStmt("db1", null);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getTaskId());
        Assert.assertNotNull(stmt.getDbName());
        Assert.assertNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV TASK FROM db1", stmt.toSql());

        TableName tableName = new TableName(null, null, "mv1");
        stmt = new ShowMTMVTaskStmt(null, tableName);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getTaskId());
        Assert.assertNull(stmt.getDbName());
        Assert.assertNotNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV TASK ON `mv1`", stmt.toSql());

        tableName = new TableName(null, "db2", "mv1");
        stmt = new ShowMTMVTaskStmt(null, tableName);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getTaskId());
        Assert.assertNotNull(stmt.getDbName());
        Assert.assertNotNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV TASK ON `db2`.`mv1`", stmt.toSql());
    }

    @Test(expected = UserException.class)
    public void testConflictDbName() throws UserException, AnalysisException {
        final Analyzer analyzer =  AccessTestUtil.fetchBlockAnalyzer();
        TableName tableName = new TableName(null, "db2", "mv1");

        ShowMTMVTaskStmt stmt = new ShowMTMVTaskStmt("db1", tableName);
        stmt.analyze(analyzer);
    }
}
