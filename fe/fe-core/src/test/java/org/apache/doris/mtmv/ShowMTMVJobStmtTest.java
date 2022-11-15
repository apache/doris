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
import org.apache.doris.analysis.ShowMTMVJobStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Test;

public class ShowMTMVJobStmtTest {
    @Test
    public void testNormal() throws UserException, AnalysisException {
        final Analyzer analyzer =  AccessTestUtil.fetchBlockAnalyzer();
        ShowMTMVJobStmt stmt = new ShowMTMVJobStmt();
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getJobName());
        Assert.assertNull(stmt.getDbName());
        Assert.assertNull(stmt.getMVName());
        Assert.assertEquals(13, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("SHOW MTMV JOB", stmt.toSql());

        stmt = new ShowMTMVJobStmt("job1");
        stmt.analyze(analyzer);
        Assert.assertNotNull(stmt.getJobName());
        Assert.assertNull(stmt.getDbName());
        Assert.assertNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV JOB FOR job1", stmt.toSql());

        stmt = new ShowMTMVJobStmt("db1", null);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getJobName());
        Assert.assertNotNull(stmt.getDbName());
        Assert.assertNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV JOB FROM db1", stmt.toSql());

        TableName tableName = new TableName(null, null, "mv1");
        stmt = new ShowMTMVJobStmt(null, tableName);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getJobName());
        Assert.assertNull(stmt.getDbName());
        Assert.assertNotNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV JOB ON `mv1`", stmt.toSql());

        tableName = new TableName(null, "db2", "mv1");
        stmt = new ShowMTMVJobStmt(null, tableName);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getJobName());
        Assert.assertNotNull(stmt.getDbName());
        Assert.assertNotNull(stmt.getMVName());
        Assert.assertEquals("SHOW MTMV JOB ON `db2`.`mv1`", stmt.toSql());
    }

    @Test(expected = UserException.class)
    public void testConflictDbName() throws UserException, AnalysisException {
        final Analyzer analyzer =  AccessTestUtil.fetchBlockAnalyzer();
        TableName tableName = new TableName(null, "db2", "mv1");

        ShowMTMVJobStmt stmt = new ShowMTMVJobStmt("db1", tableName);
        stmt.analyze(analyzer);
    }
}
