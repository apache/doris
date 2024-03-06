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

import org.apache.doris.analysis.LikePredicate.Operator;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;

import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ShowAnalyzeStmtTest {
    private Analyzer analyzer;
    private ConnectContext ctx = new ConnectContext();

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
    }

    @After
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testNoPrivilege() throws Exception {
        new MockUp<ShowAnalyzeStmt>() {
            @Mock
            public void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
                ErrorReport.reportAnalysisException(
                        ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                        "SHOW ANALYZE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tblName);
            }
        };

        ShowAnalyzeStmt stmt = new ShowAnalyzeStmt(new TableName("a.b.c"), null, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
    }

    @Test
    public void testFeatureDisabled() {
        ShowAnalyzeStmt stmt = new ShowAnalyzeStmt(1, null);
        ctx.getSessionVariable().enableStats = false;
        Assertions.assertThrows(UserException.class, () -> stmt.analyze(analyzer));
    }

    @Test
    public void testNormal() throws Exception {
        new MockUp<ShowAnalyzeStmt>() {
            @Mock
            public void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
                //do nothing
            }
        };
        ShowAnalyzeStmt stmt = new ShowAnalyzeStmt(new TableName("a.b.c"), null, true);
        stmt.analyze(analyzer);
        Assertions.assertEquals("SHOW ANALYZE `a`.`b`.`c`", stmt.toSql());
        Assertions.assertEquals(stmt.getDbTableName(), new TableName("a.b.c"));
        Assertions.assertEquals(stmt.isAuto(), true);

        stmt = new ShowAnalyzeStmt(1001, null);
        stmt.analyze(analyzer);
        Assertions.assertEquals("SHOW ANALYZE 1001", stmt.toSql());
        Assertions.assertEquals(stmt.isAuto(), false);
        Assertions.assertEquals(stmt.getJobId(), 1001);
    }

    @Test
    public void testNormalWithPredicate() throws Exception {
        new MockUp<ShowAnalyzeStmt>() {
            @Mock
            public void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
                //do nothing
            }
        };
        TableName tableName = new TableName("a.b.c");
        Expr where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "STATE"), new StringLiteral("FINISHED"));
        ShowAnalyzeStmt stmt = new ShowAnalyzeStmt(tableName, where, false);

        Assertions.assertThrows(IllegalArgumentException.class, () -> stmt.getStateValue());
        Assertions.assertThrows(IllegalArgumentException.class, () -> stmt.getWhereClause());

        stmt.analyze(analyzer);
        Assertions.assertEquals("SHOW ANALYZE `a`.`b`.`c` WHERE (`a`.`b`.`c`.`STATE` = 'FINISHED')", stmt.toSql());
        Assertions.assertEquals(where, stmt.getWhereClause());
    }

    @Test
    public void testNormalWithPredicateException() throws Exception {
        new MockUp<ShowAnalyzeStmt>() {
            @Mock
            public void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
                //do nothing
            }
        };
        TableName tableName = new TableName("a.b.c");

        Expr where = new LikePredicate(Operator.LIKE, new StringLiteral("%.b.%"), new SlotRef(tableName, "STATE_NAME"));
        ShowAnalyzeStmt stmt0 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt0.analyze(analyzer));

        where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "STATE_NAME"), new SlotRef(tableName, "STATE_NAME"));
        ShowAnalyzeStmt stmt1 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt1.analyze(analyzer));

        where = new BinaryPredicate(BinaryPredicate.Operator.GE, new StringLiteral("test"), new StringLiteral("test"));
        ShowAnalyzeStmt stmt2 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt2.analyze(analyzer));

        where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new StringLiteral("test"), new StringLiteral("test"));
        ShowAnalyzeStmt stmt3 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt3.analyze(analyzer));

        where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "test2"), new StringLiteral("test"));
        ShowAnalyzeStmt stmt4 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt4.analyze(analyzer));

        where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "STATE"), new StringLiteral(""));
        ShowAnalyzeStmt stmt5 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt5.analyze(analyzer));

        where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "STATE"), new StringLiteral("WRONG_STATE"));
        ShowAnalyzeStmt stmt6 = new ShowAnalyzeStmt(tableName, where, false);
        Assertions.assertThrows(AnalysisException.class, () -> stmt6.analyze(analyzer));
    }

    @Test
    public void testGetMetadata() {
        ShowAnalyzeStmt stmt = new ShowAnalyzeStmt(new TableName("a.b.c"), null, false);

        ShowResultSetMetaData result = stmt.getMetaData();
        Assertions.assertEquals(result.getColumnCount(), 14);
        result.getColumns().forEach(col -> Assertions.assertEquals(col.getType(), ScalarType.createVarchar(128)));
    }
}
