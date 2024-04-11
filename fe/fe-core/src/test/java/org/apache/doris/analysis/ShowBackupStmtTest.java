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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;

import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.atomic.AtomicBoolean;

public class ShowBackupStmtTest {
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
    public void getMetaData() {
        ShowBackupStmt stmt = new ShowBackupStmt("", null);
        ShowResultSetMetaData result = stmt.getMetaData();
        Assertions.assertEquals(result.getColumnCount(), 14);
        result.getColumns().forEach(col -> Assertions.assertEquals(col.getType(), ScalarType.createVarchar(30)));
    }

    @Test
    public void testNormalAnalyze() throws Exception {
        ShowBackupStmt stmt = new ShowBackupStmt("", null);

        AtomicBoolean privilege = new AtomicBoolean(true);
        new MockUp<AccessControllerManager>() {
            @Mock
            public boolean checkDbPriv(ConnectContext ctx, String ctl, String qualifiedDb, PrivPredicate wanted) {
                return privilege.get();
            }
        };

        stmt.analyze(analyzer);
        Assertions.assertEquals(stmt.toSql(), "SHOW BACKUP ");

        stmt = new ShowBackupStmt("",
                new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(new TableName("a.b.c"), "snapshotname"),
                        new StringLiteral("FINISHED")));
        stmt.analyze(analyzer);
        Assertions.assertEquals(stmt.toSql(), "SHOW BACKUP WHERE (`a`.`b`.`c`.`snapshotname` = 'FINISHED')");

        stmt = new ShowBackupStmt("",
                new LikePredicate(Operator.LIKE, new SlotRef(new TableName("a.b.c"), "snapshotname"),
                        new StringLiteral("%.b.%")));
        stmt.analyze(analyzer);
        Assertions.assertEquals(stmt.toSql(), "SHOW BACKUP WHERE `a`.`b`.`c`.`snapshotname` LIKE '%.b.%'");
    }

    @Test
    public void testExceptionAnalyze() {
        AtomicBoolean privilege = new AtomicBoolean(false);
        new MockUp<AccessControllerManager>() {
            @Mock
            public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
                return privilege.get();
            }
        };

        /* when no privilege */ {
            ShowBackupStmt stmt = new ShowBackupStmt("", null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* where clause not supported */ {
            privilege.set(true);
            ShowBackupStmt stmt = new ShowBackupStmt("test", new BoolLiteral(true));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* where clause op not supported */ {
            ShowBackupStmt stmt = new ShowBackupStmt("test",
                    new LikePredicate(Operator.REGEXP, new SlotRef(new TableName(), "STATE_NAME"),
                            new StringLiteral("%.b.%")));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* where clause op not supported*/ {
            ShowBackupStmt stmt = new ShowBackupStmt("test", new BinaryPredicate(
                    BinaryPredicate.Operator.GE, new SlotRef(new TableName(), "STATE_NAME"),
                    new StringLiteral("blah")));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /*where clause op not supported*/ {
            ShowBackupStmt stmt = new ShowBackupStmt("test", new BinaryPredicate(
                    BinaryPredicate.Operator.EQ, new SlotRef(new TableName(), "STATE_NAME"),
                    new StringLiteral("blah")));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* where clause op type(left) not supported */ {
            ShowBackupStmt stmt = new ShowBackupStmt("test", new BinaryPredicate(
                    BinaryPredicate.Operator.EQ, new IntLiteral(1), new IntLiteral(1)));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* where clause op type(right) not supported */ {
            ShowBackupStmt stmt = new ShowBackupStmt("test", new BinaryPredicate(
                    BinaryPredicate.Operator.EQ, new SlotRef(new TableName(), "snapshotname"), new IntLiteral(1)));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* where clause operand is empty */ {
            ShowBackupStmt stmt = new ShowBackupStmt("test", new BinaryPredicate(
                    BinaryPredicate.Operator.EQ, new SlotRef(new TableName(), "snapshotname"), new StringLiteral("")));
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }
    }
}
