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
import org.apache.doris.common.proc.BuildIndexProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShowBuildIndexStmtTest {
    private Analyzer analyzer;
    private ConnectContext ctx = new ConnectContext();
    @Mocked
    private BuildIndexProcDir procDir;

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
        ShowBuildIndexStmt stmt = new ShowBuildIndexStmt("", null, ImmutableList.of(), null);
        ShowResultSetMetaData result = stmt.getMetaData();
        Assertions.assertEquals(result.getColumnCount(), 10);
        result.getColumns().forEach(col -> Assertions.assertEquals(col.getType(), ScalarType.createVarchar(30)));
    }

    @Test
    public void testAnalysisNormal() throws Exception {
        AtomicBoolean pass = new AtomicBoolean(false);
        new MockUp<ProcService>() {
            @Mock
            public ProcNodeInterface open(String path) throws AnalysisException {
                if (pass.get()) {
                    return procDir;
                }
                return null;
            }
        };

        ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, null, null, null);
        Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));

        pass.set(true);
        stmt.analyze(analyzer);
        Assertions.assertEquals(stmt.toString(), "SHOW BUILD INDEX FROM `testDb`");
        Assertions.assertEquals(stmt.getDbName(), "testDb");

        TableName tableName = new TableName("a.b.c");

        /* case: WHERE compound predicate, Limit, Order */ {
            Expr subWhere1 = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(tableName, "createtime"),
                    new StringLiteral("%.b.%"));
            Expr subWhere2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "tablename"),
                    new StringLiteral("%.b.%"));
            Expr where = new CompoundPredicate(CompoundPredicate.Operator.AND, subWhere1, subWhere2);
            List<OrderByElement> orderBy = Arrays.asList(
                    new OrderByElement(new SlotRef(tableName, "TableName"), false, false));
            ShowBuildIndexStmt stmt1 = new ShowBuildIndexStmt(null, where, orderBy, new LimitElement(1, 100));
            Assertions.assertEquals(stmt1.toSql(), "SHOW BUILD INDEX  WHERE (`a`.`b`.`c`.`createtime` > '%.b.%') "
                    + "AND (`a`.`b`.`c`.`tablename` = '%.b.%') ORDER BY `a`.`b`.`c`.`TableName` DESC NULLS LAST "
                    + "LIMIT 1, 100");

            stmt1.analyze(analyzer);
            Assertions.assertEquals(stmt1.toSql(), "SHOW BUILD INDEX FROM `testDb` WHERE "
                    + "(`a`.`b`.`c`.`createtime` > CAST('%.b.%' AS datetimev2(0))) "
                    + "AND (`a`.`b`.`c`.`tablename` = '%.b.%') "
                    + "ORDER BY `a`.`b`.`c`.`TableName` DESC NULLS LAST LIMIT 1, 100");

            Assertions.assertEquals(stmt1.getFilterMap().size(), 2);
            Assertions.assertEquals(stmt1.getNode(), procDir);
            Assertions.assertEquals(stmt1.getOrderPairs().size(), 1);
            Assertions.assertEquals(stmt1.getLimitElement().getLimit(), 100);
            Assertions.assertEquals(stmt1.getLimitElement().getOffset(), 1);
        }
    }

    @Test
    public void testAnalysisWhereException() throws Exception {
        new MockUp<ProcService>() {
            @Mock
            public ProcNodeInterface open(String path) throws AnalysisException {
                return null;
            }
        };

        TableName tableName = new TableName("a.b.c");

        /* case: no BinaryPredicate */ {
            Expr where = new LikePredicate(Operator.LIKE, new StringLiteral("%.b.%"),
                    new SlotRef(tableName, "STATE_NAME"));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: binary predicate - left not SlotRef */ {
            Expr where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new StringLiteral("%.b.%"),
                    new SlotRef(tableName, "STATE_NAME"));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: binary predicate - left key wrong column */ {
            Expr where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "WRONG_COL"),
                    new StringLiteral("%.b.%"));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: binary predicate - right key wrong type */ {
            Expr where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "tablename"),
                    new IntLiteral(1234));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: binary predicate - right key wrong type */ {
            Expr where = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "createtime"),
                    new IntLiteral(1234));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: binary predicate - wrong Operator */ {
            Expr where = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(tableName, "tablename"),
                    new StringLiteral("%.b.%"));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: where-success,  */ {
            Expr where = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(tableName, "createtime"),
                    new StringLiteral("%.b.%"));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: compound predicate - wrong operator */ {
            Expr subWhere = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(tableName, "createtime"),
                    new StringLiteral("%.b.%"));
            Expr where = new CompoundPredicate(CompoundPredicate.Operator.OR, subWhere, subWhere);
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: compound predicate - wrong operator */ {
            Expr subWhere1 = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(tableName, "createtime"),
                    new StringLiteral("%.b.%"));
            Expr subWhere2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(tableName, "tablename"),
                    new StringLiteral("%.b.%"));
            Expr where = new CompoundPredicate(CompoundPredicate.Operator.AND, subWhere1, subWhere2);
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, where, null, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }
    }

    @Test
    public void testAnalysisOrderByException() throws Exception {
        new MockUp<ProcService>() {
            @Mock
            public ProcNodeInterface open(String path) throws AnalysisException {
                return null;
            }
        };

        TableName tableName = new TableName("a.b.c");

        /* case: order by not slotRef */ {
            List<OrderByElement> orderBy = Arrays.asList(new OrderByElement(new IntLiteral(123), false, false));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, null, orderBy, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: order by not known column */ {
            List<OrderByElement> orderBy = Arrays.asList(new OrderByElement(new SlotRef(tableName, "unknown"), false, false));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, null, orderBy, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: order by success - fail on procDir */ {
            List<OrderByElement> orderBy = Arrays.asList(new OrderByElement(new SlotRef(tableName, "TableName"), false, false));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, null, orderBy, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: order by success - fail on procDir */ {
            List<OrderByElement> orderBy = Arrays.asList(new OrderByElement(new SlotRef(tableName, "TableName"), false, false));
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, null, orderBy, null);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }

        /* case: limit success - fail on procDir */ {
            LimitElement limit = new LimitElement();
            ShowBuildIndexStmt stmt = new ShowBuildIndexStmt(null, null, null, limit);
            Assertions.assertThrows(AnalysisException.class, () -> stmt.analyze(analyzer));
        }
    }
}
