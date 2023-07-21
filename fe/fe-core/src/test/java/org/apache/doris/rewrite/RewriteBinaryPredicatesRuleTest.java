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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RewriteBinaryPredicatesRuleTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        connectContext = createDefaultCtx();
        createDatabase("db");
        useDatabase("db");
        String createTable = "create table table1(id smallint, cost bigint sum) "
                + "aggregate key(`id`) distributed by hash (`id`) buckets 4 "
                + "properties (\"replication_num\"=\"1\");";
        createTable(createTable);
    }

    @Test
    public void testNormal() throws Exception {
        testRewrite(Operator.EQ, "2.0", Operator.EQ, 2L, true);
        testBoolean(Operator.EQ, "2.5", false);

        testBase(Operator.NE, "2.0", Operator.NE, 2L);
        testBoolean(Operator.NE, "2.5", true);

        testBase(Operator.LE, "2.0", Operator.LE, 2L);
        testBase(Operator.LE, "-2.5", Operator.LT, -2L);
        testBase(Operator.LE, "2.5", Operator.LE, 2L);

        testBase(Operator.GE, "2.0", Operator.GE, 2L);
        testBase(Operator.GE, "-2.5", Operator.GE, -2L);
        testBase(Operator.GE, "2.5", Operator.GT, 2L);

        testBase(Operator.LT, "2.0", Operator.LT, 2L);
        testBase(Operator.LT, "-2.5", Operator.LT, -2L);
        testBase(Operator.LT, "2.5", Operator.LE, 2L);

        testBase(Operator.GT, "2.0", Operator.GT, 2L);
        testBase(Operator.GT, "-2.5", Operator.GE, -2L);
        testBase(Operator.GT, "2.5", Operator.GT, 2L);
    }

    @Test
    public void testOutOfRange() throws Exception {
        // 32767 -32768
        testBoolean(Operator.EQ, "-32769.0", false);
        testBase(Operator.EQ, "32767.0", Operator.EQ, 32767L);

        testBoolean(Operator.NE, "32768.0", true);

        testBoolean(Operator.LE, "32768.2", true);
        testBoolean(Operator.LE, "-32769.1", false);
        testBase(Operator.LE, "32767.0", Operator.LE, 32767L);

        testBoolean(Operator.GE, "32768.1", false);
        testBoolean(Operator.GE, "-32769.1", true);
        testBase(Operator.GE, "32767.0", Operator.GE, 32767L);

        testBoolean(Operator.LT, "32768.1", true);
        testBoolean(Operator.LT, "-32769.1", false);
        testBase(Operator.LT, "32767.1", Operator.LE, 32767L);

        testBoolean(Operator.GT, "32768.1", false);
        testBoolean(Operator.GT, "-32769.1", true);
        testBase(Operator.GT, "32767.0", Operator.GT, 32767L);
    }

    private void testRewrite(Operator operator, String queryLiteral, Operator expectedOperator, long expectedChild1,
            boolean expectedResultAfterRewritten)
            throws Exception {
        Expr expr1 = getExpr(operator, queryLiteral);
        if (expr1 instanceof BoolLiteral) {
            Assertions.assertEquals(((BoolLiteral) expr1).getValue(), expectedResultAfterRewritten);
        } else {
            testBase(operator, queryLiteral, expectedOperator, expectedChild1);
        }
    }

    private void testBase(Operator operator, String queryLiteral, Operator expectedOperator, long expectedChild1)
            throws Exception {
        Expr expr1 = getExpr(operator, queryLiteral);
        Assertions.assertTrue(expr1 instanceof BinaryPredicate);
        Assertions.assertEquals(expectedOperator, ((BinaryPredicate) expr1).getOp());
        Assertions.assertEquals(PrimitiveType.SMALLINT, expr1.getChild(0).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.SMALLINT, expr1.getChild(1).getType().getPrimitiveType());
        Assertions.assertEquals(expectedChild1, ((LiteralExpr) expr1.getChild(1)).getLongValue());
    }

    private void testBoolean(Operator operator, String queryLiteral, boolean result) throws Exception {
        Expr expr1 = getExpr(operator, queryLiteral);
        Assertions.assertTrue(expr1 instanceof BoolLiteral);
        Assertions.assertEquals(result, ((BoolLiteral) expr1).getValue());
    }

    private Expr getExpr(Operator operator, String queryLiteral) throws Exception {
        String queryFormat = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from table1 where id %s %s;";
        String query = String.format(queryFormat, operator.toString(), queryLiteral);
        StmtExecutor executor1 = getSqlStmtExecutor(query);
        Assertions.assertNotNull(executor1);
        return ((SelectStmt) executor1.getParsedStmt()).getWhereClause();
    }
}
