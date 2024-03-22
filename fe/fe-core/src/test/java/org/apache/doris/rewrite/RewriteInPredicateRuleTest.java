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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RewriteInPredicateRuleTest extends TestWithFeService {
    private static final String DB_NAME = "testdb";
    private static final String TABLE_SMALL = "table_small";
    private static final String TABLE_LARGE = "table_large";

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext = createDefaultCtx();
        createDatabase(DB_NAME);
        useDatabase(DB_NAME);
        String createTableFormat = "create table %s(id %s, `date` datetime, cost bigint sum) "
                + "aggregate key(`id`, `date`) distributed by hash (`id`) buckets 4 "
                + "properties (\"replication_num\"=\"1\");";
        createTable(String.format(createTableFormat, TABLE_SMALL, PrimitiveType.SMALLINT));
        createTable(String.format(createTableFormat, TABLE_LARGE, PrimitiveType.LARGEINT));
    }

    @Test
    public void testIntLiteralAndLargeIntLiteral() throws Exception {
        // id in (TINY_INT_MIN, SMALL_INT_MIN, INT_MIN, BIG_INT_MAX, LARGE_INT_MAX)
        // => id in (TINY_INT_MIN, SMALL_INT_MIN)
        testBase(3, PrimitiveType.SMALLINT, IntLiteral.TINY_INT_MIN, TABLE_SMALL,
                String.valueOf(IntLiteral.TINY_INT_MIN), String.valueOf(IntLiteral.SMALL_INT_MAX),
                String.valueOf(IntLiteral.INT_MIN), String.valueOf(IntLiteral.BIG_INT_MAX),
                LargeIntLiteral.LARGE_INT_MAX.toString());

        // id in (TINY_INT_MIN, SMALL_INT_MIN, INT_MIN, BIG_INT_MAX, LARGE_INT_MAX)
        // => id in (TINY_INT_MIN, SMALL_INT_MIN, INT_MIN, BIG_INT_MAX, LARGE_INT_MAX)
        testBase(6, PrimitiveType.LARGEINT, IntLiteral.TINY_INT_MIN, TABLE_LARGE,
                String.valueOf(IntLiteral.TINY_INT_MIN), String.valueOf(IntLiteral.SMALL_INT_MAX),
                String.valueOf(IntLiteral.INT_MIN), String.valueOf(IntLiteral.BIG_INT_MAX),
                LargeIntLiteral.LARGE_INT_MAX.toString());
    }

    @Test
    public void testDecimalLiteral() throws Exception {
        // type of id is smallint: id in (2.0, 3.5) => id in (2)
        testBase(2, PrimitiveType.SMALLINT, 2, TABLE_SMALL, "2.0", "3.5");

        testBase(2, PrimitiveType.SMALLINT, 3, TABLE_SMALL, "2.1", "3.0", "3.5");

        // type of id is largeint: id in (2.0, 3.5) => id in (2)
        testBase(2, PrimitiveType.LARGEINT, 2, TABLE_LARGE, "2.0", "3.5");
    }

    @Test
    public void testStringLiteral() throws Exception {
        // type of id is smallint: id in ("2.0", "3.5") => id in (2)
        testBase(2, PrimitiveType.SMALLINT, 2, TABLE_SMALL, "\"2.0\"", "\"3.5\"");

        // type of id is largeint: id in ("2.0", "3.5") => id in (2)
        testBase(2, PrimitiveType.LARGEINT, 2, TABLE_LARGE, "\"2.0\"", "\"3.5\"");
    }

    @Test
    public void testBooleanLiteral() throws Exception {
        // type of id is smallint: id in (true, false) => id in (1, 0)
        testBase(3, PrimitiveType.SMALLINT, 0, TABLE_SMALL, "false", "true");

        // type of id is largeint: id in (true, false) => id in (1, 0)
        testBase(3, PrimitiveType.LARGEINT, 1, TABLE_LARGE, "true", "false");
    }

    @Test
    public void testMixedLiteralExpr() throws Exception {
        // type of id is smallint: id in (1, 2.0, 3.3) -> id in (1, 2)
        testBase(3, PrimitiveType.SMALLINT, 1, TABLE_SMALL, "1", "2.0", "3.3");
        // type of id is smallint: id in (1, 1.0, 1.1) => id in (1, 1)
        testBase(3, PrimitiveType.SMALLINT, 1, TABLE_SMALL, "1", "1.0", "1.1");
        // type of id is smallint: id in ("1.0", 2.0, 3.3, "5.2") => id in (1, 2)
        testBase(3, PrimitiveType.SMALLINT, 1, TABLE_SMALL, "\"1.0\"", "2.0", "3.3", "\"5.2\"");
        // type of id is smallint: id in (false, 2.0, 3.3, "5.2", true) => id in (0, 2, 1)
        testBase(4, PrimitiveType.SMALLINT, 0, TABLE_SMALL, "false", "2.0", "3.3", "\"5.2\"", "true");

        // largeint
        testBase(3, PrimitiveType.LARGEINT, 1, TABLE_LARGE, "1", "2.0", "3.3");
        testBase(3, PrimitiveType.LARGEINT, 1, TABLE_LARGE, "1", "1.0", "1.1");
        testBase(3, PrimitiveType.LARGEINT, 1, TABLE_LARGE, "\"1.0\"", "2.0", "3.3", "\"5.2\"");
        testBase(4, PrimitiveType.LARGEINT, 0, TABLE_LARGE, "false", "2.0", "3.3", "\"5.2\"", "true");
    }

    @Test
    public void testEmpty() throws Exception {
        // type of id is smallint: id in (5.5, "6.2") => false
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from table_small where id in (5.5, \"6.2\");";
        StmtExecutor executor1 = getSqlStmtExecutor(query);
        Expr expr1 = ((SelectStmt) executor1.getParsedStmt()).getWhereClause();
        Assertions.assertTrue(expr1 instanceof BoolLiteral);
        Assertions.assertFalse(((BoolLiteral) expr1).getValue());
    }

    private void testBase(int childrenNum, PrimitiveType type, long expectedOfChild1, String... literals)
            throws Exception {
        List<String> list = Lists.newArrayList();
        Lists.newArrayList(literals).forEach(e -> list.add("%s"));
        list.remove(list.size() - 1);
        String queryFormat = "select /*+ SET_VAR(enable_nereids_planner=false,enable_fold_constant_by_be=false) */ * from %s where id in (" + Joiner.on(", ").join(list) + ");";
        String query = String.format(queryFormat, literals);
        StmtExecutor executor1 = getSqlStmtExecutor(query);
        Expr expr1 = ((SelectStmt) executor1.getParsedStmt()).getWhereClause();
        Assertions.assertTrue(expr1 instanceof InPredicate);
        Assertions.assertEquals(childrenNum, expr1.getChildren().size());
        Assertions.assertEquals(type, expr1.getChild(0).getType().getPrimitiveType());
        Assertions.assertEquals(type, expr1.getChild(1).getType().getPrimitiveType());
        Assertions.assertEquals(expectedOfChild1, ((LiteralExpr) expr1.getChild(1)).getLongValue());
    }
}
