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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;

/**
 * ExpressionUtils ut.
 */
public class ExpressionUtilsTest extends TestWithFeService {

    private static final NereidsParser PARSER = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("expression_test");
        useDatabase("expression_test");

        createTable("CREATE TABLE IF NOT EXISTS lineitem (\n"
                + "  L_ORDERKEY    INTEGER NOT NULL,\n"
                + "  L_PARTKEY     INTEGER NOT NULL,\n"
                + "  L_SUPPKEY     INTEGER NOT NULL,\n"
                + "  L_LINENUMBER  INTEGER NOT NULL,\n"
                + "  L_QUANTITY    DECIMALV3(15,2) NOT NULL,\n"
                + "  L_EXTENDEDPRICE  DECIMALV3(15,2) NOT NULL,\n"
                + "  L_DISCOUNT    DECIMALV3(15,2) NOT NULL,\n"
                + "  L_TAX         DECIMALV3(15,2) NOT NULL,\n"
                + "  L_RETURNFLAG  CHAR(1) NOT NULL,\n"
                + "  L_LINESTATUS  CHAR(1) NOT NULL,\n"
                + "  L_SHIPDATE    DATE NOT NULL,\n"
                + "  L_COMMITDATE  DATE NOT NULL,\n"
                + "  L_RECEIPTDATE DATE NOT NULL,\n"
                + "  L_SHIPINSTRUCT CHAR(25) NOT NULL,\n"
                + "  L_SHIPMODE     CHAR(10) NOT NULL,\n"
                + "  L_COMMENT      VARCHAR(44) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)\n"
                + "DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        createTable("CREATE TABLE IF NOT EXISTS orders  (\n"
                + "  O_ORDERKEY       INTEGER NOT NULL,\n"
                + "  O_CUSTKEY        INTEGER NOT NULL,\n"
                + "  O_ORDERSTATUS    CHAR(1) NOT NULL,\n"
                + "  O_TOTALPRICE     DECIMALV3(15,2) NOT NULL,\n"
                + "  O_ORDERDATE      DATE NOT NULL,\n"
                + "  O_ORDERPRIORITY  CHAR(15) NOT NULL,  \n"
                + "  O_CLERK          CHAR(15) NOT NULL, \n"
                + "  O_SHIPPRIORITY   INTEGER NOT NULL,\n"
                + "  O_COMMENT        VARCHAR(79) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)\n"
                + "DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        createTable("CREATE TABLE IF NOT EXISTS partsupp (\n"
                + "  PS_PARTKEY     INTEGER NOT NULL,\n"
                + "  PS_SUPPKEY     INTEGER NOT NULL,\n"
                + "  PS_AVAILQTY    INTEGER NOT NULL,\n"
                + "  PS_SUPPLYCOST  DECIMALV3(15,2)  NOT NULL,\n"
                + "  PS_COMMENT     VARCHAR(199) NOT NULL \n"
                + ")\n"
                + "DUPLICATE KEY(PS_PARTKEY, PS_SUPPKEY)\n"
                + "DISTRIBUTED BY HASH(PS_PARTKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    public void extractConjunctionTest() {
        List<Expression> expressions;
        Expression expr;

        expr = PARSER.parseExpression("a");
        expressions = ExpressionUtils.extractConjunction(expr);
        Assertions.assertEquals(1, expressions.size());
        Assertions.assertEquals(expr, expressions.get(0));

        expr = PARSER.parseExpression("a and b and c");
        Expression a = PARSER.parseExpression("a");
        Expression b = PARSER.parseExpression("b");
        Expression c = PARSER.parseExpression("c");

        expressions = ExpressionUtils.extractConjunction(expr);
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(a, expressions.get(0));
        Assertions.assertEquals(b, expressions.get(1));
        Assertions.assertEquals(c, expressions.get(2));

        expr = PARSER.parseExpression("(a or b) and c and (e or f)");
        expressions = ExpressionUtils.extractConjunction(expr);
        Expression aOrb = PARSER.parseExpression("a or b");
        Expression eOrf = PARSER.parseExpression("e or f");
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(aOrb, expressions.get(0));
        Assertions.assertEquals(c, expressions.get(1));
        Assertions.assertEquals(eOrf, expressions.get(2));
    }

    @Test
    public void extractDisjunctionTest() {
        List<Expression> expressions;
        Expression expr;

        expr = PARSER.parseExpression("a");
        expressions = ExpressionUtils.extractDisjunction(expr);
        Assertions.assertEquals(1, expressions.size());
        Assertions.assertEquals(expr, expressions.get(0));

        expr = PARSER.parseExpression("a or b or c");
        Expression a = PARSER.parseExpression("a");
        Expression b = PARSER.parseExpression("b");
        Expression c = PARSER.parseExpression("c");

        expressions = ExpressionUtils.extractDisjunction(expr);
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(a, expressions.get(0));
        Assertions.assertEquals(b, expressions.get(1));
        Assertions.assertEquals(c, expressions.get(2));

        expr = PARSER.parseExpression("(a and b) or c or (e and f)");
        expressions = ExpressionUtils.extractDisjunction(expr);
        Expression aAndb = PARSER.parseExpression("a and b");
        Expression eAndf = PARSER.parseExpression("e and f");
        Assertions.assertEquals(3, expressions.size());
        Assertions.assertEquals(aAndb, expressions.get(0));
        Assertions.assertEquals(c, expressions.get(1));
        Assertions.assertEquals(eAndf, expressions.get(2));
    }

    @Test
    public void shuttleExpressionWithLineageTest1() {
        PlanChecker.from(connectContext)
                .checkExplain("SELECT (o.c1_abs + ps.c2_abs) as add_alias, l.L_LINENUMBER, o.O_ORDERSTATUS "
                                + "FROM "
                                + "lineitem as l "
                                + "LEFT JOIN "
                                + "(SELECT abs(O_TOTALPRICE + 10) as c1_abs, O_CUSTKEY, O_ORDERSTATUS, O_ORDERKEY "
                                + "FROM orders) as o "
                                + "ON l.L_ORDERKEY = o.O_ORDERKEY "
                                + "JOIN "
                                + "(SELECT abs(sqrt(PS_SUPPLYCOST)) as c2_abs, PS_AVAILQTY, PS_PARTKEY, PS_SUPPKEY "
                                + "FROM partsupp) as ps "
                                + "ON l.L_PARTKEY = ps.PS_PARTKEY and l.L_SUPPKEY = ps.PS_SUPPKEY",
                        nereidsPlanner -> {
                            Plan rewrittenPlan = nereidsPlanner.getRewrittenPlan();
                            List<? extends Expression> originalExpressions = rewrittenPlan.getExpressions();
                            List<? extends Expression> shuttledExpressions
                                    = ExpressionUtils.shuttleExpressionWithLineage(originalExpressions, rewrittenPlan,
                                    Sets.newHashSet(), Sets.newHashSet(), new BitSet());
                            assertExpect(originalExpressions, shuttledExpressions,
                                    "(cast(abs((cast(O_TOTALPRICE as DECIMALV3(16, 2)) + 10.00)) as "
                                            + "DOUBLE) + abs(sqrt(cast(PS_SUPPLYCOST as DOUBLE))))",
                                    "L_LINENUMBER",
                                    "O_ORDERSTATUS");
                        });
    }

    private void assertExpect(List<? extends Expression> originalExpressions,
            List<? extends Expression> shuttledExpressions,
            String... expectExpressions) {
        Assertions.assertEquals(originalExpressions.size(), shuttledExpressions.size());
        Assertions.assertEquals(originalExpressions.size(), expectExpressions.length);
        for (int index = 0; index < shuttledExpressions.size(); index++) {
            Assertions.assertEquals(shuttledExpressions.get(index).toSql(), expectExpressions[index]);
        }
    }
}
