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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableTest;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                                    = ExpressionUtils.shuttleExpressionWithLineage(originalExpressions, rewrittenPlan);
                            assertExpect(originalExpressions, shuttledExpressions,
                                    "(cast(abs((cast(O_TOTALPRICE as DECIMALV3(16, 2)) + 10.00)) as "
                                            + "DOUBLE) + abs(sqrt(cast(PS_SUPPLYCOST as DOUBLE))))",
                                    "L_LINENUMBER",
                                    "O_ORDERSTATUS");
                        });
    }

    @Test
    public void testExtractUniformSlot() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot c = new SlotReference("c", IntegerType.INSTANCE);
        Expression va = Literal.of(1);
        Expression vb = Literal.of(2);
        Expression vc = Literal.of(3);
        Expression expression = new And(Arrays.asList(new EqualTo(a, va), new EqualTo(b, vb), new EqualTo(c, vc)));
        Map<Slot, Expression> expectUniformSlots = Maps.newHashMap();
        expectUniformSlots.put(a, va);
        expectUniformSlots.put(b, vb);
        expectUniformSlots.put(c, vc);
        Assertions.assertEquals(expectUniformSlots, ExpressionUtils.extractUniformSlot(expression));
    }

    @Test
    public void testSlotInputEqualsOutput() {
        OlapTable olapTable = TableTest.newOlapTable(10000, "test", 0);
        Slot a = new SlotReference("id", IntegerType.INSTANCE);
        Slot b = new SlotReference("id", IntegerType.INSTANCE);
        Alias bAlias = new Alias(b.getExprId(), new NonNullable(b));
        LogicalProject<LogicalOdbcScan> project = new LogicalProject<>(ImmutableList.of(a, bAlias),
                new LogicalOdbcScan(new RelationId(0), olapTable, ImmutableList.of("test")));
        List<? extends Expression> expressions = ExpressionUtils.shuttleExpressionWithLineage(project.getOutput(),
                project, new BitSet());
        // should not loop, should break out loop
        Assertions.assertEquals(expressions, ImmutableList.of(a, bAlias.toSlot()));
    }

    @Test
    public void testReplaceNullAware() {
        Slot a = new SlotReference("id1", IntegerType.INSTANCE);
        Slot b = new SlotReference("id2", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);
        Expression replacedExpression = ExpressionUtils.replaceNullAware(a, replaceMap);
        Assertions.assertEquals(replacedExpression, b);

        replaceMap = new HashMap<>();
        Slot a2 = new SlotReference("id3", IntegerType.INSTANCE);
        replaceMap.put(a2, b);
        Expression replacedExpression1 = ExpressionUtils.replaceNullAware(a, replaceMap);
        // should return null
        Assertions.assertNull(replacedExpression1);

        Expression replacedExpression2 = ExpressionUtils.replace(a, replaceMap);
        // should return a
        Assertions.assertEquals(a, replacedExpression2);
    }

    @Test
    public void testReplaceNullSafeDirectMatch() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);

        Expression expr = new EqualTo(a, Literal.of(1));
        Expression replaced = ExpressionUtils.replaceNullSafe(expr, replaceMap, e -> null);

        Assertions.assertEquals(new EqualTo(b, Literal.of(1)), replaced);
    }

    @Test
    public void testReplaceNullSafeFallbackMatch() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot aKey = new SlotReference("a_key", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(aKey, b);

        Expression replaced = ExpressionUtils.replaceNullSafe(a, replaceMap, e -> e.equals(a) ? aKey : null);
        Assertions.assertEquals(b, replaced);
    }

    @Test
    public void testReplaceNullSafeNoHitKeepOriginal() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot missKey = new SlotReference("miss_key", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        Expression replaced1 = ExpressionUtils.replaceNullSafe(a, replaceMap, e -> null);
        Assertions.assertEquals(a, replaced1);

        Expression replaced2 = ExpressionUtils.replaceNullSafe(a, replaceMap, e -> missKey);
        Assertions.assertEquals(a, replaced2);
    }

    @Test
    public void testReplaceNullSafeDeepTraversal() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot c = new SlotReference("c", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);

        Expression expr = new And(Arrays.asList(
                new EqualTo(a, Literal.of(1)),
                new EqualTo(c, Literal.of(3))
        ));
        Expression replaced = ExpressionUtils.replaceNullSafe(expr, replaceMap, e -> null);

        Expression expected = new And(Arrays.asList(
                new EqualTo(b, Literal.of(1)),
                new EqualTo(c, Literal.of(3))
        ));
        Assertions.assertEquals(expected, replaced);
    }

    @Test
    public void testReplaceNullAwareExpressionVariantKeepOriginal() {
        Slot v = new SlotReference("v", VariantType.INSTANCE);
        Map<Expression, Expression> replaceMap = new HashMap<>();
        Expression replaced = ExpressionUtils.replaceNullAware(v, replaceMap);
        Assertions.assertEquals(v, replaced);
    }

    @Test
    public void testReplaceNullAwareListAllMapped() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot c = new SlotReference("c", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);
        replaceMap.put(c, Literal.of(3));

        List<Expression> input = Arrays.asList(a, c);
        List<Expression> replaced = ExpressionUtils.replaceNullAware(input, replaceMap);

        Assertions.assertNotNull(replaced);
        Assertions.assertEquals(Arrays.asList(b, Literal.of(3)), replaced);
    }

    @Test
    public void testReplaceNullAwareListVariantKeepOriginal() {
        Slot v = new SlotReference("v", VariantType.INSTANCE);
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);

        List<Expression> input = Arrays.asList(v, a);
        List<Expression> replaced = ExpressionUtils.replaceNullAware(input, replaceMap);

        Assertions.assertNotNull(replaced);
        Assertions.assertEquals(Arrays.asList(v, b), replaced);
    }

    @Test
    public void testReplaceNullAwareListPartialMissReturnNull() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot miss = new SlotReference("miss", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);

        List<Expression> input = Arrays.asList(a, miss);
        List<Expression> replaced = ExpressionUtils.replaceNullAware(input, replaceMap);

        Assertions.assertNull(replaced);
    }

    @Test
    public void testReplaceNullAwareSetAllMapped() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot c = new SlotReference("c", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);
        replaceMap.put(c, Literal.of(3));

        Set<Expression> input = new HashSet<>(Arrays.asList(a, c));
        Set<Expression> replaced = ExpressionUtils.replaceNullAware(input, replaceMap);

        Assertions.assertNotNull(replaced);
        Set<Expression> expected = new HashSet<>(Arrays.asList(b, Literal.of(3)));
        Assertions.assertEquals(expected, replaced);
    }

    @Test
    public void testReplaceNullAwareSetPartialMissReturnNull() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot miss = new SlotReference("miss", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        replaceMap.put(a, b);

        Set<Expression> input = new HashSet<>(Arrays.asList(a, miss));
        Set<Expression> replaced = ExpressionUtils.replaceNullAware(input, replaceMap);

        Assertions.assertNull(replaced);
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
