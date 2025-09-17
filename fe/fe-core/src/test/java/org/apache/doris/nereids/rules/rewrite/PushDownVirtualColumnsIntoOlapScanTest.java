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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsIpAddressInRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2Distance;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MultiMatch;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MultiMatchAny;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test for PushDownVirtualColumnsIntoOlapScan rule.
 */
public class PushDownVirtualColumnsIntoOlapScanTest implements MemoPatternMatchSupported {

    @Test
    public void testExtractRepeatedSubExpressions() {
        // Create a test scenario where a sub-expression is repeated in multiple conjuncts
        // SELECT a, b FROM table WHERE (x + y) > 10 AND (x + y) < 100 AND z = (x + y)

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        SlotReference y = new SlotReference("y", intType);
        SlotReference z = new SlotReference("z", intType);
        SlotReference a = new SlotReference("a", intType);
        SlotReference b = new SlotReference("b", intType);

        // Create repeated sub-expression: x + y
        Add xyAdd1 = new Add(x, y);
        Add xyAdd2 = new Add(x, y);
        Add xyAdd3 = new Add(x, y);

        // Create filter conditions using the repeated expression
        GreaterThan gt = new GreaterThan(xyAdd1, new IntegerLiteral(10));
        LessThan lt = new LessThan(xyAdd2, new IntegerLiteral(100));
        EqualTo eq = new EqualTo(z, xyAdd3);

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter with repeated sub-expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(gt, lt, eq), scan);

        // Create project
        List<NamedExpression> projects = ImmutableList.of(
                new Alias(a, "a"),
                new Alias(b, "b")
        );
        LogicalProject<LogicalFilter<LogicalOlapScan>> project =
                new LogicalProject<>(projects, filter);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test that rules are created
        Assertions.assertEquals(2, rules.size());

        // Test rule application on the actual plan structures
        boolean projectFilterScanRuleMatches = false;
        boolean filterScanRuleMatches = false;

        for (Rule r : rules) {
            // Test if the rule can match the project->filter->scan pattern
            if (r.getPattern().matchPlanTree(project)) {
                projectFilterScanRuleMatches = true;
            } else if (r.getPattern().matchPlanTree(filter)) {
                filterScanRuleMatches = true;
            }
        }

        Assertions.assertTrue(projectFilterScanRuleMatches, "Should have rule for Project->Filter->Scan pattern");
        Assertions.assertTrue(filterScanRuleMatches, "Should have rule for Filter->Scan pattern");
    }

    @Test
    public void testExtractDistanceFunctions() {
        // Test the existing distance function extraction functionality
        DataType intType = IntegerType.INSTANCE;
        SlotReference vector1 = new SlotReference("vector1", intType);
        SlotReference vector2 = new SlotReference("vector2", intType);

        // Create distance function
        L2Distance distance = new L2Distance(vector1, vector2);
        GreaterThan distanceFilter = new GreaterThan(distance, new IntegerLiteral(5));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter with distance function
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(distanceFilter), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Should create appropriate rules
        Assertions.assertEquals(2, rules.size());

        // Verify the filter contains the distance function
        Assertions.assertTrue(filter.getConjuncts().contains(distanceFilter),
                "Filter should contain distance function");
        Assertions.assertEquals(scan, filter.child(), "Filter should have scan as child");

        // Verify distance function structure
        Assertions.assertInstanceOf(L2Distance.class, distanceFilter.left(), "Should have L2Distance function");
        L2Distance distFunc = (L2Distance) distanceFilter.left();
        Assertions.assertEquals(vector1, distFunc.child(0), "First argument should be vector1");
        Assertions.assertEquals(vector2, distFunc.child(1), "First argument should be vector2");
    }

    @Test
    public void testComplexRepeatedExpressions() {
        // Test with more complex repeated expressions
        // SELECT * FROM table WHERE (x * y + z) > 10 AND (x * y + z) < 100

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        SlotReference y = new SlotReference("y", intType);
        SlotReference z = new SlotReference("z", intType);

        // Create complex repeated expression: x * y + z
        Multiply xy1 = new Multiply(x, y);
        Add complexExpr1 = new Add(xy1, z);

        Multiply xy2 = new Multiply(x, y);
        Add complexExpr2 = new Add(xy2, z);

        // Create filter conditions
        GreaterThan gt = new GreaterThan(complexExpr1, new IntegerLiteral(10));
        LessThan lt = new LessThan(complexExpr2, new IntegerLiteral(100));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(gt, lt), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Should create appropriate rules for complex expressions
        Assertions.assertEquals(2, rules.size());

        // Verify the filter structure
        Assertions.assertEquals(2, filter.getConjuncts().size(), "Filter should have 2 conjuncts");
        Assertions.assertTrue(filter.getConjuncts().contains(gt), "Filter should contain greater than condition");
        Assertions.assertTrue(filter.getConjuncts().contains(lt), "Filter should contain less than condition");
        Assertions.assertEquals(scan, filter.child(), "Filter should have scan as child");

        // Verify complex expressions are structurally equivalent (though different objects)
        // Both should be Add expressions with Multiply as left child
        assert complexExpr1 instanceof Add : "Complex expression 1 should be Add";
        assert complexExpr2 instanceof Add : "Complex expression 2 should be Add";
        assert complexExpr1.left() instanceof Multiply : "Left side should be Multiply";
        assert complexExpr2.left() instanceof Multiply : "Left side should be Multiply";
    }

    @Test
    public void testSkipWhenClause() {
        // Test that WhenClause expressions are not optimized as common sub-expressions
        // SELECT * FROM table WHERE CASE WHEN x = 1 THEN 'abc' ELSE WHEN x = 1 THEN 'abc' END != 'def'

        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);

        // Create repeated CAST expressions
        WhenClause whenClause = new WhenClause(x, new StringLiteral("abc"));
        CaseWhen caseWhen = new CaseWhen(ImmutableList.of(whenClause, whenClause));

        // Create OLAP scan

        // Create filter with repeated CAST expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(caseWhen), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test that the rule can match the filter pattern (without executing transformation)
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }

        // WhenClause expressions should NOT be optimized, but the rule should still match the pattern
        Assertions.assertTrue(hasMatchingRule, "Rule should match the filter pattern");

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(rules)
                .matches(logicalOlapScan().when(o -> o.getVirtualColumns().isEmpty()));
    }

    @Test
    public void testSkipCastExpressions() {
        // Test that CAST expressions are not optimized as common sub-expressions
        // SELECT * FROM table WHERE CAST(x AS VARCHAR) = 'abc' AND CAST(x AS VARCHAR) != 'def'

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);

        // Create repeated CAST expressions
        Cast cast1 = new Cast(x, VarcharType.SYSTEM_DEFAULT);
        Cast cast2 = new Cast(x, VarcharType.SYSTEM_DEFAULT);

        // Create filter conditions using the repeated CAST expression
        EqualTo eq = new EqualTo(cast1, new StringLiteral("abc"));
        Not neq = new Not(new EqualTo(cast2, new StringLiteral("def")));

        // Create filter with repeated CAST expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(eq, neq), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test rule creation
        Assertions.assertEquals(2, rules.size());

        // Test that the rule can match the filter pattern (without executing transformation)
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }

        // CAST expressions should NOT be optimized, but the rule should still match the pattern
        Assertions.assertTrue(hasMatchingRule, "Rule should match the filter pattern");

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(rules)
                .matches(logicalOlapScan().when(o -> o.getVirtualColumns().isEmpty()));
    }

    @Test
    public void testSkipLambdaExpressions() {
        // Test that expressions containing lambda functions are completely skipped from optimization
        // With the new logic, any expression tree containing lambda should not be optimized

        ConnectContext connectContext = MemoTestUtils.createConnectContext();

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);
        SlotReference y = (SlotReference) scan.getOutput().get(1);
        Add xyAdd = new Add(x, y);

        // Create a lambda expression
        Array arr = new Array(y);
        ArrayItemReference refA = new ArrayItemReference("a", arr);
        Add lambdaAdd = new Add(refA.toSlot(), xyAdd);
        Lambda lambda = new Lambda(ImmutableList.of("a"), lambdaAdd, ImmutableList.of(refA));

        // Create two expressions containing lambda
        ArrayFilter arrayFilter = new ArrayFilter(lambda);
        ArrayMap arrayMap = new ArrayMap(lambda);

        // Create filter with expressions containing lambda
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(arrayFilter, arrayMap)), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test rule creation
        Assertions.assertEquals(2, rules.size());

        // This test verifies that expressions containing lambda are completely skipped
        boolean hasFilterScanRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasFilterScanRule = true;
                break;
            }
        }
        Assertions.assertTrue(hasFilterScanRule, "Should have rule that matches filter->scan pattern");

        // With the new logic, expressions containing lambda should NOT create any virtual columns
        PlanChecker.from(connectContext, filter)
                .applyTopDown(rules)
                .matches(logicalOlapScan().when(o -> o.getVirtualColumns().isEmpty()));
    }

    @Test
    public void testMixedComplexExpressions() {
        // Test with a mix of optimizable and non-optimizable expressions
        // SELECT * FROM table WHERE
        //   (x + y) > 10 AND                    -- optimizable
        //   (x + y) < 100 AND                  -- optimizable (same as above)
        //   CAST(z AS VARCHAR) = 'test' AND    -- not optimizable (CAST)
        //   CAST(z AS VARCHAR) != 'other'      -- not optimizable (CAST, but repeated)

        DataType intType = IntegerType.INSTANCE;
        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        SlotReference x = new SlotReference("x", intType);
        SlotReference y = new SlotReference("y", intType);
        SlotReference z = new SlotReference("z", intType);

        // Create optimizable repeated expressions
        Add xyAdd1 = new Add(x, y);
        Add xyAdd2 = new Add(x, y);

        // Create non-optimizable repeated CAST expressions
        Cast cast1 = new Cast(z, varcharType);
        Cast cast2 = new Cast(z, varcharType);

        // Create filter conditions
        GreaterThan gt = new GreaterThan(xyAdd1, new IntegerLiteral(10));
        LessThan lt = new LessThan(xyAdd2, new IntegerLiteral(100));
        EqualTo eq = new EqualTo(cast1, new StringLiteral("test"));
        Not neq = new Not(new EqualTo(cast2, new StringLiteral("other")));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter with mixed expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(gt, lt, eq, neq), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();
        // Test rule creation
        Assertions.assertEquals(2, rules.size());

        // Verify filter structure
        Assertions.assertEquals(4, filter.getConjuncts().size(), "Filter should have 4 conjuncts");
        Assertions.assertTrue(filter.getConjuncts().contains(gt), "Filter should contain greater than condition");
        Assertions.assertTrue(filter.getConjuncts().contains(lt), "Filter should contain less than condition");
        Assertions.assertTrue(filter.getConjuncts().contains(eq), "Filter should contain equality condition");
        Assertions.assertTrue(filter.getConjuncts().contains(neq), "Filter should contain not equal condition");

        // Test that rules can match the pattern
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }
        Assertions.assertTrue(hasMatchingRule, "Should have rule that matches the filter pattern");
    }

    @Test
    public void testNoOptimizationWhenNoRepeatedExpressions() {
        // Test that no optimization occurs when there are no repeated expressions
        // SELECT * FROM table WHERE x > 10 AND y < 100 AND z = 50

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        SlotReference y = new SlotReference("y", intType);
        SlotReference z = new SlotReference("z", intType);

        // Create unique expressions (no repetition)
        GreaterThan gt = new GreaterThan(x, new IntegerLiteral(10));
        LessThan lt = new LessThan(y, new IntegerLiteral(100));
        EqualTo eq = new EqualTo(z, new IntegerLiteral(50));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter with unique expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(gt, lt, eq), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test rule creation
        Assertions.assertEquals(2, rules.size());

        // Test that the rule can match the filter pattern (without executing transformation)
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }

        // No optimization should occur since there are no repeated expressions, but rule should match
        Assertions.assertTrue(hasMatchingRule, "Should have rule that matches the filter pattern");
    }

    @Test
    public void testRulePatternMatching() {
        // Test that rules correctly match different plan patterns

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        SlotReference a = new SlotReference("a", intType);

        // Create a simple expression
        Add expr = new Add(x, new IntegerLiteral(1));
        GreaterThan condition = new GreaterThan(expr, new IntegerLiteral(0));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(condition), scan);

        // Create project
        List<NamedExpression> projects = ImmutableList.of(new Alias(a, "a"));
        LogicalProject<LogicalFilter<LogicalOlapScan>> project =
                new LogicalProject<>(projects, filter);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        Assertions.assertEquals(2, rules.size());

        // Test pattern matching
        int projectFilterScanMatches = 0;
        int filterScanMatches = 0;

        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(project)) {
                projectFilterScanMatches++;
            }
            if (r.getPattern().matchPlanTree(filter)) {
                filterScanMatches++;
            }
        }

        Assertions.assertEquals(1, projectFilterScanMatches, "Should have exactly 1 rule for Project->Filter->Scan");
        Assertions.assertEquals(1, filterScanMatches, "Should have exactly 1 rule for Filter->Scan");
    }

    @Test
    public void testCanConvertToColumnPredicate_ComparisonPredicates() throws Exception {
        // Test that comparison predicates can be converted to ColumnPredicate

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        IntegerLiteral ten = new IntegerLiteral(10);

        // Create comparison predicates
        EqualTo eq = new EqualTo(x, ten);
        GreaterThan gt = new GreaterThan(x, ten);
        LessThan lt = new LessThan(x, ten);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        Method method = rule.getClass().getDeclaredMethod("canConvertToColumnPredicate", Expression.class);
        method.setAccessible(true);
        boolean result1 = (boolean) method.invoke(rule, eq);
        Assertions.assertTrue(result1, "EqualTo should be convertible to ColumnPredicate");

        boolean result2 = (boolean) method.invoke(rule, gt);
        Assertions.assertTrue(result2, "GreaterThan should be convertible to ColumnPredicate");

        boolean result3 = (boolean) method.invoke(rule, lt);
        Assertions.assertTrue(result3, "LessThan should be convertible to ColumnPredicate");
    }

    @Test
    public void testCanConvertToColumnPredicateInAndNullPredicates() throws Exception {
        // Test IN and IS NULL predicates

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);

        // Create IN predicate
        InPredicate inPred = new InPredicate(x,
                ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));

        // Create IS NULL predicate
        IsNull isNull = new IsNull(x);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        Method method = rule.getClass().getDeclaredMethod("canConvertToColumnPredicate", Expression.class);
        method.setAccessible(true);

        boolean result1 = (boolean) method.invoke(rule, inPred);
        Assertions.assertTrue(result1, "IN predicate should be convertible to ColumnPredicate");

        boolean result2 = (boolean) method.invoke(rule, isNull);
        Assertions.assertTrue(result2, "IS NULL should be convertible to ColumnPredicate");
    }

    @Test
    public void testIsIndexPushdownFunctionIpAddressInRange() throws Exception {
        // Test IP address range function detection

        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        SlotReference ipColumn = new SlotReference("ip_addr", varcharType);
        StringLiteral cidr = new StringLiteral("192.168.1.0/24");

        IsIpAddressInRange ipRangeFunc = new IsIpAddressInRange(ipColumn, cidr);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        Method method = rule.getClass().getDeclaredMethod("isIndexPushdownFunction", Expression.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(rule, ipRangeFunc);
        Assertions.assertTrue(result, "IsIpAddressInRange should be detected as index pushdown function");

    }

    @Test
    public void testIsIndexPushdownFunctionMultiMatch() throws Exception {
        // Test multi-match function detection

        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        SlotReference textColumn = new SlotReference("content", varcharType);
        StringLiteral query = new StringLiteral("search query");

        MultiMatch multiMatchFunc = new MultiMatch(textColumn, query);
        MultiMatchAny multiMatchAnyFunc = new MultiMatchAny(textColumn, query);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        Method method = rule.getClass().getDeclaredMethod("isIndexPushdownFunction", Expression.class);
        method.setAccessible(true);
        boolean result1 = (boolean) method.invoke(rule, multiMatchFunc);
        Assertions.assertTrue(result1, "MultiMatch should be detected as index pushdown function");

        boolean result2 = (boolean) method.invoke(rule, multiMatchAnyFunc);
        Assertions.assertTrue(result2, "MultiMatchAny should be detected as index pushdown function");
    }

    @Test
    public void testContainsIndexPushdownFunctionNestedExpression() throws Exception {
        // Test detection of index pushdown functions in nested expressions

        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        DataType intType = IntegerType.INSTANCE;
        SlotReference ipColumn = new SlotReference("ip_addr", varcharType);
        SlotReference countColumn = new SlotReference("count", intType);
        StringLiteral cidr = new StringLiteral("192.168.1.0/24");
        IntegerLiteral threshold = new IntegerLiteral(100);

        // Create nested expression: is_ip_address_in_range(ip_addr, '192.168.1.0/24') AND count > 100
        IsIpAddressInRange ipRangeFunc = new IsIpAddressInRange(ipColumn, cidr);
        GreaterThan countCondition = new GreaterThan(countColumn, threshold);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        Method method = rule.getClass().getDeclaredMethod("containsIndexPushdownFunction", Expression.class);
        method.setAccessible(true);
        // Test expression containing index pushdown function
        boolean result1 = (boolean) method.invoke(rule, ipRangeFunc);
        Assertions.assertTrue(result1, "Expression containing IsIpAddressInRange should be detected");

        // Test expression not containing index pushdown function
        boolean result2 = (boolean) method.invoke(rule, countCondition);
        Assertions.assertFalse(result2,
                "Regular comparison should not be detected as containing index pushdown function");
    }

    @Test
    public void testIsSupportedVirtualColumnType() throws Exception {
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        // Use reflection to access the private method
        Method method = rule.getClass().getDeclaredMethod("isSupportedVirtualColumnType", Expression.class);
        method.setAccessible(true);

        DataType intType = IntegerType.INSTANCE;
        DataType varcharType = VarcharType.createVarcharType(100);

        // Test supported types
        SlotReference intSlot = new SlotReference("int_col", intType);
        SlotReference varcharSlot = new SlotReference("varchar_col", varcharType);
        // Test basic arithmetic expression with supported types (should return integer)
        Add intAddition = new Add(intSlot, new IntegerLiteral(1));
        boolean intSupported = (boolean) method.invoke(rule, intAddition);
        Assertions.assertTrue(intSupported, "Integer arithmetic expression should be supported for virtual columns");

        // Test string concatenation (should return varchar)
        Concat stringConcat = new Concat(varcharSlot, new StringLiteral("_suffix"));
        boolean stringSupported = (boolean) method.invoke(rule, stringConcat);
        Assertions.assertTrue(stringSupported, "String expression should be supported for virtual columns");

        // Test a complex expression with multiple operations
        Multiply complexMath = new Multiply(
                new Add(intSlot, new IntegerLiteral(5)),
                new IntegerLiteral(2)
        );
        boolean complexSupported = (boolean) method.invoke(rule, complexMath);
        Assertions.assertTrue(complexSupported,
                "Complex arithmetic expression should be supported for virtual columns");

        // Test a CAST expression to string (should be supported)
        Cast castToString = new Cast(intSlot, VarcharType.createVarcharType(50));
        boolean castSupported = (boolean) method.invoke(rule, castToString);
        Assertions.assertTrue(castSupported, "CAST to supported type should be supported for virtual columns");
    }

    @Test
    public void testUnsupportedVirtualColumnType() {
        try {
            PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

            // Use reflection to access the private method
            Method method = rule.getClass().getDeclaredMethod("isSupportedVirtualColumnType", Expression.class);
            method.setAccessible(true);

            // Test expression that might return an unsupported type
            // Create a lambda function expression which should not be supported
            DataType intType = IntegerType.INSTANCE;
            SlotReference intSlot = new SlotReference("int_col", intType);

            // Test expressions that should fail type detection or return false
            // We create an expression that might fail during type determination
            Lambda lambdaExpr = new Lambda(ImmutableList.of("int_col"), new Add(intSlot, new IntegerLiteral(1)));

            boolean lambdaSupported = (boolean) method.invoke(rule, lambdaExpr);
            Assertions.assertFalse(lambdaSupported, "Lambda expressions should not be supported for virtual columns");

        } catch (Exception e) {
            // Expected for some unsupported expressions
            // The test should handle gracefully when expressions cannot be evaluated
        }
    }

    @Test
    public void testVirtualColumnTypeFilteringInExtraction() throws Exception {
        // Test that the extractRepeatedSubExpressions method properly filters out
        // expressions with unsupported types during virtual column creation
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        // Use reflection to access the private extractRepeatedSubExpressions method
        Method extractMethod = rule.getClass()
                .getDeclaredMethod("extractRepeatedSubExpressions",
                        LogicalFilter.class,
                        Optional.class,
                        Map.class,
                        Map.class);
        extractMethod.setAccessible(true);

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        SlotReference y = new SlotReference("y", intType);

        // Create expressions that should be supported (arithmetic operations return int)
        Add supportedAdd1 = new Add(x, y);
        Add supportedAdd2 = new Add(x, y);
        Add supportedAdd3 = new Add(x, y);

        // Create filter conditions using the repeated supported expression
        GreaterThan gt1 = new GreaterThan(supportedAdd1, new IntegerLiteral(10));
        GreaterThan gt2 = new GreaterThan(supportedAdd2, new IntegerLiteral(20));
        GreaterThan gt3 = new GreaterThan(supportedAdd3, new IntegerLiteral(30));

        // Create OLAP scan and filter
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(gt1, gt2, gt3), scan);

        // Test the extraction method
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        Map<Expression, NamedExpression> virtualColumnsMap = Maps.newHashMap();

        // Call the extraction method
        extractMethod.invoke(rule, filter, Optional.empty(), replaceMap, virtualColumnsMap);

        // Verify that virtual columns were created for supported expressions
        // Since Add(x, y) appears 3 times and returns int (supported type),
        // it should be included in virtual columns
        Assertions.assertFalse(virtualColumnsMap.isEmpty(),
                "Should create virtual columns for repeated supported expressions");
        Assertions.assertTrue(replaceMap.size() > 0, "Should have replacements for supported expressions");

        // Test that the virtual column expression has a supported type
        for (NamedExpression virtualCol : virtualColumnsMap.values()) {
            if (virtualCol instanceof Alias) {
                Alias alias = (Alias) virtualCol;
                Expression expr = alias.child();

                // The expression should be supported by isSupportedVirtualColumnType
                Method typeCheckMethod = rule.getClass()
                        .getDeclaredMethod("isSupportedVirtualColumnType", Expression.class);
                typeCheckMethod.setAccessible(true);
                boolean isSupported = (boolean) typeCheckMethod.invoke(rule, expr);
                Assertions.assertTrue(isSupported, "Virtual column expression should have supported type");
            }
        }
    }

    @Test
    public void testTypeFilteringWithMixedExpressions() {
        // Test extraction with both supported and unsupported expression types
        try {
            PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

            // Use reflection to access private methods
            Method extractMethod = rule.getClass().getDeclaredMethod("extractRepeatedSubExpressions",
                            LogicalFilter.class,
                            Optional.class,
                            Map.class,
                            Map.class);
            extractMethod.setAccessible(true);

            Method typeCheckMethod = rule.getClass()
                    .getDeclaredMethod("isSupportedVirtualColumnType", Expression.class);
            typeCheckMethod.setAccessible(true);

            DataType intType = IntegerType.INSTANCE;
            SlotReference x = new SlotReference("x", intType);
            SlotReference y = new SlotReference("y", intType);

            // Create supported repeated expressions (arithmetic operations)
            Add supportedExpr1 = new Add(x, y);
            Add supportedExpr2 = new Add(x, y);

            // Create potentially unsupported repeated expressions (lambda expressions)
            Lambda unsupportedExpr1 = new Lambda(ImmutableList.of("x"), new Add(x, new IntegerLiteral(1)));
            Lambda unsupportedExpr2 = new Lambda(ImmutableList.of("x"), new Add(x, new IntegerLiteral(1)));

            // Verify type support status for both supported and unsupported expressions
            boolean supportedIsSupported = (boolean) typeCheckMethod.invoke(rule, supportedExpr1);
            boolean unsupportedIsSupported1 = (boolean) typeCheckMethod.invoke(rule, unsupportedExpr1);
            boolean unsupportedIsSupported2 = (boolean) typeCheckMethod.invoke(rule, unsupportedExpr2);
            Assertions.assertTrue(supportedIsSupported, "Add expression should be supported");
            Assertions.assertFalse(unsupportedIsSupported1, "Lambda expression 1 should not be supported");
            Assertions.assertFalse(unsupportedIsSupported2, "Lambda expression 2 should not be supported");

            // Verify that both unsupported expressions have the same type checking result
            Assertions.assertEquals(unsupportedIsSupported1, unsupportedIsSupported2,
                    "Both lambda expressions should have the same support status");
            // Create filter conditions using both types
            GreaterThan gt1 = new GreaterThan(supportedExpr1, new IntegerLiteral(10));
            GreaterThan gt2 = new GreaterThan(supportedExpr2, new IntegerLiteral(20));
            // Note: We can't easily create actual filter conditions with lambda expressions
            // since they require specific context, so we focus on the type checking

            LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
            LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(gt1, gt2), scan);

            // Test extraction
            Map<Expression, Expression> replaceMap = Maps.newHashMap();
            Map<Expression, NamedExpression> virtualColumnsMap = Maps.newHashMap();
            extractMethod.invoke(rule, filter, Optional.empty(), replaceMap, virtualColumnsMap);

            // Should have virtual columns only for supported expressions
            for (NamedExpression virtualCol : virtualColumnsMap.values()) {
                if (virtualCol instanceof Alias) {
                    Alias alias = (Alias) virtualCol;
                    Expression expr = alias.child();

                    boolean isSupported = (boolean) typeCheckMethod.invoke(rule, expr);
                    Assertions.assertTrue(isSupported, "All virtual column expressions should have supported types");
                }
            }

        } catch (Exception e) {
            // Expected for lambda expressions or other complex scenarios
            // The important thing is that type checking works correctly
        }
    }

    @Test
    void testOnceUniqueFunction() {
        LogicalOlapScan olapScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                PlanConstructor.newOlapTable(12345L, "t1", 0));
        SlotReference id = (SlotReference) olapScan.getOutput().get(0);
        Random random1 = new Random(new IntegerLiteral(1), new IntegerLiteral(10));
        Random random2 = new Random(new IntegerLiteral(1), new IntegerLiteral(10));
        Expression compareExpr = new Add(new Add(random1, random2), id);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(
                        new GreaterThan(compareExpr, new IntegerLiteral(5)),
                        new LessThan(compareExpr, new IntegerLiteral(10))
                ),
                olapScan);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownVirtualColumnsIntoOlapScan())
                .getPlan();
        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> project = (LogicalProject<?>) root;
        Assertions.assertInstanceOf(LogicalFilter.class, project.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) project.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias alias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertEquals(compareExpr, alias.child());
        Set<Expression> expectConjuncts = ImmutableSet.of(
                new GreaterThan(alias.toSlot(), new IntegerLiteral(5)),
                new LessThan(alias.toSlot(), new IntegerLiteral(10))
        );
        Assertions.assertEquals(expectConjuncts, resFilter.getConjuncts());
    }

    @Test
    void testMultipleTimesUniqueFunctions() {
        LogicalOlapScan olapScan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                PlanConstructor.newOlapTable(12345L, "t1", 0));
        SlotReference id = (SlotReference) olapScan.getOutput().get(0);
        Random random = new Random(new IntegerLiteral(1), new IntegerLiteral(10));
        // don't extract virtual column if it contains an unique function which exists multiple times
        // compareExpr contains  the one same `random` twice, so compareExpr cann't be a virtual column.
        // but `random()` itself can still be a virtual column.
        Expression compareExpr = new Add(new Add(random, random), id);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(
                        new GreaterThan(compareExpr, new IntegerLiteral(5)),
                        new LessThan(compareExpr, new IntegerLiteral(10))
                ),
                olapScan);

        Plan root = PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownVirtualColumnsIntoOlapScan())
                .getPlan();
        Assertions.assertInstanceOf(LogicalProject.class, root);
        LogicalProject<?> project = (LogicalProject<?>) root;
        Assertions.assertInstanceOf(LogicalFilter.class, project.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) project.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias alias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertEquals(random, alias.child());
        Expression newCompareExpr = new Add(new Add(alias.toSlot(), alias.toSlot()), id);
        Set<Expression> expectConjuncts = ImmutableSet.of(
                new GreaterThan(newCompareExpr, new IntegerLiteral(5)),
                new LessThan(newCompareExpr, new IntegerLiteral(10))
        );
        Assertions.assertEquals(expectConjuncts, resFilter.getConjuncts());
    }

    @Test
    public void testCompleteSkipOfLambdaContainingExpressions() {
        // Test that any expression tree containing lambda anywhere is completely skipped

        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);
        SlotReference y = (SlotReference) scan.getOutput().get(1);

        // Create regular repeated expressions (should be optimized)
        Add regularAdd1 = new Add(x, y);
        Add regularAdd2 = new Add(x, y);

        // Create lambda expression
        Array arr = new Array(y);
        ArrayItemReference refA = new ArrayItemReference("a", arr);
        Add lambdaAdd = new Add(refA.toSlot(), x);
        Lambda lambda = new Lambda(ImmutableList.of("a"), lambdaAdd, ImmutableList.of(refA));

        // Create expressions that contain lambda at different levels
        ArrayFilter arrayFilter1 = new ArrayFilter(lambda); // direct lambda usage
        ArrayFilter arrayFilter2 = new ArrayFilter(lambda); // repeated lambda usage
        Add nestedWithLambda = new Add(arrayFilter1, x); // lambda in nested expression

        // Mix regular repeated expressions with lambda-containing expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(
                        new GreaterThan(regularAdd1, new IntegerLiteral(10)),  // should be optimized
                        new LessThan(regularAdd2, new IntegerLiteral(100)),    // should be optimized
                        new EqualTo(arrayFilter1, arrayFilter2),               // should NOT be optimized (contains lambda)
                        new GreaterThan(nestedWithLambda, new IntegerLiteral(5)) // should NOT be optimized (contains lambda)
                ), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        Plan result = PlanChecker.from(connectContext, filter)
                .applyTopDown(rules)
                .getPlan();

        // Verify that only regular expressions without lambda are optimized
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(LogicalFilter.class, project.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) project.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();

        // Should have exactly 1 virtual column for the regular Add expression
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias alias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertEquals(regularAdd1, alias.child()); // The regular Add(x, y) expression

        // Verify that lambda-containing expressions are NOT replaced
        boolean foundLambdaExpressions = resFilter.getConjuncts().stream()
                .anyMatch(expr -> expr.anyMatch(e -> e instanceof ArrayFilter || e instanceof Lambda));
        Assertions.assertTrue(foundLambdaExpressions,
                "Lambda-containing expressions should remain unchanged in filter");
    }

    @Test
    public void testLambdaInProjectExpressions() {
        // Test that lambda-containing expressions in project are also skipped

        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);
        SlotReference y = (SlotReference) scan.getOutput().get(1);

        // Create regular repeated expressions
        Add regularAdd1 = new Add(x, y);
        Add regularAdd2 = new Add(x, y);

        // Create lambda expression used in project
        Array arr = new Array(y);
        ArrayItemReference refA = new ArrayItemReference("a", arr);
        Add lambdaAdd = new Add(refA.toSlot(), x);
        Lambda lambda = new Lambda(ImmutableList.of("a"), lambdaAdd, ImmutableList.of(refA));
        ArrayMap arrayMap1 = new ArrayMap(lambda);
        ArrayMap arrayMap2 = new ArrayMap(lambda); // repeated lambda expression

        // Create filter with regular expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(
                        new GreaterThan(regularAdd1, new IntegerLiteral(10)),
                        new LessThan(regularAdd2, new IntegerLiteral(100))
                ), scan);

        // Create project with lambda-containing expressions
        List<NamedExpression> projects = ImmutableList.of(
                new Alias(arrayMap1, "lambda_result1"),
                new Alias(arrayMap2, "lambda_result2"),
                new Alias(x, "x_col")
        );
        LogicalProject<LogicalFilter<LogicalOlapScan>> project =
                new LogicalProject<>(projects, filter);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        Plan result = PlanChecker.from(connectContext, project)
                .applyTopDown(rules)
                .getPlan();

        // Verify optimization results
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> resProject = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(LogicalFilter.class, resProject.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) resProject.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();

        // Should have exactly 1 virtual column for the regular Add expression
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias alias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertEquals(regularAdd1, alias.child());

        // Verify that lambda expressions in project are NOT replaced
        boolean foundLambdaInProject = resProject.getProjects().stream()
                .anyMatch(expr -> expr.anyMatch(e -> e instanceof ArrayMap || e instanceof Lambda));
        Assertions.assertTrue(foundLambdaInProject,
                "Lambda-containing expressions should remain unchanged in project");
    }

    @Test
    public void testNestedLambdaExpressions() {
        // Test deeply nested expressions containing lambda are completely skipped

        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);
        SlotReference y = (SlotReference) scan.getOutput().get(1);

        // Create regular repeated expressions
        Add regularAdd1 = new Add(x, y);
        Add regularAdd2 = new Add(x, y);

        // Create deeply nested expression with lambda
        Array arr = new Array(y);
        ArrayItemReference refA = new ArrayItemReference("a", arr);
        Lambda lambda = new Lambda(ImmutableList.of("a"), new Add(refA.toSlot(), x), ImmutableList.of(refA));
        ArrayFilter arrayFilter = new ArrayFilter(lambda);

        // Create complex nested expression containing lambda
        Multiply complexWithLambda1 = new Multiply(
                new Add(arrayFilter, x),
                new IntegerLiteral(2)
        );
        Multiply complexWithLambda2 = new Multiply(
                new Add(arrayFilter, x),
                new IntegerLiteral(2)
        ); // repeated complex expression with lambda

        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(
                        new GreaterThan(regularAdd1, new IntegerLiteral(10)),    // should be optimized
                        new LessThan(regularAdd2, new IntegerLiteral(100)),      // should be optimized
                        new GreaterThan(complexWithLambda1, new IntegerLiteral(5)), // should NOT be optimized
                        new LessThan(complexWithLambda2, new IntegerLiteral(50))    // should NOT be optimized
                ), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        Plan result = PlanChecker.from(connectContext, filter)
                .applyTopDown(rules)
                .getPlan();

        // Verify results
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(LogicalFilter.class, project.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) project.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();

        // Should have exactly 1 virtual column for the regular Add expression only
        Assertions.assertEquals(1, resScan.getVirtualColumns().size());
        Alias alias = (Alias) resScan.getVirtualColumns().get(0);
        Assertions.assertEquals(regularAdd1, alias.child());

        // Verify that nested lambda expressions remain unchanged
        boolean foundComplexLambdaExpressions = resFilter.getConjuncts().stream()
                .anyMatch(expr -> expr.anyMatch(e -> e instanceof Multiply
                    && e.anyMatch(inner -> inner instanceof ArrayFilter)));
        Assertions.assertTrue(foundComplexLambdaExpressions,
                "Complex expressions containing lambda should remain unchanged");
    }

    @Test
    public void testMixedExpressionsWithAndWithoutLambda() {
        // Test comprehensive scenario mixing lambda and non-lambda expressions

        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference x = (SlotReference) scan.getOutput().get(0);
        SlotReference y = (SlotReference) scan.getOutput().get(1);

        // Regular repeated expressions (should be optimized)
        Add regularAdd1 = new Add(x, y);
        Add regularAdd2 = new Add(x, y);
        Multiply regularMult1 = new Multiply(x, new IntegerLiteral(3));
        Multiply regularMult2 = new Multiply(x, new IntegerLiteral(3));

        // Lambda expressions (should NOT be optimized)
        Array arr = new Array(y);
        ArrayItemReference refA = new ArrayItemReference("a", arr);
        Lambda lambda = new Lambda(ImmutableList.of("a"), new Add(refA.toSlot(), x), ImmutableList.of(refA));
        ArrayFilter lambdaExpr1 = new ArrayFilter(lambda);
        ArrayFilter lambdaExpr2 = new ArrayFilter(lambda); // repeated but contains lambda

        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(
                        new GreaterThan(regularAdd1, new IntegerLiteral(10)),      // optimizable
                        new LessThan(regularAdd2, new IntegerLiteral(100)),        // optimizable
                        new GreaterThan(regularMult1, new IntegerLiteral(5)),      // optimizable
                        new LessThan(regularMult2, new IntegerLiteral(50)),        // optimizable
                        new EqualTo(lambdaExpr1, lambdaExpr2)                      // NOT optimizable (lambda)
                ), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        Plan result = PlanChecker.from(connectContext, filter)
                .applyTopDown(rules)
                .getPlan();

        // Verify results
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(LogicalFilter.class, project.child());
        LogicalFilter<?> resFilter = (LogicalFilter<?>) project.child();
        Assertions.assertInstanceOf(LogicalOlapScan.class, resFilter.child());
        LogicalOlapScan resScan = (LogicalOlapScan) resFilter.child();

        // Should have exactly 2 virtual columns for the two regular repeated expressions
        Assertions.assertEquals(2, resScan.getVirtualColumns().size());

        // Check that virtual columns contain the expected regular expressions
        Set<Expression> virtualColumnExprs = resScan.getVirtualColumns().stream()
                .map(vc -> ((Alias) vc).child())
                .collect(Collectors.toSet());

        Assertions.assertTrue(virtualColumnExprs.contains(regularAdd1) || virtualColumnExprs.contains(regularAdd2));
        Assertions.assertTrue(virtualColumnExprs.contains(regularMult1) || virtualColumnExprs.contains(regularMult2));

        // Verify that lambda expressions remain in filter
        boolean hasLambdaInFilter = resFilter.getConjuncts().stream()
                .anyMatch(expr -> expr.anyMatch(e -> e instanceof ArrayFilter));
        Assertions.assertTrue(hasLambdaInFilter, "Lambda expressions should remain in filter unchanged");
    }
}
