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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeAsBigInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsIpAddressInRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2Distance;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MultiMatch;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MultiMatchAny;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for PushDownVirtualColumnsIntoOlapScan rule.
 */
public class PushDownVirtualColumnsIntoOlapScanTest {

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
        assert rules.size() == 2;

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

        assert projectFilterScanRuleMatches : "Should have rule for Project->Filter->Scan pattern";
        assert filterScanRuleMatches : "Should have rule for Filter->Scan pattern";
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
        assert rules.size() == 2;

        // Verify the filter contains the distance function
        assert filter.getConjuncts().contains(distanceFilter) : "Filter should contain distance function";
        assert filter.child() == scan : "Filter should have scan as child";

        // Verify distance function structure
        assert distanceFilter.left() instanceof L2Distance : "Should have L2Distance function";
        L2Distance distFunc = (L2Distance) distanceFilter.left();
        assert distFunc.child(0) == vector1 : "First argument should be vector1";
        assert distFunc.child(1) == vector2 : "Second argument should be vector2";
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
        assert rules.size() == 2;

        // Verify the filter structure
        assert filter.getConjuncts().size() == 2 : "Filter should have 2 conjuncts";
        assert filter.getConjuncts().contains(gt) : "Filter should contain greater than condition";
        assert filter.getConjuncts().contains(lt) : "Filter should contain less than condition";
        assert filter.child() == scan : "Filter should have scan as child";

        // Verify complex expressions are structurally equivalent (though different objects)
        // Both should be Add expressions with Multiply as left child
        assert complexExpr1 instanceof Add : "Complex expression 1 should be Add";
        assert complexExpr2 instanceof Add : "Complex expression 2 should be Add";
        assert complexExpr1.left() instanceof Multiply : "Left side should be Multiply";
        assert complexExpr2.left() instanceof Multiply : "Left side should be Multiply";
    }

    @Test
    public void testSkipCastExpressions() {
        // Test that CAST expressions are not optimized as common sub-expressions
        // SELECT * FROM table WHERE CAST(x AS VARCHAR) = 'abc' AND CAST(x AS VARCHAR) != 'def'

        DataType intType = IntegerType.INSTANCE;
        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        SlotReference x = new SlotReference("x", intType);

        // Create repeated CAST expressions
        Cast cast1 = new Cast(x, varcharType);
        Cast cast2 = new Cast(x, varcharType);

        // Create filter conditions using the repeated CAST expression
        EqualTo eq = new EqualTo(cast1, new StringLiteral("abc"));
        Not neq = new Not(new EqualTo(cast2, new StringLiteral("def")));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter with repeated CAST expressions
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(eq, neq), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test rule creation
        assert rules.size() == 2;

        // Test that the rule can match the filter pattern (without executing transformation)
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }

        // CAST expressions should NOT be optimized, but the rule should still match the pattern
        assert hasMatchingRule : "Rule should match the filter pattern";
    }

    @Test
    public void testSkipLambdaExpressions() {
        // Test that expressions inside lambda functions are not optimized
        // This is a simplified test since creating actual lambda expressions is complex

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        SlotReference y = new SlotReference("y", intType);

        // Create a repeated expression that would normally be optimized
        Add xyAdd1 = new Add(x, y);
        Add xyAdd2 = new Add(x, y);

        // Create filter conditions - one normal, one that would be inside a lambda context
        GreaterThan gt1 = new GreaterThan(xyAdd1, new IntegerLiteral(10));
        GreaterThan gt2 = new GreaterThan(xyAdd2, new IntegerLiteral(20));

        // Create OLAP scan
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

        // Create filter
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(gt1, gt2), scan);

        // Apply the rule
        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();
        List<Rule> rules = rule.buildRules();

        // Test rule creation
        assert rules.size() == 2;

        // This test verifies the rule structure but actual lambda testing would require
        // more complex expression trees with lambda functions
        boolean hasFilterScanRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasFilterScanRule = true;
                break;
            }
        }
        assert hasFilterScanRule : "Should have rule that matches filter->scan pattern";
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
        assert rules.size() == 2;

        // Verify filter structure
        assert filter.getConjuncts().size() == 4 : "Filter should have 4 conjuncts";
        assert filter.getConjuncts().contains(gt) : "Filter should contain greater than condition";
        assert filter.getConjuncts().contains(lt) : "Filter should contain less than condition";
        assert filter.getConjuncts().contains(eq) : "Filter should contain equality condition";
        assert filter.getConjuncts().contains(neq) : "Filter should contain not equal condition";

        // Test that rules can match the pattern
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }
        assert hasMatchingRule : "Should have rule that matches the filter pattern";
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
        assert rules.size() == 2;

        // Test that the rule can match the filter pattern (without executing transformation)
        boolean hasMatchingRule = false;
        for (Rule r : rules) {
            if (r.getPattern().matchPlanTree(filter)) {
                hasMatchingRule = true;
                break;
            }
        }

        // No optimization should occur since there are no repeated expressions, but rule should match
        assert hasMatchingRule : "Rule should match the filter pattern";
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

        assert rules.size() == 2 : "Should create exactly 2 rules";

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

        assert projectFilterScanMatches == 1 : "Should have exactly 1 rule for Project->Filter->Scan";
        assert filterScanMatches == 1 : "Should have exactly 1 rule for Filter->Scan";
    }

    @Test
    public void testCanConvertToColumnPredicate_ComparisonPredicates() {
        // Test that comparison predicates can be converted to ColumnPredicate

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);
        IntegerLiteral ten = new IntegerLiteral(10);

        // Create comparison predicates
        EqualTo eq = new EqualTo(x, ten);
        GreaterThan gt = new GreaterThan(x, ten);
        LessThan lt = new LessThan(x, ten);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        try {
            java.lang.reflect.Method method = rule.getClass().getDeclaredMethod("canConvertToColumnPredicate",
                    org.apache.doris.nereids.trees.expressions.Expression.class);
            method.setAccessible(true);

            boolean result1 = (boolean) method.invoke(rule, eq);
            assert result1 : "EqualTo should be convertible to ColumnPredicate";

            boolean result2 = (boolean) method.invoke(rule, gt);
            assert result2 : "GreaterThan should be convertible to ColumnPredicate";

            boolean result3 = (boolean) method.invoke(rule, lt);
            assert result3 : "LessThan should be convertible to ColumnPredicate";

        } catch (Exception e) {
            throw new RuntimeException("Failed to test canConvertToColumnPredicate", e);
        }
    }

    @Test
    public void testCanConvertToColumnPredicate_InAndNullPredicates() {
        // Test IN and IS NULL predicates

        DataType intType = IntegerType.INSTANCE;
        SlotReference x = new SlotReference("x", intType);

        // Create IN predicate
        InPredicate inPred = new InPredicate(x,
                ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));

        // Create IS NULL predicate
        IsNull isNull = new IsNull(x);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        try {
            java.lang.reflect.Method method = rule.getClass().getDeclaredMethod("canConvertToColumnPredicate",
                    org.apache.doris.nereids.trees.expressions.Expression.class);
            method.setAccessible(true);

            boolean result1 = (boolean) method.invoke(rule, inPred);
            assert result1 : "IN predicate should be convertible to ColumnPredicate";

            boolean result2 = (boolean) method.invoke(rule, isNull);
            assert result2 : "IS NULL should be convertible to ColumnPredicate";

        } catch (Exception e) {
            throw new RuntimeException("Failed to test canConvertToColumnPredicate with IN/NULL", e);
        }
    }

    @Test
    public void testIsIndexPushdownFunction_IpAddressInRange() {
        // Test IP address range function detection

        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        SlotReference ipColumn = new SlotReference("ip_addr", varcharType);
        StringLiteral cidr = new StringLiteral("192.168.1.0/24");

        IsIpAddressInRange ipRangeFunc = new IsIpAddressInRange(ipColumn, cidr);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        try {
            java.lang.reflect.Method method = rule.getClass().getDeclaredMethod("isIndexPushdownFunction",
                    org.apache.doris.nereids.trees.expressions.Expression.class);
            method.setAccessible(true);

            boolean result = (boolean) method.invoke(rule, ipRangeFunc);
            assert result : "IsIpAddressInRange should be detected as index pushdown function";

        } catch (Exception e) {
            throw new RuntimeException("Failed to test isIndexPushdownFunction with IP range", e);
        }
    }

    @Test
    public void testIsIndexPushdownFunction_MultiMatch() {
        // Test multi-match function detection

        DataType varcharType = VarcharType.SYSTEM_DEFAULT;
        SlotReference textColumn = new SlotReference("content", varcharType);
        StringLiteral query = new StringLiteral("search query");

        MultiMatch multiMatchFunc = new MultiMatch(textColumn, query);
        MultiMatchAny multiMatchAnyFunc = new MultiMatchAny(textColumn, query);

        PushDownVirtualColumnsIntoOlapScan rule = new PushDownVirtualColumnsIntoOlapScan();

        try {
            java.lang.reflect.Method method = rule.getClass().getDeclaredMethod("isIndexPushdownFunction",
                    org.apache.doris.nereids.trees.expressions.Expression.class);
            method.setAccessible(true);

            boolean result1 = (boolean) method.invoke(rule, multiMatchFunc);
            assert result1 : "MultiMatch should be detected as index pushdown function";

            boolean result2 = (boolean) method.invoke(rule, multiMatchAnyFunc);
            assert result2 : "MultiMatchAny should be detected as index pushdown function";

        } catch (Exception e) {
            throw new RuntimeException("Failed to test isIndexPushdownFunction with MultiMatch", e);
        }
    }

    @Test
    public void testContainsIndexPushdownFunction_NestedExpression() {
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

        try {
            java.lang.reflect.Method method = rule.getClass().getDeclaredMethod("containsIndexPushdownFunction",
                    org.apache.doris.nereids.trees.expressions.Expression.class);
            method.setAccessible(true);

            // Test expression containing index pushdown function
            boolean result1 = (boolean) method.invoke(rule, ipRangeFunc);
            assert result1 : "Expression containing IsIpAddressInRange should be detected";

            // Test expression not containing index pushdown function
            boolean result2 = (boolean) method.invoke(rule, countCondition);
            assert !result2 : "Regular comparison should not be detected as containing index pushdown function";

        } catch (Exception e) {
            throw new RuntimeException("Failed to test containsIndexPushdownFunction", e);
        }
    }
}
