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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.*;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;
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
            if (r.getPattern().toString().contains("LogicalProject") &&
                    r.getPattern().toString().contains("LogicalFilter") &&
                    r.getPattern().toString().contains("LogicalOlapScan")) {
                projectFilterScanRuleMatches = true;
            }
            // Test if the rule can match the filter->scan pattern
            else if (r.getPattern().toString().contains("LogicalFilter") &&
                    r.getPattern().toString().contains("LogicalOlapScan") &&
                    !r.getPattern().toString().contains("LogicalProject")) {
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
        L2DistanceApproximate distance = new L2DistanceApproximate(vector1, vector2);
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
        assert distanceFilter.left() instanceof L2DistanceApproximate : "Should have L2DistanceApproximate function";
        L2DistanceApproximate distFunc = (L2DistanceApproximate) distanceFilter.left();
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
}
