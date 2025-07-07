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
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Extract virtual columns from filter and push down them into olap scan.
 * This rule can extract:
 * 1. Distance functions like L2DistanceApproximate and InnerProductApproximate
 * 2. Common repeated sub-expressions across multiple conjuncts to eliminate redundant computation
 *
 * Example transformation:
 * Before:
 * Project[a, b, c]
 * └── Filter[func(x, y) > 10 AND func(x, y) < 100 AND func(z, w) = func(x, y)]
 *     └── OlapScan[table]
 *
 * After:
 * Project[a, b, c]
 * └── Filter[v_func_1 > 10 AND v_func_1 < 100 AND v_func_2 = v_func_1]
 *     └── OlapScan[table, virtual_columns=[func(x, y) as v_func_1, func(z, w) as v_func_2]]
 *
 * Benefits:
 * - Eliminates redundant computation of repeated expressions
 * - Can leverage vectorization and SIMD optimizations at scan level
 * - Reduces CPU usage in upper operators
 */
public class PushDownVirtualColumnsIntoOlapScan implements RewriteRuleFactory {

    private static final Logger LOG = LogManager.getLogger(PushDownVirtualColumnsIntoOlapScan.class);

    // Configuration constants for sub-expression extraction
    private static final int MIN_OCCURRENCE_COUNT = 2; // Minimum times an expression must appear to be considered
    private static final int MIN_EXPRESSION_DEPTH = 2; // Minimum depth of expression tree to be beneficial
    private static final int MAX_VIRTUAL_COLUMNS = 50; // Maximum number of virtual columns to avoid explosion

    // Logger for debugging
    private static final Logger logger = LogManager.getLogger(PushDownVirtualColumnsIntoOlapScan.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject(logicalFilter(logicalOlapScan()
                        .when(s -> s.getVirtualColumns().isEmpty())))
                        .then(project -> {
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(filter, scan, Optional.of(project));
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN),
                logicalFilter(logicalOlapScan()
                        .when(s -> s.getVirtualColumns().isEmpty()))
                        .then(filter -> {
                            LogicalOlapScan scan = filter.child();
                            return pushDown(filter, scan, Optional.empty());
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN)

        );
    }

    private Plan pushDown(LogicalFilter<LogicalOlapScan> filter, LogicalOlapScan logicalOlapScan,
            Optional<LogicalProject<?>> optionalProject) {
        // 1. extract distance functions (existing functionality)
        // 2. extract repeated sub-expressions from filter conjuncts (new functionality)
        // 3. generate virtual columns and add them to scan
        // 4. replace filter and project

        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        ImmutableList.Builder<NamedExpression> virtualColumnsBuilder = ImmutableList.builder();

        // Step 1: Extract distance functions (preserve existing functionality)
        extractDistanceFunctions(filter, replaceMap, virtualColumnsBuilder);

        // Step 2: Extract repeated sub-expressions
        extractRepeatedSubExpressions(filter, optionalProject, replaceMap, virtualColumnsBuilder);

        if (replaceMap.isEmpty()) {
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("PushDownVirtualColumnsIntoOlapScan: Created {} virtual columns for expressions: {}",
                    replaceMap.size(), replaceMap.keySet());
        }

        // Step 3: Create new scan with virtual columns
        logicalOlapScan = logicalOlapScan.withVirtualColumns(virtualColumnsBuilder.build());

        // Step 4: Replace expressions in filter and project
        Set<Expression> conjuncts = ExpressionUtils.replace(filter.getConjuncts(), replaceMap);
        Plan plan = filter.withConjunctsAndChild(conjuncts, logicalOlapScan);

        if (optionalProject.isPresent()) {
            LogicalProject<?> project = optionalProject.get();
            List<NamedExpression> projections = ExpressionUtils.replace(
                    (List) project.getProjects(), replaceMap);
            plan = project.withProjectsAndChild(projections, plan);
        } else {
            plan = new LogicalProject<>((List) filter.getOutput(), plan);
        }
        return plan;
    }

    /**
     * Extract distance functions from filter conjuncts (existing functionality)
     */
    private void extractDistanceFunctions(LogicalFilter<LogicalOlapScan> filter,
            Map<Expression, Expression> replaceMap,
            ImmutableList.Builder<NamedExpression> virtualColumnsBuilder) {
        for (Expression conjunct : filter.getConjuncts()) {
            Set<Expression> distanceFunctions = conjunct.collect(
                    e -> e instanceof L2DistanceApproximate || e instanceof InnerProductApproximate);
            for (Expression distanceFunction : distanceFunctions) {
                if (replaceMap.containsKey(distanceFunction)) {
                    continue;
                }
                Alias alias = new Alias(distanceFunction);
                replaceMap.put(distanceFunction, alias.toSlot());
                virtualColumnsBuilder.add(alias);
            }
        }
    }

    /**
     * Extract repeated sub-expressions from filter conjuncts and project expressions
     */
    private void extractRepeatedSubExpressions(LogicalFilter<LogicalOlapScan> filter,
            Optional<LogicalProject<?>> optionalProject,
            Map<Expression, Expression> replaceMap,
            ImmutableList.Builder<NamedExpression> virtualColumnsBuilder) {

        // Collect all expressions from filter and project
        Set<Expression> allExpressions = new HashSet<>();
        for (Expression conjunct : filter.getConjuncts()) {
            allExpressions.add(conjunct);
        }
        if (optionalProject.isPresent()) {
            LogicalProject<?> project = optionalProject.get();
            for (NamedExpression projection : project.getProjects()) {
                allExpressions.add(projection);
            }
        }

        // Count occurrences of each sub-expression
        Map<Expression, Integer> expressionCounts = new HashMap<>();

        for (Expression expr : allExpressions) {
            collectSubExpressions(expr, expressionCounts);
        }

        // Find expressions that occur more than once and are beneficial to push down
        // Sort by cost-benefit ratio to prioritize the most beneficial expressions
        expressionCounts.entrySet().stream()
                .filter(entry -> entry.getValue() >= MIN_OCCURRENCE_COUNT)
                .filter(entry -> !replaceMap.containsKey(entry.getKey()))
                .filter(entry -> isBeneficialToPushDown(entry.getKey()))
                .sorted((e1, e2) -> {
                    // Sort by benefit: (occurrence_count - 1) * expression_complexity
                    int benefit1 = (e1.getValue() - 1) * getExpressionComplexity(e1.getKey());
                    int benefit2 = (e2.getValue() - 1) * getExpressionComplexity(e2.getKey());
                    return Integer.compare(benefit2, benefit1); // descending order
                })
                .limit(MAX_VIRTUAL_COLUMNS - replaceMap.size()) // Limit total virtual columns
                .forEach(entry -> {
                    Expression expr = entry.getKey();
                    Alias alias = new Alias(expr);
                    replaceMap.put(expr, alias.toSlot());
                    virtualColumnsBuilder.add(alias);
                });

        // Logging for debugging
        logger.debug("Extracted virtual columns: {}", virtualColumnsBuilder.build());
    }

    /**
     * Recursively collect all sub-expressions and count their occurrences
     */
    private void collectSubExpressions(Expression expr, Map<Expression, Integer> expressionCounts) {
        // Skip simple slots and literals as they don't benefit from being pushed down
        if (expr instanceof Slot || expr.isConstant()) {
            return;
        }

        // Only count expressions that meet minimum complexity requirements
        if (expr.getDepth() >= MIN_EXPRESSION_DEPTH && expr.children().size() > 0) {
            expressionCounts.put(expr, expressionCounts.getOrDefault(expr, 0) + 1);
        }

        // Recursively process children
        for (Expression child : expr.children()) {
            collectSubExpressions(child, expressionCounts);
        }
    }

    /**
     * Determine if an expression is beneficial to push down as a virtual column
     */
    private boolean isBeneficialToPushDown(Expression expr) {
        // Skip simple expressions that don't benefit from being pushed down
        if (expr instanceof Slot || expr.isConstant()) {
            return false;
        }

        // Skip expressions that are already distance functions (handled separately)
        if (expr instanceof L2DistanceApproximate || expr instanceof InnerProductApproximate) {
            return false;
        }

        // Consider expressions with sufficient depth beneficial
        return expr.getDepth() >= MIN_EXPRESSION_DEPTH && expr.children().size() > 0;
    }

    /**
     * Calculate the complexity/cost of an expression for cost-benefit analysis
     */
    private int getExpressionComplexity(Expression expr) {
        // Use expression depth and width as a simple complexity metric
        // More sophisticated metrics could consider function call costs, etc.
        return expr.getDepth() * expr.getWidth();
    }
}
