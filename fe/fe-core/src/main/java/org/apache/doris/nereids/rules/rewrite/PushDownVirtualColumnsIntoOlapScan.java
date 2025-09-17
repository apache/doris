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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IsIpAddressInRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MultiMatch;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MultiMatchAny;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
 * 1. Common repeated sub-expressions across multiple conjuncts to eliminate redundant computation
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
 *
 * BLACKLIST STRATEGY:
 * To avoid reverse optimization (preventing more important optimizations), this rule implements
 * a blacklist strategy that skips certain types of expressions:
 *
 * 1. Index Pushdown Functions: Functions like is_ip_address_in_range(), multi_match(), match_*
 *    can be pushed down to storage engine as index operations. Virtual column optimization would
 *    prevent this index pushdown optimization.
 *
 * 2. ColumnPredicate Expressions: Comparison predicates (>, <, =, IN, IS NULL) can be converted
 *    to ColumnPredicate objects for efficient filtering in BE. Virtual columns would lose this
 *    optimization opportunity.
 *
 * 3. CAST Expressions: CAST operations are lightweight and creating virtual columns for them
 *    may not provide significant benefit while adding complexity.
 *
 * 4. Lambda-containing Expressions: Expression trees that contain lambda functions anywhere
 *    are completely skipped from optimization. Lambda functions have complex evaluation contexts
 *    that make virtual column optimization problematic.
 */
public class PushDownVirtualColumnsIntoOlapScan implements RewriteRuleFactory {

    private static final Logger LOG = LogManager.getLogger(PushDownVirtualColumnsIntoOlapScan.class);

    // Configuration constants for sub-expression extraction
    private static final int MIN_OCCURRENCE_COUNT = 2; // Minimum times an expression must appear to be considered
    private static final int MIN_EXPRESSION_DEPTH = 2; // Minimum depth of expression tree to be beneficial
    private static final int MAX_VIRTUAL_COLUMNS = 5; // Maximum number of virtual columns to avoid explosion

    // Logger for debugging
    private static final Logger logger = LogManager.getLogger(PushDownVirtualColumnsIntoOlapScan.class);

    // Supported data types for virtual columns based on TabletColumn::get_field_length_by_type
    // Using Nereids DataType instead of TPrimitiveType for better type safety
    private static final ImmutableSet<Class<? extends DataType>> SUPPORTED_VIRTUAL_COLUMN_TYPES = ImmutableSet.of(
            TinyIntType.class,
            BooleanType.class,
            SmallIntType.class,
            IntegerType.class,
            BigIntType.class,
            LargeIntType.class,
            IPv4Type.class,
            IPv6Type.class,
            DateType.class,
            DateV2Type.class,
            DateTimeType.class,
            DateTimeV2Type.class,
            FloatType.class,
            DoubleType.class,
            CharType.class,
            VarcharType.class,
            StringType.class,
            DecimalV2Type.class,
            DecimalV3Type.class
    );

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject(logicalFilter(logicalOlapScan()
                        .when(s -> {
                            boolean dupTblOrMOW = s.getTable().getKeysType() == KeysType.DUP_KEYS
                                    || s.getTable().getTableProperty().getEnableUniqueKeyMergeOnWrite();
                            return dupTblOrMOW && s.getVirtualColumns().isEmpty();
                        })))
                        .then(project -> {
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(filter, scan, Optional.of(project));
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN),
                logicalFilter(logicalOlapScan()
                        .when(s -> {
                            boolean dupTblOrMOW = s.getTable().getKeysType() == KeysType.DUP_KEYS
                                    || s.getTable().getTableProperty().getEnableUniqueKeyMergeOnWrite();
                            return dupTblOrMOW && s.getVirtualColumns().isEmpty();
                        }))
                        .then(filter -> {
                            LogicalOlapScan scan = filter.child();
                            return pushDown(filter, scan, Optional.empty());
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN)
        );
    }

    private Plan pushDown(LogicalFilter<LogicalOlapScan> filter, LogicalOlapScan logicalOlapScan,
            Optional<LogicalProject<?>> optionalProject) {
        // 1. extract repeated sub-expressions from filter conjuncts
        // 2. generate virtual columns
        // 3. replace filter and project
        // 4. add useful virtual columns to scan

        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        Map<Expression, NamedExpression> virtualColumnsMap = Maps.newHashMap();

        // Extract repeated sub-expressions
        extractRepeatedSubExpressions(filter, optionalProject, replaceMap, virtualColumnsMap);

        if (replaceMap.isEmpty()) {
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("PushDownVirtualColumnsIntoOlapScan: Created {} virtual columns for expressions: {}",
                    replaceMap.size(), replaceMap.keySet());
        }

        // Replace expressions in filter and project
        Map<Expression, Integer> counterMap = Maps.newHashMap();
        Set<Expression> conjuncts = ExpressionUtils.replaceWithCounter(filter.getConjuncts(), replaceMap, counterMap);
        List<NamedExpression> projections = null;
        if (optionalProject.isPresent()) {
            LogicalProject<?> project = optionalProject.get();
            projections = ExpressionUtils.replaceWithCounter(
                    (List) project.getProjects(), replaceMap, counterMap);
        }

        // generate a map that only contains the expression really used in conjuncts and projections
        Map<Expression, Expression> realReplacedMap = Maps.newHashMap();
        for (Map.Entry<Expression, Integer> entry : counterMap.entrySet()) {
            realReplacedMap.put(entry.getKey(), replaceMap.get(entry.getKey()));
        }
        // use replace map to replace virtual column expression
        for (Map.Entry<Expression, NamedExpression> entry : virtualColumnsMap.entrySet()) {
            Expression value = entry.getValue();
            NamedExpression afterReplacement = (NamedExpression) ExpressionUtils.replaceIf(
                    value, replaceMap, e -> !e.equals(value.child(0)), false);
            if (afterReplacement != value) {
                virtualColumnsMap.put(entry.getKey(), afterReplacement);
            }
        }

        // replace virtual columns with other virtual columns
        ImmutableList.Builder<NamedExpression> virtualColumnsBuilder = ImmutableList.builder();
        for (Map.Entry<Expression, Expression> entry : replaceMap.entrySet()) {
            virtualColumnsBuilder.add(virtualColumnsMap.get(entry.getKey()));
        }

        logicalOlapScan = logicalOlapScan.withVirtualColumns(virtualColumnsBuilder.build());
        Plan plan = filter.withConjunctsAndChild(conjuncts, logicalOlapScan);
        if (optionalProject.isPresent()) {
            LogicalProject<?> project = optionalProject.get();
            plan = project.withProjectsAndChild(projections, plan);
        } else {
            plan = new LogicalProject<>((List) filter.getOutput(), plan);
        }
        return plan;
    }

    /**
     * Extract repeated sub-expressions from filter conjuncts and project expressions
     */
    private void extractRepeatedSubExpressions(LogicalFilter<LogicalOlapScan> filter,
            Optional<LogicalProject<?>> optionalProject,
            Map<Expression, Expression> replaceMap,
            Map<Expression, NamedExpression> virtualColumnsMap) {

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
            // Skip expressions that contain lambda functions anywhere in the tree
            if (expr.anyMatch(e -> e instanceof Lambda)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping expression containing lambda: {}", expr.toSql());
                }
                continue;
            }
            collectSubExpressions(expr, expressionCounts);
        }

        // Find expressions that occur more than once and are beneficial to push down
        // Sort by cost-benefit ratio to prioritize the most beneficial expressions
        expressionCounts.entrySet().stream()
                .filter(entry -> entry.getValue() >= MIN_OCCURRENCE_COUNT)
                .filter(entry -> !replaceMap.containsKey(entry.getKey()))
                .filter(entry -> isSupportedVirtualColumnType(entry.getKey())) // Check virtual column type support
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
                    virtualColumnsMap.put(expr, alias);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Created virtual column for expression: {} with type: {}",
                                 expr.toSql(), expr.getDataType().simpleString());
                    }
                });

        // Logging for debugging
        if (LOG.isDebugEnabled()) {
            logger.debug("Extracted virtual columns: {}", virtualColumnsMap.values());
        }
    }

    /**
     * Recursively collect all sub-expressions and count their occurrences
     */
    private void collectSubExpressions(Expression expr, Map<Expression, Integer> expressionCounts) {
        // Check if we should skip this expression and how to handle it
        SkipResult skipResult = shouldSkipExpression(expr);

        if (skipResult.shouldTerminate()) {
            // Examples: x (slot), 10 (constant)
            // These expressions are completely skipped - no counting, no recursion
            return;
        }

        // CONTINUE case: Examples like x + y, func(a, b), (x + y) * z
        // Only count expressions that meet minimum complexity requirements
        if (!(skipResult.shouldSkipCounting() || skipResult.isNotBeneficial())) {
            if (expr.getDepth() >= MIN_EXPRESSION_DEPTH
                    && expr.children().size() > 0
                    && !ExpressionUtils.containUniqueFunctionExistMultiple(ImmutableList.of(expr))) {
                expressionCounts.put(expr, expressionCounts.getOrDefault(expr, 0) + 1);
            }
        }

        // if the Expression has been collected, we do not collect it's children again
        if (expressionCounts.getOrDefault(expr, 0) > 1) {
            return;
        }

        // Recursively process children
        for (Expression child : expr.children()) {
            collectSubExpressions(child, expressionCounts);
        }
    }

    /**
     * Determine how to handle an expression during sub-expression collection
     * This method consolidates ALL skip logic in one place
     * @param expr the expression to check
     * @return SkipResult indicating how to handle this expression
     */
    private SkipResult shouldSkipExpression(Expression expr) {
        // Skip simple slots and literals as they don't benefit from being pushed down
        if (expr instanceof Slot || expr.isConstant()) {
            return SkipResult.TERMINATE;
        }

        // Skip CAST and WhenClause expressions - they shouldn't be optimized as common sub-expressions
        // but we still need to process their children
        if (expr instanceof Cast || expr instanceof WhenClause) {
            return SkipResult.SKIP_COUNTING;
        }

        // Skip expressions with decode_as_varchar or encode_as_bigint as root
        if (expr instanceof DecodeAsVarchar || expr instanceof EncodeString) {
            return SkipResult.SKIP_NOT_BENEFICIAL;
        }

        // Skip expressions that can be converted to ColumnPredicate or can use index
        // This is the key blacklist logic to avoid reverse optimization
        if (canConvertToColumnPredicate(expr) || containsIndexPushdownFunction(expr)) {
            return SkipResult.SKIP_NOT_BENEFICIAL;
        }

        // Continue normal processing
        return SkipResult.CONTINUE;
    }

    /**
     * Result type for expression skip decisions
     */
    private enum SkipResult {
        // Process normally (count and recurse)
        // Examples: x + y, func(a, b), (x + y) * z - beneficial arithmetic/function expressions
        CONTINUE,

        // Skip counting but continue processing children (for CAST expressions)
        // Examples: CAST(x AS VARCHAR), CAST(date_col AS STRING)
        // We don't optimize CAST itself but may optimize its children
        SKIP_COUNTING,

        // Skip counting but continue processing children (expressions not beneficial for optimization)
        // Examples:
        //   - encode_as_bigint(x), decode_as_varchar(x) - encoding/decoding functions
        //   - x > 10, x IN (1,2,3) - ColumnPredicate convertible expressions
        //   - is_ip_address_in_range(ip, '192.168.1.0/24') - index pushdown functions
        SKIP_NOT_BENEFICIAL,

        // Stop processing entirely (don't count, don't recurse)
        // Examples: x (slot), 10 (constant)
        TERMINATE;

        public boolean shouldTerminate() {
            return this == TERMINATE;
        }

        public boolean shouldSkipCounting() {
            return this == SKIP_COUNTING;
        }

        public boolean isNotBeneficial() {
            return this == SKIP_NOT_BENEFICIAL;
        }
    }

    /**
     * Calculate the complexity/cost of an expression for cost-benefit analysis
     */
    private int getExpressionComplexity(Expression expr) {
        // Use expression depth and width as a simple complexity metric
        // More sophisticated metrics could consider function call costs, etc.
        return expr.getDepth() * expr.getWidth();
    }

    /**
     * Check if an expression can be converted to a ColumnPredicate
     * ColumnPredicate types include: EQ, NE, LT, LE, GT, GE, IN_LIST, NOT_IN_LIST, IS_NULL, IS_NOT_NULL, etc.
     */
    private boolean canConvertToColumnPredicate(Expression expr) {
        // Basic comparison predicates that can be converted to ColumnPredicate
        if (expr instanceof ComparisonPredicate) {
            // EQ, NE, LT, LE, GT, GE
            return true;
        }

        // IN and NOT IN predicates
        if (expr instanceof InPredicate) {
            return true;
        }

        // IS NULL and IS NOT NULL predicates
        if (expr instanceof IsNull) {
            return true;
        }

        // Note: Other predicates like LIKE, MATCH, etc. might also be convertible
        // but they are handled separately in containsIndexPushdownFunction
        return false;
    }

    /**
     * Check if an expression contains functions that can be pushed down to index
     */
    private boolean containsIndexPushdownFunction(Expression expr) {
        return expr.anyMatch(node -> isIndexPushdownFunction((Expression) node));
    }

    /**
     * Check if a single expression is an index pushdown function
     */
    private boolean isIndexPushdownFunction(Expression expr) {
        // Functions that implement evaluate_inverted_index and can be pushed down to index

        // IP address range functions
        if (expr instanceof IsIpAddressInRange) {
            return true;
        }

        // Multi-match functions or Match predicate
        if (expr instanceof MultiMatch || expr instanceof MultiMatchAny || expr instanceof Match) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if an expression's return type is supported for virtual columns
     * Based on TabletColumn::get_field_length_by_type implementation
     */
    private boolean isSupportedVirtualColumnType(Expression expr) {
        try {
            DataType dataType = expr.getDataType();
            Class<? extends DataType> typeClass = dataType.getClass();

            boolean isSupported = SUPPORTED_VIRTUAL_COLUMN_TYPES.contains(typeClass);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Expression {} has type {} (class: {}), supported: {}",
                         expr.toSql(), dataType.simpleString(), typeClass.getSimpleName(), isSupported);
            }

            return isSupported;
        } catch (Exception e) {
            // If we can't determine the type, it's safer to not create virtual column
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to determine type for expression {}: {}", expr.toSql(), e.getMessage());
            }
            return false;
        }
    }

    /**
     * Get function name from expression if it's a function call
     */
    private String getFunctionName(Expression expr) {
        // Try to get function name from expression
        // This is a simplified approach - in practice, you might need more robust name extraction
        if (expr instanceof NamedExpression) {
            return ((NamedExpression) expr).getName();
        }
        return null;
    }
}
