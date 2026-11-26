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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Proves that an ordinal's parameterized SELECT item survives as a real grouping column. */
final class SPMGroupByOrdinalValidator {
    private SPMGroupByOrdinalValidator() {
    }

    /**
     * Proves the rendered grouping-column references and, for a parameterized repeat,
     * the complete grouping-set multiset and ordered GROUPING function arguments.
     * A writer-value replan alone cannot prove these properties (a substituted bare
     * integer might bind to a different SELECT item, or a grand-total set may disappear).
     */
    static boolean preservesTargets(Plan frozen, Plan parameterized, Set<Set<Long>> targets) {
        Set<Set<Long>> remaining = new HashSet<>(targets);
        List<LogicalRepeat<?>> sourceRepeats = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(parameterized, node -> {
            if (node instanceof LogicalRepeat) {
                LogicalRepeat<?> repeat = (LogicalRepeat<?>) node;
                if (repeat.getGroupingSets().stream().flatMap(List::stream)
                        .anyMatch(key -> !sourceGroupKey(repeat, key).ids().isEmpty())) {
                    sourceRepeats.add(repeat);
                }
            }
        });
        if (!sourceRepeats.isEmpty()) {
            Map<LogicalRepeat<?>, List<Expression>> sourceCalls = groupingCallsByRepeat(parameterized);
            List<RepeatContract> requiredRepeats = new ArrayList<>();
            for (LogicalRepeat<?> repeat : sourceRepeats) {
                RepeatContract contract = repeatContract(repeat.getGroupingSets(),
                        sourceCalls.getOrDefault(repeat, List.of()), key -> sourceGroupKey(repeat, key),
                        SPMGroupByOrdinalValidator::sourceKey);
                if (contract == null) {
                    return false;
                }
                requiredRepeats.add(contract);
            }
            Map<LogicalRepeat<?>, List<Expression>> frozenCalls = groupingCallsByRepeat(frozen);
            SPMPlanTreeSupport.<RuntimeException>walkPlans(frozen, node -> {
                if (node instanceof LogicalRepeat && !requiredRepeats.isEmpty()) {
                    LogicalRepeat<?> repeat = (LogicalRepeat<?>) node;
                    RepeatContract contract = frozenRepeatContract(repeat, frozenCalls.getOrDefault(repeat, List.of()));
                    if (contract != null && requiredRepeats.remove(contract)) {
                        remaining.removeAll(contract.targets());
                    }
                }
            });
            if (!requiredRepeats.isEmpty()) {
                return false;
            }
        }
        SPMPlanTreeSupport.<RuntimeException>walkPlans(frozen, node -> {
            if (!remaining.isEmpty() && node instanceof LogicalAggregate) {
                remaining.removeAll(provenTargets((LogicalAggregate<?>) node));
            }
        });
        return remaining.isEmpty();
    }

    private static Set<Set<Long>> provenTargets(LogicalAggregate<?> aggregate) {
        ExportedColumns input = exportedColumns(aggregate.child());
        if (input == null) {
            return Set.of();
        }
        Set<Set<Long>> proven = new HashSet<>();
        for (Expression key : aggregate.getGroupByExpressions()) {
            String name = inputColumn(key, input.alias());
            NamedExpression producer = input.columns().get(name);
            if (producer == null) {
                // A key that would bind as an ordinal/alias rather than an exported input slot.
                return Set.of();
            }
            Set<Long> ids = new HashSet<>();
            collectMarkerIds(producer, ids);
            if (!ids.isEmpty() && selectsSameColumn(aggregate.getOutputExpressions(), input.alias(), name)) {
                proven.add(ids);
            }
        }
        return proven;
    }

    private static RepeatContract frozenRepeatContract(LogicalRepeat<?> repeat, List<Expression> calls) {
        ExportedColumns input = exportedColumns(repeat.child());
        if (input == null) {
            return null;
        }
        Function<Expression, GroupingKey> resolve = key -> {
            String name = inputColumn(key, input.alias());
            NamedExpression producer = input.columns().get(name);
            if (producer == null) {
                return null;
            }
            Set<Long> ids = new HashSet<>();
            collectMarkerIds(producer, ids);
            if (!ids.isEmpty() && !selectsSameColumn(repeat.getOutputExpressions(), input.alias(), name)) {
                return null;
            }
            return groupingKey(valueOf(producer), ids);
        };
        return repeatContract(repeat.getGroupingSets(), calls, resolve, resolve);
    }

    /** A column's marker identity, or the concrete source expression identity for an unparameterized key. */
    private record GroupingKey(Set<Long> ids, String concreteIdentity) {
    }

    /** Argument order matters, in particular GROUPING_ID(a,b) != GROUPING_ID(b,a). */
    private record GroupingCall(String name, List<GroupingKey> arguments) {
    }

    /** Set order is immaterial, but duplicate sets and the empty (grand-total) set must survive. */
    private record RepeatContract(Map<Set<GroupingKey>, Integer> sets, Set<GroupingCall> calls,
            Set<Set<Long>> targets) {
    }

    private static GroupingKey sourceGroupKey(LogicalRepeat<?> repeat, Expression key) {
        Expression target = SPMPlanTreeSupport.groupingOrdinalTarget(key, repeat.getOutputExpressions());
        return sourceKey(target == null ? key : target);
    }

    private static GroupingKey sourceKey(Expression expression) {
        return groupingKey(expression, SPMPlanTreeSupport.collectPlaceholderIds(expression));
    }

    private static GroupingKey groupingKey(Expression expression, Set<Long> ids) {
        return new GroupingKey(Set.copyOf(ids), ids.isEmpty() ? SPMPlanTreeSupport.expressionIdentity(expression) : "");
    }

    private static RepeatContract repeatContract(List<List<Expression>> groupingSets, List<Expression> functions,
            Function<Expression, GroupingKey> resolveKey, Function<Expression, GroupingKey> resolveArgument) {
        Map<Set<GroupingKey>, Integer> sets = new HashMap<>();
        Set<GroupingKey> allKeys = new HashSet<>();
        Set<Set<Long>> targets = new HashSet<>();
        for (List<Expression> groupingSet : groupingSets) {
            Set<GroupingKey> members = new HashSet<>();
            for (Expression expression : groupingSet) {
                GroupingKey key = resolveKey.apply(expression);
                if (key == null) {
                    return null;
                }
                members.add(key);
                allKeys.add(key);
                if (!key.ids().isEmpty()) {
                    targets.add(key.ids());
                }
            }
            sets.merge(Set.copyOf(members), 1, Integer::sum);
        }
        Set<GroupingCall> calls = new HashSet<>();
        for (Expression function : functions) {
            List<GroupingKey> arguments = new ArrayList<>(function.children().size());
            for (Expression argument : function.children()) {
                GroupingKey key = resolveArgument.apply(argument);
                if (key == null || !allKeys.contains(key)) {
                    return null;
                }
                arguments.add(key);
            }
            calls.add(new GroupingCall(SPMPlanTreeSupport.groupingFunctionName(function), arguments));
        }
        return new RepeatContract(sets, calls, targets);
    }

    /**
     * GROUPING calls may also live in HAVING/ORDER BY above the repeat. Collect them
     * in the same query block; physical analysis may export a single virtual grouping
     * output for several occurrences. Never cross a derived-table/ordinary-aggregate boundary.
     */
    private static Map<LogicalRepeat<?>, List<Expression>> groupingCallsByRepeat(Plan root) {
        Map<LogicalRepeat<?>, List<Expression>> calls = new IdentityHashMap<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(root, node -> {
            List<Expression> functions = new ArrayList<>();
            for (Expression expression : node.getExpressions()) {
                collectGroupingCalls(expression, functions);
            }
            if (!functions.isEmpty()) {
                Plan owner = node;
                while (!(owner instanceof LogicalRepeat) && !(owner instanceof LogicalSubQueryAlias)
                        && !(owner instanceof LogicalAggregate) && owner.children().size() == 1) {
                    owner = owner.child(0);
                }
                if (owner instanceof LogicalRepeat) {
                    calls.computeIfAbsent((LogicalRepeat<?>) owner, key -> new ArrayList<>()).addAll(functions);
                }
            }
        });
        return calls;
    }

    private static void collectGroupingCalls(Expression expression, List<Expression> functions) {
        if (SPMPlanTreeSupport.groupingFunctionName(expression) != null) {
            functions.add(expression);
        } else {
            for (Expression child : expression.children()) {
                collectGroupingCalls(child, functions);
            }
        }
    }

    private record ExportedColumns(String alias, Map<String, NamedExpression> columns) {
    }

    private static ExportedColumns exportedColumns(Plan child) {
        if (!(child instanceof LogicalSubQueryAlias)) {
            return null;
        }
        LogicalSubQueryAlias<?> input = (LogicalSubQueryAlias<?>) child;
        if (!(input.child() instanceof LogicalProject) || input.getColumnAliases().isPresent()) {
            return null;
        }
        LogicalProject<?> projection = (LogicalProject<?>) input.child();
        // All exports must have unique names; a wildcard can hide a second column of the same name.
        Map<String, NamedExpression> exports = new HashMap<>();
        for (NamedExpression expression : projection.getProjects()) {
            String name = outputName(expression);
            if (name == null || exports.putIfAbsent(name, expression) != null) {
                return null;
            }
        }
        return new ExportedColumns(input.getAlias(), exports);
    }

    private static Expression valueOf(NamedExpression expression) {
        return expression instanceof UnboundAlias || expression instanceof Alias ? expression.child(0) : expression;
    }

    private static boolean selectsSameColumn(List<NamedExpression> outputs, String inputAlias, String name) {
        boolean found = false;
        for (NamedExpression output : outputs) {
            boolean sameColumn = name.equals(inputColumn(valueOf(output), inputAlias));
            String outputName = outputName(output);
            if (outputName == null || (name.equals(outputName) && !sameColumn)) {
                // An output alias of a different expression must not capture the unqualified group reference.
                return false;
            }
            found |= sameColumn;
        }
        return found;
    }

    private static String inputColumn(Expression expression, String alias) {
        if (!(expression instanceof UnboundSlot)) {
            return null;
        }
        List<String> parts = ((UnboundSlot) expression).getNameParts();
        if (parts.size() == 1 || (parts.size() == 2 && parts.get(0).equalsIgnoreCase(alias))) {
            return parts.get(parts.size() - 1).toLowerCase(Locale.ROOT);
        }
        return null;
    }

    private static String outputName(NamedExpression expression) {
        if (expression instanceof UnboundAlias) {
            return ((UnboundAlias) expression).getAlias().map(name -> name.toLowerCase(Locale.ROOT)).orElse(null);
        }
        if (expression instanceof Alias) {
            return expression.getName().toLowerCase(Locale.ROOT);
        }
        if (expression instanceof UnboundSlot) {
            List<String> parts = ((UnboundSlot) expression).getNameParts();
            return parts.get(parts.size() - 1).toLowerCase(Locale.ROOT);
        }
        return null;
    }

    private static void collectMarkerIds(Expression expression, Set<Long> ids) {
        Long id = SPMFrozenTreeReplacer.frozenPlaceholderId(expression);
        if (id != null) {
            ids.add(id);
        } else {
            for (Expression child : expression.children()) {
                collectMarkerIds(child, ids);
            }
        }
    }
}
