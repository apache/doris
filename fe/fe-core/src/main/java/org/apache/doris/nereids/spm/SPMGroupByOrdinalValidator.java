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
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Proves that an ordinal's parameterized SELECT item survives as a real grouping column. */
final class SPMGroupByOrdinalValidator {
    private SPMGroupByOrdinalValidator() {
    }

    /**
     * NormalizeAggregate pushes the expression to a child projection and the physical
     * decompiler emits its slot in both SELECT and GROUP BY. Verify the rendered SQL,
     * rather than merely the physical ExprIds: a missing export or an inlined marker
     * might still produce parseable SQL, and replaying a bare integer would bind an
     * ordinal instead of a value. A grouped aggregate must also stay grouped on empty input.
     * Only certify the ordinary derived-table shape; unknown shapes keep the raw plan.
     */
    static boolean preservesTargets(Plan frozen, Set<Set<Long>> targets) {
        Set<Set<Long>> remaining = new HashSet<>(targets);
        SPMPlanTreeSupport.<RuntimeException>walkPlans(frozen, node -> {
            if (!remaining.isEmpty() && node instanceof LogicalAggregate) {
                remaining.removeAll(provenTargets((LogicalAggregate<?>) node));
            }
        });
        return remaining.isEmpty();
    }

    private static Set<Set<Long>> provenTargets(LogicalAggregate<?> aggregate) {
        if (!(aggregate.child() instanceof LogicalSubQueryAlias)) {
            return Set.of();
        }
        LogicalSubQueryAlias<?> input = (LogicalSubQueryAlias<?>) aggregate.child();
        if (!(input.child() instanceof LogicalProject) || input.getColumnAliases().isPresent()) {
            return Set.of();
        }
        LogicalProject<?> projection = (LogicalProject<?>) input.child();
        // All exports must have unique names. In particular a wildcard could hide a
        // second column with the same name, and a textual alias alone is not a slot proof.
        Map<String, NamedExpression> exports = new HashMap<>();
        for (NamedExpression expression : projection.getProjects()) {
            String name = outputName(expression);
            if (name == null || exports.putIfAbsent(name, expression) != null) {
                return Set.of();
            }
        }
        Set<Set<Long>> proven = new HashSet<>();
        for (Expression key : aggregate.getGroupByExpressions()) {
            String name = inputColumn(key, input.getAlias());
            NamedExpression producer = exports.get(name);
            if (producer == null) {
                // A key that would bind as an ordinal/alias rather than an exported
                // input slot is not part of the supported ordinary grouping shape.
                return Set.of();
            }
            Set<Long> ids = new HashSet<>();
            collectMarkerIds(producer, ids);
            if (!ids.isEmpty() && selectsSameColumn(aggregate, input.getAlias(), name)) {
                proven.add(ids);
            }
        }
        return proven;
    }

    private static boolean selectsSameColumn(LogicalAggregate<?> aggregate, String inputAlias, String name) {
        boolean found = false;
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            Expression value = output instanceof UnboundAlias || output instanceof Alias ? output.child(0) : output;
            boolean sameColumn = name.equals(inputColumn(value, inputAlias));
            String outputName = outputName(output);
            if (outputName == null || (name.equals(outputName) && !sameColumn)) {
                // An output alias of a different expression must not capture the
                // unqualified group reference under alias-first binding.
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
