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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.paimon.PaimonRowChangeOperation;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.PaimonRowChangeSpec;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Builds a Paimon changelog projection against the current write target. */
final class PaimonRowChangePlanBuilder {
    private PaimonRowChangePlanBuilder() {
    }

    static LogicalProject<?> build(
            PaimonWriteTarget target, PaimonRowChangeSpec spec, LogicalPlan child,
            CascadesContext cascadesContext) {
        PaimonRowChangeCapabilities.check(target, spec, cascadesContext);
        if (spec instanceof PaimonRowChangeSpec.Update) {
            return buildUpdate(target, (PaimonRowChangeSpec.Update) spec,
                    child, cascadesContext);
        }
        if (spec instanceof PaimonRowChangeSpec.Delete) {
            return buildDelete(target, (PaimonRowChangeSpec.Delete) spec,
                    child, cascadesContext);
        }
        if (spec instanceof PaimonRowChangeSpec.Merge) {
            return PaimonMergePlanner.build(target, (PaimonRowChangeSpec.Merge) spec,
                    child, cascadesContext);
        }
        throw new AnalysisException("Unsupported Paimon row-change specification: "
                + spec.getClass().getSimpleName());
    }

    private static LogicalProject<?> buildUpdate(PaimonWriteTarget target,
            PaimonRowChangeSpec.Update update, LogicalPlan child,
            CascadesContext cascadesContext) {
        Map<String, Expression> changes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : update.getAssignments()) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            String columnName = parts.get(parts.size() - 1);
            if (changes.put(columnName, assignment.right()) != null) {
                throw new AnalysisException(
                        "Duplicate column name in Paimon UPDATE: " + columnName);
            }
        }

        ExpressionAnalyzer analyzer = expressionAnalyzer(child, cascadesContext);
        List<NamedExpression> projects = new ArrayList<>();
        projects.add(operation(PaimonRowChangeOperation.UPDATE));
        for (Column column : target.getSchema()) {
            Expression value = changes.remove(column.getName());
            if (value == null) {
                value = targetSlot(update.getTargetNameInPlan(), column.getName());
            }
            projects.add(bindColumn(analyzer, value, column));
        }
        if (!changes.isEmpty()) {
            throw new AnalysisException(
                    "Unknown column in Paimon UPDATE: " + String.join(", ", changes.keySet()));
        }
        return new LogicalProject<>(projects, child);
    }

    private static LogicalProject<?> buildDelete(PaimonWriteTarget target,
            PaimonRowChangeSpec.Delete delete, LogicalPlan child,
            CascadesContext cascadesContext) {
        ExpressionAnalyzer analyzer = expressionAnalyzer(child, cascadesContext);
        List<NamedExpression> projects = new ArrayList<>();
        projects.add(operation(PaimonRowChangeOperation.DELETE));
        for (Column column : target.getSchema()) {
            projects.add(bindColumn(
                    analyzer, targetSlot(delete.getTargetNameInPlan(), column.getName()), column));
        }
        // SQL DELETE changes each target row once even when a USING join matches it repeatedly.
        return new LogicalProject<>(projects, true, child);
    }

    private static UnboundSlot targetSlot(List<String> targetNameInPlan, String columnName) {
        List<String> nameParts = new ArrayList<>(targetNameInPlan);
        nameParts.add(columnName);
        return new UnboundSlot(nameParts);
    }

    private static Alias operation(byte operation) {
        return new Alias(new TinyIntLiteral(operation),
                PaimonRowChangeOperation.OPERATION_COLUMN);
    }

    private static ExpressionAnalyzer expressionAnalyzer(
            LogicalPlan plan, CascadesContext cascadesContext) {
        return new ExpressionAnalyzer(
                plan, new Scope(plan.getOutput()), cascadesContext, true, false);
    }

    private static Alias bindColumn(
            ExpressionAnalyzer analyzer, Expression expression, Column column) {
        Expression value = analyzer.analyze(expression);
        value = TypeCoercionUtils.castIfNotSameType(
                value, DataType.fromCatalogType(column.getType()));
        return new Alias(value, column.getName());
    }
}
