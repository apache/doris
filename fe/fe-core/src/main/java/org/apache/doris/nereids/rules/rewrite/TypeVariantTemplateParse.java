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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseToVariant;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TryParseToVariant;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Types every {@code parse_to_variant} / {@code try_parse_to_variant} call that feeds a VARIANT column whose type
 * carries a Schema Template with that column's type, so the BE parses the JSON text with the template's fields in
 * hand. Only a template that declares a DECIMAL or LARGEINT path rides to the BE (see
 * {@code VariantType#toThrift}): a cast after the parse can only relabel a value already read through a binary64,
 * and comes too late for an integer beyond 64 bits, which fails the parse of the whole document.
 *
 * <p>A template applies at the sink and nowhere else, so the typing starts from each {@link LogicalOlapTableSink}
 * and follows a templated column's slot down to the alias that computes it: through projects, the constant rows
 * and the children of a set operation, and every node that passes its child's slots through; and inside the
 * expression, through a variant to variant cast and the arguments an IF, CASE WHEN, COALESCE, NVL, NULLABLE or
 * NON_NULLABLE can return, as in the short-circuit IF that MERGE INTO wraps around every value it assigns. It
 * therefore reaches every INSERT shape alike, including the fast VALUES planner, which runs no Rewriter, and it
 * never touches a call outside a sink.
 *
 * <p>The call is typed where it is computed. Every reader of its slot between the call and the sink, a filter, a
 * join condition or a sort key among them, therefore sees the value the template reads, which is the value the
 * column stores: a DECIMAL carrying the text's digits where a query outside a sink sees a DOUBLE.
 *
 * <p>A call behind a CTE consumer is not reached (its producer is not below the sink; a CTE inlined by
 * {@link CTEInline} is), a slot feeding several templated columns takes the first column's template, and the
 * slots above a typed call keep the plain VARIANT type they were bound with: the template is an ingestion hint
 * on the call's own result type, which the BE reads from the function node, and a variant to variant cast on
 * the way to the sink is a passthrough there.
 */
public class TypeVariantTemplateParse extends DefaultPlanRewriter<Void> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, null);
    }

    @Override
    public Plan visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> sink, Void context) {
        List<Slot> targets = sink.getTargetTableSlots();
        List<Slot> inputs = sink.child().getOutput();
        Map<ExprId, VariantType> templates = Maps.newHashMap();
        for (int i = 0; i < targets.size(); i++) {
            DataType targetType = targets.get(i).getDataType();
            if (targetType instanceof VariantType && !((VariantType) targetType).getPredefinedFields().isEmpty()) {
                templates.putIfAbsent(inputs.get(i).getExprId(), (VariantType) targetType);
            }
        }
        if (templates.isEmpty()) {
            return sink;
        }
        Plan child = typeCalls(sink.child(), templates);
        return child == sink.child() ? sink : sink.withChildren(ImmutableList.of(child));
    }

    /** {@code templates}: by ExprId, the type each of the plan's output slots has to be computed with. */
    private static Plan typeCalls(Plan plan, Map<ExprId, VariantType> templates) {
        if (plan instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            Map<ExprId, VariantType> childTemplates = Maps.newHashMap();
            List<NamedExpression> projects = typeCalls(project.getProjects(), templates, childTemplates);
            Plan child = childTemplates.isEmpty() ? project.child() : typeCalls(project.child(), childTemplates);
            if (projects == project.getProjects() && child == project.child()) {
                return plan;
            }
            return project.withProjectsAndChild(projects, child);
        }
        if (plan instanceof LogicalOneRowRelation) {
            LogicalOneRowRelation oneRowRelation = (LogicalOneRowRelation) plan;
            List<NamedExpression> projects
                    = typeCalls(oneRowRelation.getProjects(), templates, Maps.newHashMap());
            return projects == oneRowRelation.getProjects() ? plan : oneRowRelation.withProjects(projects);
        }
        if (plan instanceof LogicalSetOperation) {
            return typeCalls((LogicalSetOperation) plan, templates);
        }
        // Every other node passes its children's slots through unchanged (filter, sort, limit, join, ...);
        // a leaf, a CTE consumer among them, computes nothing to type.
        ImmutableList.Builder<Plan> children = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (Plan child : plan.children()) {
            Map<ExprId, VariantType> childTemplates = Maps.newHashMap();
            for (Slot slot : child.getOutput()) {
                VariantType template = templates.get(slot.getExprId());
                if (template != null) {
                    childTemplates.put(slot.getExprId(), template);
                }
            }
            Plan typed = childTemplates.isEmpty() ? child : typeCalls(child, childTemplates);
            changed |= typed != child;
            children.add(typed);
        }
        return changed ? plan.withChildren(children.build()) : plan;
    }

    /** A set operation's outputs map to its children's outputs and to its constant rows by position. */
    private static Plan typeCalls(LogicalSetOperation setOperation, Map<ExprId, VariantType> templates) {
        List<Slot> outputs = setOperation.getOutput();
        boolean changed = false;
        ImmutableList.Builder<Plan> children = ImmutableList.builderWithExpectedSize(setOperation.arity());
        for (int childIndex = 0; childIndex < setOperation.arity(); childIndex++) {
            List<SlotReference> childOutput = setOperation.getRegularChildOutput(childIndex);
            Map<ExprId, VariantType> childTemplates = Maps.newHashMap();
            for (int i = 0; i < outputs.size(); i++) {
                VariantType template = templates.get(outputs.get(i).getExprId());
                if (template != null) {
                    childTemplates.putIfAbsent(childOutput.get(i).getExprId(), template);
                }
            }
            Plan child = setOperation.child(childIndex);
            Plan typed = childTemplates.isEmpty() ? child : typeCalls(child, childTemplates);
            changed |= typed != child;
            children.add(typed);
        }
        if (!(setOperation instanceof LogicalUnion)) {
            return changed
                    ? setOperation.withChildrenAndTheirOutputs(children.build(),
                            setOperation.getRegularChildrenOutputs())
                    : setOperation;
        }
        LogicalUnion union = (LogicalUnion) setOperation;
        ImmutableList.Builder<List<NamedExpression>> rows
                = ImmutableList.builderWithExpectedSize(union.getConstantExprsList().size());
        for (List<NamedExpression> row : union.getConstantExprsList()) {
            Map<ExprId, VariantType> rowTemplates = Maps.newHashMap();
            for (int i = 0; i < outputs.size(); i++) {
                VariantType template = templates.get(outputs.get(i).getExprId());
                if (template != null) {
                    rowTemplates.putIfAbsent(row.get(i).getExprId(), template);
                }
            }
            List<NamedExpression> typed = rowTemplates.isEmpty()
                    ? row : typeCalls(row, rowTemplates, Maps.newHashMap());
            changed |= typed != row;
            rows.add(typed);
        }
        return changed
                ? union.withChildrenAndConstExprsList(children.build(), union.getRegularChildrenOutputs(),
                        rows.build())
                : union;
    }

    /**
     * Types the calls among {@code expressions} that compute a templated slot; a slot passed through, bare or
     * under a cast, carries its template on to {@code childTemplates}.
     */
    private static List<NamedExpression> typeCalls(List<NamedExpression> expressions,
            Map<ExprId, VariantType> templates, Map<ExprId, VariantType> childTemplates) {
        ImmutableList.Builder<NamedExpression> typed = ImmutableList.builderWithExpectedSize(expressions.size());
        boolean changed = false;
        for (NamedExpression expression : expressions) {
            NamedExpression result = expression;
            VariantType template = templates.get(expression.getExprId());
            if (template != null) {
                if (expression instanceof Alias) {
                    Expression child = typeCall(expression.child(0), template, childTemplates);
                    if (child != expression.child(0)) {
                        result = ((Alias) expression).withChildren(ImmutableList.of(child));
                    }
                } else if (expression instanceof Slot) {
                    childTemplates.putIfAbsent(expression.getExprId(), template);
                }
            }
            changed |= result != expression;
            typed.add(result);
        }
        return changed ? typed.build() : expressions;
    }

    /** The call typed with {@code template}, or {@code expression} itself when it does not compute one. */
    private static Expression typeCall(Expression expression, VariantType template,
            Map<ExprId, VariantType> childTemplates) {
        if (expression instanceof Cast && expression.getDataType() instanceof VariantType
                && expression.child(0).getDataType() instanceof VariantType) {
            // the sink's coercion, or a cast the statement wrote: variant to variant is a relabel, look through
            Expression child = typeCall(expression.child(0), template, childTemplates);
            if (child == expression.child(0)) {
                return expression;
            }
            return child.getDataType().equals(expression.getDataType())
                    ? child : expression.withChildren(ImmutableList.of(child));
        }
        if (expression instanceof ParseToVariant) {
            ParseToVariant call = (ParseToVariant) expression;
            return call.getReturnType().equalsWithPatternTypes(template) ? call : call.withReturnType(template);
        }
        if (expression instanceof TryParseToVariant) {
            TryParseToVariant call = (TryParseToVariant) expression;
            return call.getReturnType().equalsWithPatternTypes(template) ? call : call.withReturnType(template);
        }
        if (expression instanceof Slot) {
            childTemplates.putIfAbsent(((Slot) expression).getExprId(), template);
            return expression;
        }
        if (expression.getDataType() instanceof VariantType && returnsAnArgument(expression)) {
            return typeValueArguments(expression, template, childTemplates);
        }
        return expression;
    }

    /**
     * Whether {@code expression}'s result is one of its arguments: IF (and the short-circuit IF that MERGE INTO
     * selects every assigned value with), CASE WHEN, COALESCE, NVL, NULLABLE and NON_NULLABLE.
     */
    private static boolean returnsAnArgument(Expression expression) {
        return expression instanceof If || expression instanceof CaseWhen || expression instanceof Coalesce
                || expression instanceof Nvl || expression instanceof Nullable || expression instanceof NonNullable;
    }

    /** Types the calls among the arguments {@code expression} can return; a condition is left as it is. */
    private static Expression typeValueArguments(Expression expression, VariantType template,
            Map<ExprId, VariantType> childTemplates) {
        List<Expression> children = expression.children();
        ImmutableList.Builder<Expression> typed = ImmutableList.builderWithExpectedSize(children.size());
        boolean changed = false;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            Expression result = child;
            if (child instanceof WhenClause) {
                WhenClause clause = (WhenClause) child;
                Expression value = typeArgument(clause.getResult(), template, childTemplates);
                if (value != clause.getResult()) {
                    result = clause.withChildren(ImmutableList.of(clause.getOperand(), value));
                }
            } else if (expression instanceof If) {
                // the first argument is the condition
                if (i > 0) {
                    result = typeArgument(child, template, childTemplates);
                }
            } else if (!(expression instanceof CaseWhen) || i > 0) {
                // every argument of the others; of CASE WHEN, the ELSE value after its WHEN clauses
                result = typeArgument(child, template, childTemplates);
            }
            changed |= result != child;
            typed.add(result);
        }
        return changed ? expression.withChildren(typed.build()) : expression;
    }

    /**
     * {@code argument} with its calls typed, kept at the type the enclosing expression was bound with: the typed
     * call goes under a variant to variant cast, a passthrough on the BE like the sink's own.
     */
    private static Expression typeArgument(Expression argument, VariantType template,
            Map<ExprId, VariantType> childTemplates) {
        Expression typed = typeCall(argument, template, childTemplates);
        if (typed == argument || typed.getDataType().equals(argument.getDataType())) {
            return typed;
        }
        return new Cast(typed, argument.getDataType());
    }
}
