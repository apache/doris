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
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseToVariant;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TryParseToVariant;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Types every {@code parse_to_variant} / {@code try_parse_to_variant} call that feeds a VARIANT column whose
 * Schema Template declares a DECIMAL or LARGEINT path with that column's type, so the BE parses the JSON text
 * with the template's fields in hand. Only such a template rides to the BE (see {@code VariantType#toThrift}),
 * and a call that feeds any other column is left exactly as it was. A cast after the parse can only relabel a
 * value already read through a binary64, and comes too late for an integer beyond 64 bits, which fails the
 * parse of the whole document.
 *
 * <p>A template applies to the value its column stores and to nothing else. The typing starts from each
 * {@link LogicalOlapTableSink} and follows a templated column's slot down to the alias that computes it:
 * through projects, through the constant rows and the children of a UNION ALL, and through the nodes that hand
 * their child's slots on unchanged (filter, sort, top-n, limit, join, window, CTE anchor); and inside the
 * expression, through a variant to variant cast and the arguments an IF, CASE WHEN, COALESCE, NVL, NULLABLE or
 * NON_NULLABLE can return, as in the short-circuit IF that MERGE INTO wraps around every value it assigns. It
 * reaches every INSERT shape alike, including the fast VALUES planner, which runs no Rewriter, and it never
 * touches a call outside a sink.
 *
 * <p>The call is typed only when the templated column is the one reader of its value. On the way down a slot
 * loses its template wherever anything else names it: a filter, a join or sort key, a condition, a window, a
 * second column of the sink, a second template. So does a slot that reaches a node this rule does not know to
 * hand slots on unchanged: an aggregate, a set operation that compares rows, any other. Such a call stays as it
 * is and the statement reads and stores what it did before, a number through a binary64. Every other reader of
 * a parsed value therefore sees what it always saw, and two reads of one text in a statement still agree.
 *
 * <p>A reader is what names the slot when this rule runs. One that holds a copy of the call instead reads its
 * own untyped call, and the column's call is typed: the statement may have written the call twice, or an
 * earlier rule may have pushed a filter below the alias and substituted it, as subquery unnesting does in a
 * statement that holds a subquery, for a filter that sits directly on the alias's project.
 *
 * <p>A call behind a CTE consumer is not reached (its producer is not below the sink; a CTE inlined by
 * {@link CTEInline} is), and the slots above a typed call keep the plain VARIANT type they were bound with: the
 * template is an ingestion hint on the call's own result type, which the BE reads from the function node, and a
 * variant to variant cast on the way to the sink is a passthrough there.
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
        ValueSlots valueSlots = new ValueSlots();
        for (int i = 0; i < targets.size(); i++) {
            DataType targetType = targets.get(i).getDataType();
            // Only a template the BE reads numbers by. Any other sends nothing, and typing the call for it
            // would change nothing but the call's identity: two copies of one parse would stop being one
            // common sub-expression, and stop matching a materialized view that computes it.
            if (targetType instanceof VariantType && ((VariantType) targetType).readsNumbersFromText()) {
                valueSlots.add(inputs.get(i).getExprId(), (VariantType) targetType);
            }
        }
        // a slot that feeds another column of the sink as well has that column for a second reader
        Map<ExprId, VariantType> templates = valueSlots.withNoOtherReaderAmong(inputs);
        if (templates.isEmpty()) {
            return sink;
        }
        Plan child = typeCalls(sink.child(), templates);
        return child == sink.child() ? sink : sink.withChildren(ImmutableList.of(child));
    }

    /**
     * {@code templates}: by ExprId, the type each of those output slots of the plan has to be computed with.
     * Only a slot whose one reader above is its templated column is among them.
     */
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
        if (plan instanceof LogicalUnion && ((LogicalUnion) plan).getQualifier() == Qualifier.ALL) {
            return typeCalls((LogicalUnion) plan, templates);
        }
        if (!handsItsChildrensSlotsOn(plan)) {
            // an aggregate groups by the value, a set operation that removes duplicates compares it, and what
            // any other node does with it is not known here: the calls below stay as they are
            return plan;
        }
        Set<ExprId> read = Sets.newHashSet();
        for (Expression expression : plan.getExpressions()) {
            if (expression.anyMatch(SubqueryExpr.class::isInstance)) {
                // what a subquery reads is not in this expression
                return plan;
            }
            for (Slot slot : expression.getInputSlots()) {
                read.add(slot.getExprId());
            }
        }
        ImmutableList.Builder<Plan> children = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (Plan child : plan.children()) {
            Map<ExprId, VariantType> childTemplates = Maps.newHashMap();
            for (Slot slot : child.getOutput()) {
                VariantType template = templates.get(slot.getExprId());
                if (template != null && !read.contains(slot.getExprId())) {
                    childTemplates.put(slot.getExprId(), template);
                }
            }
            Plan typed = childTemplates.isEmpty() ? child : typeCalls(child, childTemplates);
            changed |= typed != child;
            children.add(typed);
        }
        return changed ? plan.withChildren(children.build()) : plan;
    }

    /** A UNION ALL's outputs map to its children's outputs and to its constant rows by position, unread. */
    private static Plan typeCalls(LogicalUnion union, Map<ExprId, VariantType> templates) {
        List<Slot> outputs = union.getOutput();
        boolean changed = false;
        ImmutableList.Builder<Plan> children = ImmutableList.builderWithExpectedSize(union.arity());
        for (int childIndex = 0; childIndex < union.arity(); childIndex++) {
            List<SlotReference> childOutput = union.getRegularChildOutput(childIndex);
            ValueSlots valueSlots = new ValueSlots();
            for (int i = 0; i < outputs.size(); i++) {
                VariantType template = templates.get(outputs.get(i).getExprId());
                if (template != null) {
                    valueSlots.add(childOutput.get(i).getExprId(), template);
                }
            }
            // a child's slot at two positions of the union feeds two of its columns
            Map<ExprId, VariantType> childTemplates = valueSlots.withNoOtherReaderAmong(childOutput);
            Plan child = union.child(childIndex);
            Plan typed = childTemplates.isEmpty() ? child : typeCalls(child, childTemplates);
            changed |= typed != child;
            children.add(typed);
        }
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
     * Types the calls among {@code expressions} that compute a templated slot. A slot the value is taken from,
     * bare or under a cast or an argument that can be returned, carries its template on to
     * {@code childTemplates} when nothing else among {@code expressions} reads it.
     */
    private static List<NamedExpression> typeCalls(List<NamedExpression> expressions,
            Map<ExprId, VariantType> templates, Map<ExprId, VariantType> childTemplates) {
        ImmutableList.Builder<NamedExpression> typed = ImmutableList.builderWithExpectedSize(expressions.size());
        ValueSlots valueSlots = new ValueSlots();
        boolean changed = false;
        for (NamedExpression expression : expressions) {
            NamedExpression result = expression;
            VariantType template = templates.get(expression.getExprId());
            if (template != null) {
                if (expression instanceof Alias) {
                    Expression child = typeCall(expression.child(0), template, valueSlots);
                    if (child != expression.child(0)) {
                        result = ((Alias) expression).withChildren(ImmutableList.of(child));
                    }
                } else if (expression instanceof Slot) {
                    valueSlots.add(expression.getExprId(), template);
                }
            }
            changed |= result != expression;
            typed.add(result);
        }
        childTemplates.putAll(valueSlots.withNoOtherReaderAmong(expressions));
        return changed ? typed.build() : expressions;
    }

    /**
     * Whether {@code plan} hands each slot of its children on as it is, so that a slot none of its expressions
     * names passes it unread. The list is closed on purpose: a node not on it keeps the calls below untyped.
     */
    private static boolean handsItsChildrensSlotsOn(Plan plan) {
        return plan instanceof LogicalFilter || plan instanceof LogicalSort || plan instanceof LogicalTopN
                || plan instanceof LogicalLimit || plan instanceof LogicalJoin || plan instanceof LogicalWindow
                || plan instanceof LogicalCTEAnchor;
    }

    /** The call typed with {@code template}, or {@code expression} itself when it does not compute one. */
    private static Expression typeCall(Expression expression, VariantType template, ValueSlots valueSlots) {
        if (expression instanceof Cast && expression.getDataType() instanceof VariantType
                && expression.child(0).getDataType() instanceof VariantType) {
            // the sink's coercion, or a cast the statement wrote: variant to variant is a relabel, look through
            Expression child = typeCall(expression.child(0), template, valueSlots);
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
            valueSlots.add(((Slot) expression).getExprId(), template);
            return expression;
        }
        if (expression.getDataType() instanceof VariantType && returnsAnArgument(expression)) {
            return typeValueArguments(expression, template, valueSlots);
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
            ValueSlots valueSlots) {
        List<Expression> children = expression.children();
        ImmutableList.Builder<Expression> typed = ImmutableList.builderWithExpectedSize(children.size());
        boolean changed = false;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            Expression result = child;
            if (child instanceof WhenClause) {
                WhenClause clause = (WhenClause) child;
                Expression value = typeArgument(clause.getResult(), template, valueSlots);
                if (value != clause.getResult()) {
                    result = clause.withChildren(ImmutableList.of(clause.getOperand(), value));
                }
            } else if (expression instanceof If) {
                // the first argument is the condition
                if (i > 0) {
                    result = typeArgument(child, template, valueSlots);
                }
            } else if (!(expression instanceof CaseWhen) || i > 0) {
                // every argument of the others; of CASE WHEN, the ELSE value after its WHEN clauses
                result = typeArgument(child, template, valueSlots);
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
    private static Expression typeArgument(Expression argument, VariantType template, ValueSlots valueSlots) {
        Expression typed = typeCall(argument, template, valueSlots);
        if (typed == argument || typed.getDataType().equals(argument.getDataType())) {
            return typed;
        }
        return new Cast(typed, argument.getDataType());
    }

    /**
     * The slots a templated column takes its value from at one node: each with its template and with the number
     * of places the value is taken from it.
     */
    private static final class ValueSlots {
        private final Map<ExprId, VariantType> templates = Maps.newLinkedHashMap();
        private final Map<ExprId, Integer> valueReads = Maps.newHashMap();
        private final Set<ExprId> withTwoTemplates = Sets.newHashSet();

        void add(ExprId slot, VariantType template) {
            VariantType first = templates.putIfAbsent(slot, template);
            if (first != null && !first.equalsWithPatternTypes(template)) {
                withTwoTemplates.add(slot);
            }
            valueReads.merge(slot, 1, Integer::sum);
        }

        /**
         * The slots nothing else among {@code expressions} reads, with their templates. A slot is read once for
         * each place it is named, so one named anywhere but where a templated column takes its value has another
         * reader: a condition, another column, an expression over it. Its call is left as it is, so that this
         * reader sees what it always saw. So is the call two columns would type with different templates.
         */
        Map<ExprId, VariantType> withNoOtherReaderAmong(List<? extends Expression> expressions) {
            Map<ExprId, VariantType> sole = Maps.newHashMap();
            if (templates.isEmpty()) {
                return sole;
            }
            Map<ExprId, Integer> reads = Maps.newHashMap();
            for (Expression expression : expressions) {
                if (expression.anyMatch(SubqueryExpr.class::isInstance)) {
                    // what a subquery reads is not in this expression
                    return sole;
                }
                expression.foreach(node -> {
                    if (node instanceof Slot) {
                        reads.merge(((Slot) node).getExprId(), 1, Integer::sum);
                    }
                });
            }
            templates.forEach((slot, template) -> {
                if (!withTwoTemplates.contains(slot) && valueReads.get(slot).equals(reads.get(slot))) {
                    sole.put(slot, template);
                }
            });
            return sole;
        }
    }
}
