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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StringRegexPredicate;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.RecursiveCte;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWorkTableReference;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Shared source-plan validation for external Variant sinks. */
public final class VariantWritePlanValidator {
    private VariantWritePlanValidator() {
    }

    /**
     * Rejects an implicit Variant-to-non-Variant cast in the lineage of a Variant target column.
     *
     * <p>Common-type analysis for UNION, IF and CASE runs before sink binding. Without this
     * check, an object or array Variant can become SQL NULL while being cast to a scalar, and the
     * sink will only see that scalar/NULL and encode it back as Variant. Explicit casts remain an
     * intentional user conversion and are not rejected. Only the expressions whose values can reach
     * the target are inspected: the condition of an IF or of a CASE branch only chooses a value, so a
     * Variant path compared with a scalar there loses nothing that is written. Likewise the later
     * inputs of EXCEPT and INTERSECT only decide which rows of the first input stay, and the partition
     * and order keys of a window and the ORDER BY of an aggregate function only arrange the rows whose
     * values are used. For the same reason, the second argument of NULLIF, the index or key of
     * element_at and the cast a predicate applies to an operand are not inspected: they are only
     * compared or choose the element returned.</p>
     *
     * <p>Of a value, only the parts that reach a Variant leaf of the target are inspected. The BOOLEAN
     * field of a STRUCT&lt;matched BOOLEAN, payload VARIANT&gt; target is written as a BOOLEAN, so a
     * Variant cast to a scalar while computing it loses nothing in a Variant. Struct, array and map
     * constructors, field and element access, casts and the results of IF, CASE, NULLIF and NVL pass
     * those parts on to their arguments; any other expression inspects all of its arguments.</p>
     */
    public static void validateNoLossyCoercion(List<Column> targetColumns, Plan sourcePlan) {
        validateNoLossyCoercion(targetColumns, sourcePlan, Optional.empty());
    }

    /** Validates a sink while its enclosing CTE producers still live in the analyzer context. */
    public static void validateNoLossyCoercion(
            List<Column> targetColumns, Plan sourcePlan, CTEContext cteContext) {
        validateNoLossyCoercion(targetColumns, sourcePlan, Optional.of(cteContext));
    }

    private static void validateNoLossyCoercion(
            List<Column> targetColumns, Plan sourcePlan, Optional<CTEContext> cteContext) {
        if (targetColumns.size() != sourcePlan.getOutput().size()) {
            throw new AnalysisException("VARIANT write target and source columns are not aligned");
        }
        List<Integer> sourceOrdinals = new ArrayList<>(targetColumns.size());
        for (int i = 0; i < targetColumns.size(); i++) {
            sourceOrdinals.add(i);
        }
        validateNoLossyCoercion(targetColumns, sourcePlan, sourceOrdinals, cteContext);
    }

    /**
     * Validates target columns that are embedded in a wider source output, such as row-level DML
     * where the operation and row-id routing columns precede the table data columns.
     */
    public static void validateNoLossyCoercion(
            List<Column> targetColumns, Plan sourcePlan, List<Integer> sourceOrdinals) {
        validateNoLossyCoercion(targetColumns, sourcePlan, sourceOrdinals, Optional.empty());
    }

    /** Validates mapped source ordinals with analyzer-time CTE producer visibility. */
    public static void validateNoLossyCoercion(
            List<Column> targetColumns, Plan sourcePlan, List<Integer> sourceOrdinals,
            CTEContext cteContext) {
        validateNoLossyCoercion(targetColumns, sourcePlan, sourceOrdinals, Optional.of(cteContext));
    }

    private static void validateNoLossyCoercion(
            List<Column> targetColumns, Plan sourcePlan, List<Integer> sourceOrdinals,
            Optional<CTEContext> cteContext) {
        if (targetColumns.size() != sourceOrdinals.size()) {
            throw new AnalysisException("VARIANT write target and source columns are not aligned");
        }

        Map<Integer, Demand> variantTargetDemands = new LinkedHashMap<>();
        for (int i = 0; i < targetColumns.size(); i++) {
            Demand demand = Demand.of(DataType.fromCatalogType(targetColumns.get(i).getType()));
            if (!demand.isNone()) {
                variantTargetDemands.put(i, demand);
            }
        }
        if (variantTargetDemands.isEmpty()) {
            return;
        }

        TraceContext traceContext = new TraceContext(sourcePlan, cteContext);
        List<Slot> sourceOutputs = sourcePlan.getOutput();
        for (Map.Entry<Integer, Demand> targetDemand : variantTargetDemands.entrySet()) {
            int sourceOrdinal = sourceOrdinals.get(targetDemand.getKey());
            if (sourceOrdinal < 0 || sourceOrdinal >= sourceOutputs.size()) {
                throw new AnalysisException("VARIANT write target and source columns are not aligned");
            }
            Slot sourceOutput = sourceOutputs.get(sourceOrdinal);
            // The sink casts the source value to the target type, field by field in order.
            traceOutputLineage(
                    sourcePlan,
                    Collections.singletonMap(
                            sourceOutput.getExprId(), targetDemand.getValue().alignTo(sourceOutput.getDataType())),
                    targetColumns.get(targetDemand.getKey()).getName(),
                    traceContext);
        }
    }

    /**
     * Validates the required parts of the outputs and returns the parts of the lineage ExprIds that an
     * outer scope supplies. Scalar subqueries use them to continue tracing correlated columns in the
     * enclosing plan.
     */
    private static Map<ExprId, Demand> traceOutputLineage(
            Plan plan, Map<ExprId, Demand> requiredDemands, String targetColumn, TraceContext context) {
        if (requiredDemands.isEmpty()) {
            return Collections.emptyMap();
        }
        if (plan instanceof SetOperation) {
            return traceSetOperation(plan, (SetOperation) plan, requiredDemands, targetColumn, context);
        }
        if (plan instanceof RecursiveCte) {
            return traceRecursiveCte(plan, (RecursiveCte) plan, requiredDemands, targetColumn, context);
        }
        if (plan instanceof LogicalGenerate) {
            return traceGenerate((LogicalGenerate<?>) plan, requiredDemands, targetColumn, context);
        }
        if (plan instanceof LogicalCTEConsumer) {
            return traceCteConsumer((LogicalCTEConsumer) plan, requiredDemands, targetColumn, context);
        }

        Map<ExprId, Demand> unresolvedDemands = new HashMap<>(requiredDemands);
        Map<ExprId, Demand> inputDemands = new HashMap<>();
        for (Expression expression : plan.getExpressions()) {
            if (!(expression instanceof NamedExpression)) {
                continue;
            }
            NamedExpression namedExpression = (NamedExpression) expression;
            Demand demand = requiredDemands.get(namedExpression.getExprId());
            if (demand == null) {
                continue;
            }
            mergeDemands(inputDemands, validateExpression(namedExpression, demand, targetColumn, context));
            unresolvedDemands.remove(namedExpression.getExprId());
        }
        mergeDemands(inputDemands, unresolvedDemands);

        Map<ExprId, Demand> externalDemands = new HashMap<>(inputDemands);
        for (Plan child : plan.children()) {
            Set<ExprId> childOutputExprIds = child.getOutputExprIdSet();
            Map<ExprId, Demand> childRequiredDemands = new HashMap<>(inputDemands);
            childRequiredDemands.keySet().retainAll(childOutputExprIds);
            externalDemands.keySet().removeAll(childOutputExprIds);
            mergeDemands(externalDemands, traceOutputLineage(child, childRequiredDemands, targetColumn, context));
        }
        return externalDemands;
    }

    private static Map<ExprId, Demand> traceRecursiveCte(
            Plan plan, RecursiveCte recursiveCte, Map<ExprId, Demand> requiredDemands,
            String targetColumn, TraceContext context) {
        // The recursive term reads the previous iteration through the work table, so a column it writes
        // can carry the value of another column. Trace those columns as well until no new part of one is
        // reached.
        Map<ExprId, Integer> workTableOrdinals = workTableOrdinals(plan);
        Map<ExprId, Demand> externalDemands = new HashMap<>();
        Map<Integer, Demand> tracedDemands = new HashMap<>();
        Map<Integer, Demand> pendingDemands = requiredOutputDemands(plan, requiredDemands);
        while (!pendingDemands.isEmpty()) {
            tracedDemands.putAll(pendingDemands);
            Map<ExprId, Demand> childExternalDemands = new HashMap<>();
            for (int childIndex = 0; childIndex < plan.arity(); childIndex++) {
                mergeDemands(childExternalDemands, traceOutputLineage(
                        plan.child(childIndex),
                        childOutputDemands(recursiveCte.getRegularChildOutput(childIndex), pendingDemands),
                        targetColumn,
                        context));
            }
            pendingDemands = new HashMap<>();
            for (Map.Entry<ExprId, Demand> childExternalDemand : childExternalDemands.entrySet()) {
                Integer ordinal = workTableOrdinals.get(childExternalDemand.getKey());
                if (ordinal == null) {
                    externalDemands.merge(childExternalDemand.getKey(), childExternalDemand.getValue(), Demand::merge);
                    continue;
                }
                Demand tracedDemand = tracedDemands.getOrDefault(ordinal, Demand.NONE);
                Demand reachedDemand = tracedDemand.merge(childExternalDemand.getValue());
                if (!reachedDemand.equals(tracedDemand)) {
                    pendingDemands.merge(ordinal, reachedDemand, Demand::merge);
                }
            }
        }
        return externalDemands;
    }

    /** Maps each column of the work table the recursive term reads to the CTE output ordinal it holds. */
    private static Map<ExprId, Integer> workTableOrdinals(Plan recursiveCte) {
        CTEId cteId = ((LogicalRecursiveUnionAnchor<?>) recursiveCte.child(0)).getCteId();
        List<LogicalWorkTableReference> workTables = recursiveCte.child(1).collectToList(
                node -> node instanceof LogicalWorkTableReference
                        && ((LogicalWorkTableReference) node).getCteId().equals(cteId));
        Map<ExprId, Integer> ordinals = new HashMap<>();
        for (LogicalWorkTableReference workTable : workTables) {
            List<Slot> outputs = workTable.getOutput();
            for (int ordinal = 0; ordinal < outputs.size(); ordinal++) {
                ordinals.put(outputs.get(ordinal).getExprId(), ordinal);
            }
        }
        return ordinals;
    }

    private static Map<ExprId, Demand> traceGenerate(
            LogicalGenerate<?> generate, Map<ExprId, Demand> requiredDemands,
            String targetColumn, TraceContext context) {
        Map<ExprId, Demand> childRequiredDemands = new HashMap<>(requiredDemands);
        List<Slot> generatorOutputs = generate.getGeneratorOutput();
        List<? extends Expression> generators = generate.getGenerators();
        for (int ordinal = 0; ordinal < generatorOutputs.size(); ordinal++) {
            Slot generatorOutput = generatorOutputs.get(ordinal);
            Demand demand = requiredDemands.get(generatorOutput.getExprId());
            if (demand == null) {
                continue;
            }
            childRequiredDemands.remove(generatorOutput.getExprId());
            mergeDemands(childRequiredDemands,
                    validateExpression(generators.get(ordinal), demand, targetColumn, context));
        }
        Set<ExprId> childOutputExprIds = generate.child().getOutputExprIdSet();
        Map<ExprId, Demand> externalDemands = new HashMap<>(childRequiredDemands);
        externalDemands.keySet().removeAll(childOutputExprIds);
        childRequiredDemands.keySet().retainAll(childOutputExprIds);
        mergeDemands(externalDemands, traceOutputLineage(
                generate.child(), childRequiredDemands, targetColumn, context));
        return externalDemands;
    }

    private static Map<ExprId, Demand> traceSetOperation(
            Plan plan, SetOperation setOperation, Map<ExprId, Demand> requiredDemands,
            String targetColumn, TraceContext context) {
        Map<ExprId, Demand> externalDemands = new HashMap<>();
        Map<Integer, Demand> ordinalDemands = requiredOutputDemands(plan, requiredDemands);

        if (plan instanceof LogicalUnion) {
            for (List<NamedExpression> constantRow : ((LogicalUnion) plan).getConstantExprsList()) {
                for (Map.Entry<Integer, Demand> ordinalDemand : ordinalDemands.entrySet()) {
                    int ordinal = ordinalDemand.getKey();
                    if (ordinal < constantRow.size()) {
                        mergeDemands(externalDemands, validateExpression(
                                constantRow.get(ordinal), ordinalDemand.getValue(), targetColumn, context));
                    }
                }
            }
        }

        // EXCEPT and INTERSECT return rows of their first input; the later inputs only decide which of
        // those rows stay, so none of their values is written.
        int valueInputs = plan instanceof LogicalExcept || plan instanceof LogicalIntersect
                ? 1
                : setOperation.getArity();
        for (int childIndex = 0; childIndex < valueInputs; childIndex++) {
            mergeDemands(externalDemands, traceOutputLineage(
                    plan.child(childIndex),
                    childOutputDemands(setOperation.getRegularChildOutput(childIndex), ordinalDemands),
                    targetColumn,
                    context));
        }
        return externalDemands;
    }

    /** The required parts of the outputs of {@code plan}, by output ordinal. */
    private static Map<Integer, Demand> requiredOutputDemands(Plan plan, Map<ExprId, Demand> requiredDemands) {
        Map<Integer, Demand> ordinalDemands = new HashMap<>();
        List<Slot> outputs = plan.getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            Demand demand = requiredDemands.get(outputs.get(i).getExprId());
            if (demand != null) {
                ordinalDemands.put(i, demand);
            }
        }
        return ordinalDemands;
    }

    /** The required parts of the outputs of one input of a set operation, which it casts to its own outputs. */
    private static Map<ExprId, Demand> childOutputDemands(
            List<SlotReference> childOutputs, Map<Integer, Demand> ordinalDemands) {
        Map<ExprId, Demand> childDemands = new HashMap<>();
        for (Map.Entry<Integer, Demand> ordinalDemand : ordinalDemands.entrySet()) {
            int ordinal = ordinalDemand.getKey();
            if (ordinal < childOutputs.size()) {
                SlotReference childOutput = childOutputs.get(ordinal);
                childDemands.merge(childOutput.getExprId(),
                        ordinalDemand.getValue().alignTo(childOutput.getDataType()), Demand::merge);
            }
        }
        return childDemands;
    }

    private static Map<ExprId, Demand> traceCteConsumer(
            LogicalCTEConsumer consumer, Map<ExprId, Demand> requiredDemands,
            String targetColumn, TraceContext context) {
        // A producer shared by several consumers is traced once per output, whichever sets of its outputs
        // the consumers request, and again only for a part of the output it has not been traced for, which
        // happens at most once per nested field of the output type. So the work stays proportional to the
        // plan rather than to the number of paths through it or of the sets of outputs requested along them.
        Map<ExprId, Demand> externalDemands = new HashMap<>();
        for (Map.Entry<Slot, Slot> mapping : consumer.getConsumerToProducerOutputMap().entrySet()) {
            Demand demand = requiredDemands.get(mapping.getKey().getExprId());
            if (demand != null) {
                mergeDemands(externalDemands, traceCteOutput(
                        consumer, mapping.getValue().getExprId(), demand, targetColumn, context));
            }
        }
        return externalDemands;
    }

    private static Map<ExprId, Demand> traceCteOutput(
            LogicalCTEConsumer consumer, ExprId producerExprId, Demand demand,
            String targetColumn, TraceContext context) {
        Pair<CTEId, ExprId> traceKey = Pair.of(consumer.getCteId(), producerExprId);
        CteTrace completedTrace = context.completedCteTraces.get(traceKey);
        Demand tracedDemand = demand;
        if (completedTrace != null) {
            tracedDemand = completedTrace.demand.merge(demand);
            if (tracedDemand.equals(completedTrace.demand)) {
                return completedTrace.externalDemands;
            }
        }
        LogicalCTEProducer<?> producer = context.cteProducers.get(consumer.getCteId());
        Optional<Plan> producerPlan;
        if (producer == null) {
            // A producer taken from the analyzer context is not part of the traced plan, so the
            // producers of the WITH clauses nested in it have not been collected yet.
            producerPlan = context.findAnalyzerCteProducer(consumer);
            producerPlan.ifPresent(context::collectCteProducers);
        } else {
            producerPlan = Optional.of(producer.child());
        }
        if (!producerPlan.isPresent() || !context.activeCteIds.add(consumer.getCteId())) {
            return Collections.emptyMap();
        }
        try {
            Map<ExprId, Demand> externalDemands = ImmutableMap.copyOf(traceOutputLineage(
                    producerPlan.get(),
                    Collections.singletonMap(producerExprId, tracedDemand),
                    targetColumn,
                    context));
            context.completedCteTraces.put(traceKey, new CteTrace(tracedDemand, externalDemands));
            return externalDemands;
        } finally {
            context.activeCteIds.remove(consumer.getCteId());
        }
    }

    /**
     * Validates the casts on the parts of {@code expression} that {@code demand} requires and returns the
     * ExprIds those parts come from, with the part of each: the slots they read, and the correlated slots
     * of their scalar subqueries, which an outer scope may supply.
     */
    private static Map<ExprId, Demand> validateExpression(
            Expression expression, Demand demand, String targetColumn, TraceContext context) {
        List<Pair<Expression, Demand>> valueNodes = valueNodes(expression, demand);
        validateLossyCast(valueNodes, targetColumn);
        Map<ExprId, Demand> inputDemands = new HashMap<>();
        for (Pair<Expression, Demand> node : valueNodes) {
            if (node.first instanceof Slot) {
                inputDemands.merge(((Slot) node.first).getExprId(), node.second, Demand::merge);
            } else if (node.first instanceof ScalarSubquery) {
                mergeDemands(inputDemands,
                        traceScalarSubquery((ScalarSubquery) node.first, node.second, targetColumn, context));
            }
        }
        return inputDemands;
    }

    /** Traces a scalar subquery and returns its correlated slots that the subquery itself does not supply. */
    private static Map<ExprId, Demand> traceScalarSubquery(
            ScalarSubquery scalarSubquery, Demand demand, String targetColumn, TraceContext context) {
        Plan queryPlan = scalarSubquery.getQueryPlan();
        // The type coercion casts the output of the query, so it decides which parts of that output are written.
        Map<ExprId, Demand> outputDemands = validateExpression(
                scalarSubquery.getTypeCoercionExpr().orElse(queryPlan.getOutput().get(0)),
                demand,
                targetColumn,
                context);
        context.collectCteProducers(queryPlan);
        Map<ExprId, Demand> unresolvedDemands = new HashMap<>(
                traceOutputLineage(queryPlan, outputDemands, targetColumn, context));
        Set<ExprId> correlatedExprIds = new HashSet<>();
        for (Slot correlateSlot : scalarSubquery.getCorrelateSlots()) {
            correlatedExprIds.add(correlateSlot.getExprId());
        }
        unresolvedDemands.keySet().retainAll(correlatedExprIds);
        return unresolvedDemands;
    }

    /**
     * The nodes of {@code expression} whose values reach the parts {@code demand} requires, in pre-order,
     * each with the part of its own value that does. A node is left out if none of its value does: those
     * under the condition of an IF or of a CASE branch, under the second argument of NULLIF or under the
     * index or key of element_at, which only choose a value; those under the partition and order keys of a
     * window or the ORDER BY of an aggregate function such as GROUP_CONCAT, which only arrange the rows whose
     * values are used; and the arguments of a constructor or the fields of an element_at container that are
     * not required. The implicit cast a predicate applies to an operand is left out as well, since only its
     * comparison is written; the operand under it is not.
     */
    private static List<Pair<Expression, Demand>> valueNodes(Expression expression, Demand demand) {
        List<Pair<Expression, Demand>> nodes = new ArrayList<>();
        collectValueNodes(expression, demand, nodes);
        return nodes;
    }

    private static void collectValueNodes(
            Expression expression, Demand demand, List<Pair<Expression, Demand>> nodes) {
        if (demand.isNone()) {
            return;
        }
        // Casts, set operations and constructors convert values field by field in order, so a demand on
        // the fields of a value is aligned to the type of each value it passes to.
        Demand valueDemand = demand.isAll() ? demand : demand.alignTo(expression.getDataType());
        nodes.add(Pair.of(expression, valueDemand));
        Optional<List<Expression>> branchResults = ExpressionUtils.getCaseWhenLikeBranchResults(expression);
        if (branchResults.isPresent()) {
            for (Expression branchResult : branchResults.get()) {
                collectValueNodes(branchResult, valueDemand, nodes);
            }
        } else if (expression instanceof WindowExpression) {
            collectValueNodes(((WindowExpression) expression).getFunction(), valueDemand, nodes);
        } else if (expression instanceof Alias || expression instanceof Cast) {
            collectValueNodes(expression.child(0), valueDemand, nodes);
        } else if (expression instanceof CreateNamedStruct) {
            // The arguments alternate field names and field values.
            for (int field = 0; field * 2 + 1 < expression.arity(); field++) {
                collectValueNodes(expression.child(field * 2 + 1), valueDemand.part(field), nodes);
            }
        } else if (expression instanceof CreateStruct) {
            for (int field = 0; field < expression.arity(); field++) {
                collectValueNodes(expression.child(field), valueDemand.part(field), nodes);
            }
        } else if (expression instanceof Array) {
            for (Expression element : expression.children()) {
                collectValueNodes(element, valueDemand.part(0), nodes);
            }
        } else if (expression instanceof CreateMap) {
            // The arguments alternate keys and values.
            for (int i = 0; i < expression.arity(); i++) {
                collectValueNodes(expression.child(i), valueDemand.part(i % 2), nodes);
            }
        } else if (expression instanceof ElementAt) {
            collectValueNodes(expression.child(0), containerDemand((ElementAt) expression, valueDemand), nodes);
        } else if (isPredicate(expression)) {
            for (Expression operand : expression.children()) {
                boolean comparisonCast = operand instanceof Cast && !((Cast) operand).isExplicitType();
                collectValueNodes(comparisonCast ? operand.child(0) : operand, Demand.ALL, nodes);
            }
        } else {
            for (Expression child : expression.children()) {
                if (!(child instanceof OrderExpression)) {
                    collectValueNodes(child, Demand.ALL, nodes);
                }
            }
        }
    }

    /** The demand on the container {@code elementAt} reads that puts {@code elementDemand} on the element read. */
    private static Demand containerDemand(ElementAt elementAt, Demand elementDemand) {
        DataType containerType = elementAt.child(0).getDataType();
        if (containerType instanceof ArrayType) {
            return Demand.composite(Demand.Kind.ARRAY, ImmutableList.of(elementDemand));
        }
        if (containerType instanceof MapType) {
            return Demand.composite(Demand.Kind.MAP, ImmutableList.of(Demand.NONE, elementDemand));
        }
        if (containerType instanceof StructType) {
            // Analysis only accepts a constant 1-based position or field name to choose a struct field.
            List<StructField> fields = ((StructType) containerType).getFields();
            Expression selector = elementAt.child(1);
            int fieldIndex = selector instanceof IntegerLikeLiteral
                    ? ((IntegerLikeLiteral) selector).getIntValue() - 1
                    : fields.indexOf(((StructType) containerType).getField(
                            ((StringLikeLiteral) selector).getStringValue()));
            List<Demand> fieldDemands = new ArrayList<>(Collections.nCopies(fields.size(), Demand.NONE));
            fieldDemands.set(fieldIndex, elementDemand);
            return Demand.composite(Demand.Kind.STRUCT, fieldDemands);
        }
        return Demand.ALL;
    }

    /** Whether {@code expression} is a predicate, whose result only tells how its operands compare. */
    private static boolean isPredicate(Expression expression) {
        return expression instanceof ComparisonPredicate
                || expression instanceof InPredicate
                || expression instanceof StringRegexPredicate
                || expression instanceof CompoundPredicate
                || expression instanceof Not;
    }

    private static void validateLossyCast(List<Pair<Expression, Demand>> valueNodes, String targetColumn) {
        for (Pair<Expression, Demand> node : valueNodes) {
            if (!(node.first instanceof Cast)) {
                continue;
            }
            Cast cast = (Cast) node.first;
            if (!cast.isExplicitType()
                    && losesVariantLeaf(cast.child().getDataType(), cast.getDataType(), node.second)) {
                throw new AnalysisException(
                        "VARIANT write cannot safely convert input column '" + targetColumn
                                + "': source expression implicitly casts VARIANT to "
                                + cast.getDataType().toSql() + " before sink analysis;"
                                + " use an explicit CAST if the conversion is intended");
            }
        }
    }

    /** Whether a cast loses a Variant leaf in the parts of its value that {@code demand} requires. */
    private static boolean losesVariantLeaf(DataType sourceType, DataType targetType, Demand demand) {
        if (demand.isNone()) {
            return false;
        }
        // The demand is aligned to the cast type. A cast from a value of another shape does not keep the
        // parts apart, so it is judged on the whole value.
        if (demand.isAll() || !demand.hasShapeOf(sourceType)) {
            return losesVariantLeaf(sourceType, targetType);
        }
        List<DataType> sourcePartTypes = Demand.partTypes(sourceType);
        List<DataType> targetPartTypes = Demand.partTypes(targetType);
        for (int i = 0; i < sourcePartTypes.size(); i++) {
            if (losesVariantLeaf(sourcePartTypes.get(i), targetPartTypes.get(i), demand.part(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean losesVariantLeaf(DataType sourceType, DataType targetType) {
        // Encoding a scalar or supported ARRAY value as one top-level Variant preserves all
        // nested Variant leaves. The lossy case is a Variant leaf coerced to a non-Variant path.
        if (targetType instanceof VariantType) {
            return false;
        }
        if (sourceType instanceof VariantType) {
            return true;
        }
        if (sourceType instanceof ArrayType) {
            return targetType instanceof ArrayType
                    ? losesVariantLeaf(
                            ((ArrayType) sourceType).getItemType(),
                            ((ArrayType) targetType).getItemType())
                    : VariantType.containsVariant(sourceType);
        }
        if (sourceType instanceof MapType) {
            if (!(targetType instanceof MapType)) {
                return VariantType.containsVariant(sourceType);
            }
            MapType sourceMap = (MapType) sourceType;
            MapType targetMap = (MapType) targetType;
            return losesVariantLeaf(sourceMap.getKeyType(), targetMap.getKeyType())
                    || losesVariantLeaf(sourceMap.getValueType(), targetMap.getValueType());
        }
        if (sourceType instanceof StructType) {
            if (!(targetType instanceof StructType)) {
                return VariantType.containsVariant(sourceType);
            }
            List<StructField> sourceFields = ((StructType) sourceType).getFields();
            List<StructField> targetFields = ((StructType) targetType).getFields();
            if (sourceFields.size() != targetFields.size()) {
                return VariantType.containsVariant(sourceType);
            }
            for (int i = 0; i < sourceFields.size(); i++) {
                if (losesVariantLeaf(sourceFields.get(i).getDataType(), targetFields.get(i).getDataType())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void mergeDemands(Map<ExprId, Demand> demands, Map<ExprId, Demand> addedDemands) {
        addedDemands.forEach((exprId, demand) -> demands.merge(exprId, demand, Demand::merge));
    }

    /**
     * The parts of a value that reach a Variant leaf of the target column: none, all of it, or, of a STRUCT,
     * ARRAY or MAP value, the parts of each field, of the elements, or of the keys and of the values.
     */
    private static final class Demand {
        private static final Demand NONE = new Demand(Kind.NONE, ImmutableList.of());
        private static final Demand ALL = new Demand(Kind.ALL, ImmutableList.of());

        private enum Kind {
            NONE, ALL, STRUCT, ARRAY, MAP
        }

        private final Kind kind;
        // The demands on the fields of a STRUCT, on the element of an ARRAY, or on the key and the value of a MAP.
        private final List<Demand> parts;

        private Demand(Kind kind, List<Demand> parts) {
            this.kind = kind;
            this.parts = parts;
        }

        /** The Variant leaves of a value of {@code type}. */
        private static Demand of(DataType type) {
            if (type instanceof VariantType) {
                return ALL;
            }
            List<Demand> partDemands = new ArrayList<>();
            for (DataType partType : partTypes(type)) {
                partDemands.add(of(partType));
            }
            return composite(kindOf(type), partDemands);
        }

        /** The demand of {@code kind} on these parts, or none if none of them is required. */
        private static Demand composite(Kind kind, List<Demand> partDemands) {
            return partDemands.stream().allMatch(Demand::isNone)
                    ? NONE
                    : new Demand(kind, ImmutableList.copyOf(partDemands));
        }

        /** The kind of the parts of a value of {@code type}, or NONE if the value has no parts. */
        private static Kind kindOf(DataType type) {
            if (type instanceof StructType) {
                return Kind.STRUCT;
            }
            if (type instanceof ArrayType) {
                return Kind.ARRAY;
            }
            return type instanceof MapType ? Kind.MAP : Kind.NONE;
        }

        /** The types of the fields of a STRUCT, of the element of an ARRAY, or of the key and the value of a MAP. */
        private static List<DataType> partTypes(DataType type) {
            if (type instanceof StructType) {
                List<DataType> fieldTypes = new ArrayList<>();
                for (StructField field : ((StructType) type).getFields()) {
                    fieldTypes.add(field.getDataType());
                }
                return fieldTypes;
            }
            if (type instanceof ArrayType) {
                return ImmutableList.of(((ArrayType) type).getItemType());
            }
            if (type instanceof MapType) {
                return ImmutableList.of(((MapType) type).getKeyType(), ((MapType) type).getValueType());
            }
            return ImmutableList.of();
        }

        private boolean isNone() {
            return kind == Kind.NONE;
        }

        private boolean isAll() {
            return kind == Kind.ALL;
        }

        /** Whether a value of {@code type} has the parts this demand is on. */
        private boolean hasShapeOf(DataType type) {
            return kindOf(type) == kind && partTypes(type).size() == parts.size();
        }

        /** The demand on the field, element, key or value at {@code index} of a STRUCT, ARRAY or MAP value. */
        private Demand part(int index) {
            return isAll() ? ALL : parts.get(index);
        }

        /**
         * This demand on a value of {@code type} that is converted to the value it is on, part by part in
         * order. A value of another shape does not keep the parts apart, so all of it is required.
         */
        private Demand alignTo(DataType type) {
            if (isNone() || isAll()) {
                return this;
            }
            if (!hasShapeOf(type)) {
                return ALL;
            }
            List<DataType> partTypes = partTypes(type);
            List<Demand> alignedParts = new ArrayList<>(parts.size());
            for (int i = 0; i < parts.size(); i++) {
                alignedParts.add(parts.get(i).alignTo(partTypes.get(i)));
            }
            return new Demand(kind, ImmutableList.copyOf(alignedParts));
        }

        /** The parts that this demand or {@code other} requires. */
        private Demand merge(Demand other) {
            if (isNone()) {
                return other;
            }
            if (other.isNone()) {
                return this;
            }
            if (isAll() || kind != other.kind || parts.size() != other.parts.size()) {
                return ALL;
            }
            List<Demand> mergedParts = new ArrayList<>(parts.size());
            for (int i = 0; i < parts.size(); i++) {
                mergedParts.add(parts.get(i).merge(other.parts.get(i)));
            }
            return new Demand(kind, ImmutableList.copyOf(mergedParts));
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Demand)) {
                return false;
            }
            Demand that = (Demand) other;
            return kind == that.kind && parts.equals(that.parts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, parts);
        }
    }

    /** The part of a CTE output traced so far, with the parts of the ExprIds that trace returned. */
    private static final class CteTrace {
        private final Demand demand;
        private final Map<ExprId, Demand> externalDemands;

        private CteTrace(Demand demand, Map<ExprId, Demand> externalDemands) {
            this.demand = demand;
            this.externalDemands = externalDemands;
        }
    }

    private static final class TraceContext {
        private final Map<CTEId, LogicalCTEProducer<?>> cteProducers = new HashMap<>();
        private final Set<CTEId> activeCteIds = new HashSet<>();
        // Completed traces of producer outputs, keyed by CTE and output.
        private final Map<Pair<CTEId, ExprId>, CteTrace> completedCteTraces = new HashMap<>();
        private final Optional<CTEContext> analyzerCteContext;

        private TraceContext(Plan sourcePlan, Optional<CTEContext> analyzerCteContext) {
            this.analyzerCteContext = analyzerCteContext;
            collectCteProducers(sourcePlan);
        }

        private Optional<Plan> findAnalyzerCteProducer(LogicalCTEConsumer consumer) {
            return analyzerCteContext
                    .flatMap(context -> context.findCTEContext(consumer.getName()))
                    .filter(context -> context.getCteId().equals(consumer.getCteId()))
                    .flatMap(context -> context.getAnalyzedCTEPlan(consumer.getName()))
                    .map(Plan.class::cast);
        }

        private void collectCteProducers(Plan plan) {
            if (plan instanceof LogicalCTEProducer) {
                LogicalCTEProducer<?> producer = (LogicalCTEProducer<?>) plan;
                cteProducers.put(producer.getCteId(), producer);
            }
            for (Plan child : plan.children()) {
                collectCteProducers(child);
            }
        }
    }
}
