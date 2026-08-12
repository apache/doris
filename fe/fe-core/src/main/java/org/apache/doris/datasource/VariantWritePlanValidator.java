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
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.RecursiveCte;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
     * intentional user conversion and are not rejected.</p>
     */
    public static void validateNoLossyCoercion(
            String sinkName, List<Column> targetColumns, Plan sourcePlan) {
        validateNoLossyCoercion(
                sinkName, targetColumns, sourcePlan, Optional.empty());
    }

    /** Validates a sink while its enclosing CTE producers still live in the analyzer context. */
    public static void validateNoLossyCoercion(
            String sinkName, List<Column> targetColumns, Plan sourcePlan,
            CTEContext cteContext) {
        validateNoLossyCoercion(
                sinkName, targetColumns, sourcePlan, Optional.of(cteContext));
    }

    private static void validateNoLossyCoercion(
            String sinkName, List<Column> targetColumns, Plan sourcePlan,
            Optional<CTEContext> cteContext) {
        if (targetColumns.size() != sourcePlan.getOutput().size()) {
            throw new AnalysisException(
                    sinkName + " Variant write target and source columns are not aligned");
        }
        List<Integer> sourceOrdinals = new ArrayList<>(targetColumns.size());
        for (int i = 0; i < targetColumns.size(); i++) {
            sourceOrdinals.add(i);
        }
        validateNoLossyCoercion(
                sinkName, targetColumns, sourcePlan, sourceOrdinals, cteContext);
    }

    /**
     * Validates target columns that are embedded in a wider source output, such as Iceberg
     * row-level DML where operation and row-id routing columns precede table data columns.
     */
    public static void validateNoLossyCoercion(
            String sinkName, List<Column> targetColumns, Plan sourcePlan,
            List<Integer> sourceOrdinals) {
        validateNoLossyCoercion(
                sinkName, targetColumns, sourcePlan, sourceOrdinals, Optional.empty());
    }

    /** Validates mapped source ordinals with analyzer-time CTE producer visibility. */
    public static void validateNoLossyCoercion(
            String sinkName, List<Column> targetColumns, Plan sourcePlan,
            List<Integer> sourceOrdinals, CTEContext cteContext) {
        validateNoLossyCoercion(
                sinkName, targetColumns, sourcePlan, sourceOrdinals, Optional.of(cteContext));
    }

    private static void validateNoLossyCoercion(
            String sinkName, List<Column> targetColumns, Plan sourcePlan,
            List<Integer> sourceOrdinals, Optional<CTEContext> cteContext) {
        if (targetColumns.size() != sourceOrdinals.size()) {
            throw new AnalysisException(
                    sinkName + " Variant write target and source columns are not aligned");
        }

        List<Integer> variantTargetOrdinals = new ArrayList<>();
        for (int i = 0; i < targetColumns.size(); i++) {
            DataType targetType = DataType.fromCatalogType(targetColumns.get(i).getType());
            if (VariantType.containsVariant(targetType)) {
                variantTargetOrdinals.add(i);
            }
        }
        if (variantTargetOrdinals.isEmpty()) {
            return;
        }

        TraceContext traceContext = new TraceContext(sourcePlan, cteContext);
        List<Slot> sourceOutputs = sourcePlan.getOutput();
        for (int targetOrdinal : variantTargetOrdinals) {
            int sourceOrdinal = sourceOrdinals.get(targetOrdinal);
            if (sourceOrdinal < 0 || sourceOrdinal >= sourceOutputs.size()) {
                throw new AnalysisException(
                        sinkName + " Variant write target and source columns are not aligned");
            }
            Column targetColumn = targetColumns.get(targetOrdinal);
            traceOutputLineage(
                    sourcePlan,
                    Collections.singleton(sourceOutputs.get(sourceOrdinal).getExprId()),
                    sinkName,
                    targetColumn.getName(),
                    traceContext);
        }
    }

    /**
     * Validates the requested outputs and returns lineage ExprIds supplied by an outer scope.
     * Scalar subqueries use the returned IDs to continue tracing correlated columns in the
     * enclosing plan.
     */
    private static Set<ExprId> traceOutputLineage(
            Plan plan, Set<ExprId> requiredExprIds, String sinkName, String targetColumn,
            TraceContext context) {
        if (requiredExprIds.isEmpty()) {
            return Collections.emptySet();
        }
        if (plan instanceof SetOperation) {
            return traceSetOperation(
                    plan, (SetOperation) plan, requiredExprIds, sinkName, targetColumn, context);
        }
        if (plan instanceof RecursiveCte) {
            return traceRecursiveCte(
                    plan, (RecursiveCte) plan, requiredExprIds, sinkName, targetColumn, context);
        }
        if (plan instanceof LogicalGenerate) {
            return traceGenerate(
                    (LogicalGenerate<?>) plan, requiredExprIds, sinkName, targetColumn, context);
        }
        if (plan instanceof LogicalCTEConsumer) {
            return traceCteConsumer(
                    (LogicalCTEConsumer) plan, requiredExprIds, sinkName, targetColumn, context);
        }

        Set<ExprId> unresolvedExprIds = new HashSet<>(requiredExprIds);
        Set<ExprId> inputExprIds = new HashSet<>();
        for (Expression expression : plan.getExpressions()) {
            if (!(expression instanceof NamedExpression)) {
                continue;
            }
            NamedExpression namedExpression = (NamedExpression) expression;
            if (!requiredExprIds.contains(namedExpression.getExprId())) {
                continue;
            }
            inputExprIds.addAll(validateExpression(
                    namedExpression, sinkName, targetColumn, context));
            inputExprIds.addAll(namedExpression.getInputSlotExprIds());
            unresolvedExprIds.remove(namedExpression.getExprId());
        }
        inputExprIds.addAll(unresolvedExprIds);

        Set<ExprId> externalExprIds = new HashSet<>(inputExprIds);
        for (Plan child : plan.children()) {
            Set<ExprId> childOutputExprIds = child.getOutputExprIdSet();
            Set<ExprId> childRequiredExprIds = new HashSet<>(inputExprIds);
            childRequiredExprIds.retainAll(childOutputExprIds);
            externalExprIds.removeAll(childOutputExprIds);
            externalExprIds.addAll(traceOutputLineage(
                    child, childRequiredExprIds, sinkName, targetColumn, context));
        }
        return externalExprIds;
    }

    private static Set<ExprId> traceRecursiveCte(
            Plan plan, RecursiveCte recursiveCte, Set<ExprId> requiredExprIds,
            String sinkName, String targetColumn, TraceContext context) {
        Set<ExprId> externalExprIds = new HashSet<>();
        Set<Integer> requiredOrdinals = requiredOutputOrdinals(plan, requiredExprIds);
        for (int childIndex = 0; childIndex < plan.arity(); childIndex++) {
            List<SlotReference> childOutputs = recursiveCte.getRegularChildOutput(childIndex);
            Set<ExprId> childRequiredExprIds = new HashSet<>();
            for (int ordinal : requiredOrdinals) {
                if (ordinal < childOutputs.size()) {
                    childRequiredExprIds.add(childOutputs.get(ordinal).getExprId());
                }
            }
            externalExprIds.addAll(traceOutputLineage(
                    plan.child(childIndex), childRequiredExprIds,
                    sinkName, targetColumn, context));
        }
        return externalExprIds;
    }

    private static Set<ExprId> traceGenerate(
            LogicalGenerate<?> generate, Set<ExprId> requiredExprIds,
            String sinkName, String targetColumn, TraceContext context) {
        Set<ExprId> childRequiredExprIds = new HashSet<>(requiredExprIds);
        List<Slot> generatorOutputs = generate.getGeneratorOutput();
        List<? extends Expression> generators = generate.getGenerators();
        for (int ordinal = 0; ordinal < generatorOutputs.size(); ordinal++) {
            Slot generatorOutput = generatorOutputs.get(ordinal);
            if (!requiredExprIds.contains(generatorOutput.getExprId())) {
                continue;
            }
            Expression generator = generators.get(ordinal);
            childRequiredExprIds.addAll(validateExpression(
                    generator, sinkName, targetColumn, context));
            childRequiredExprIds.remove(generatorOutput.getExprId());
            childRequiredExprIds.addAll(generator.getInputSlotExprIds());
        }
        Set<ExprId> childOutputExprIds = generate.child().getOutputExprIdSet();
        Set<ExprId> externalExprIds = new HashSet<>(childRequiredExprIds);
        externalExprIds.removeAll(childOutputExprIds);
        childRequiredExprIds.retainAll(childOutputExprIds);
        externalExprIds.addAll(traceOutputLineage(
                generate.child(), childRequiredExprIds, sinkName, targetColumn, context));
        return externalExprIds;
    }

    private static Set<ExprId> traceSetOperation(
            Plan plan, SetOperation setOperation, Set<ExprId> requiredExprIds,
            String sinkName, String targetColumn, TraceContext context) {
        Set<ExprId> externalExprIds = new HashSet<>();
        Set<Integer> requiredOrdinals = requiredOutputOrdinals(plan, requiredExprIds);

        if (plan instanceof LogicalUnion) {
            for (List<NamedExpression> constantRow
                    : ((LogicalUnion) plan).getConstantExprsList()) {
                for (int ordinal : requiredOrdinals) {
                    if (ordinal < constantRow.size()) {
                        externalExprIds.addAll(validateExpression(
                                constantRow.get(ordinal), sinkName, targetColumn, context));
                    }
                }
            }
        }

        for (int childIndex = 0; childIndex < setOperation.getArity(); childIndex++) {
            List<SlotReference> childOutputs = setOperation.getRegularChildOutput(childIndex);
            Set<ExprId> childRequiredExprIds = new HashSet<>();
            for (int ordinal : requiredOrdinals) {
                if (ordinal < childOutputs.size()) {
                    childRequiredExprIds.add(childOutputs.get(ordinal).getExprId());
                }
            }
            externalExprIds.addAll(traceOutputLineage(
                    plan.child(childIndex), childRequiredExprIds,
                    sinkName, targetColumn, context));
        }
        return externalExprIds;
    }

    private static Set<Integer> requiredOutputOrdinals(
            Plan plan, Set<ExprId> requiredExprIds) {
        Set<Integer> requiredOrdinals = new HashSet<>();
        List<Slot> outputs = plan.getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            if (requiredExprIds.contains(outputs.get(i).getExprId())) {
                requiredOrdinals.add(i);
            }
        }
        return requiredOrdinals;
    }

    private static Set<ExprId> traceCteConsumer(
            LogicalCTEConsumer consumer, Set<ExprId> requiredExprIds,
            String sinkName, String targetColumn, TraceContext context) {
        LogicalCTEProducer<?> producer = context.cteProducers.get(consumer.getCteId());
        Optional<Plan> producerPlan = producer == null
                ? context.findAnalyzerCteProducer(consumer)
                : Optional.of(producer.child());
        if (!producerPlan.isPresent() || !context.activeCteIds.add(consumer.getCteId())) {
            return Collections.emptySet();
        }
        try {
            Set<ExprId> producerExprIds = new HashSet<>();
            for (Map.Entry<Slot, Slot> mapping
                    : consumer.getConsumerToProducerOutputMap().entrySet()) {
                if (requiredExprIds.contains(mapping.getKey().getExprId())) {
                    producerExprIds.add(mapping.getValue().getExprId());
                }
            }
            return traceOutputLineage(
                    producerPlan.get(), producerExprIds, sinkName, targetColumn, context);
        } finally {
            context.activeCteIds.remove(consumer.getCteId());
        }
    }

    private static Set<ExprId> validateExpression(
            Expression expression, String sinkName, String targetColumn, TraceContext context) {
        validateLossyCast(expression, sinkName, targetColumn);
        Set<ExprId> correlatedExprIds = new HashSet<>();
        for (ScalarSubquery scalarSubquery
                : expression.<ScalarSubquery>collectToList(ScalarSubquery.class::isInstance)) {
            scalarSubquery.getTypeCoercionExpr()
                    .ifPresent(coercion -> validateLossyCast(coercion, sinkName, targetColumn));
            Plan queryPlan = scalarSubquery.getQueryPlan();
            context.collectCteProducers(queryPlan);
            Set<ExprId> unresolvedExprIds = new HashSet<>(traceOutputLineage(
                    queryPlan,
                    Collections.singleton(queryPlan.getOutput().get(0).getExprId()),
                    sinkName,
                    targetColumn,
                    context));
            Set<ExprId> scalarCorrelatedExprIds = new HashSet<>();
            for (Slot correlateSlot : scalarSubquery.getCorrelateSlots()) {
                scalarCorrelatedExprIds.add(correlateSlot.getExprId());
            }
            unresolvedExprIds.retainAll(scalarCorrelatedExprIds);
            correlatedExprIds.addAll(unresolvedExprIds);
        }
        return correlatedExprIds;
    }

    private static void validateLossyCast(
            Expression expression, String sinkName, String targetColumn) {
        Optional<Cast> lossyCast = expression.collectFirst(node -> {
            if (!(node instanceof Cast)) {
                return false;
            }
            Cast cast = (Cast) node;
            return !cast.isExplicitType()
                    && losesVariantLeaf(cast.child().getDataType(), cast.getDataType());
        });
        if (lossyCast.isPresent()) {
            throw new AnalysisException(
                    sinkName + " VARIANT write cannot safely convert input column '"
                            + targetColumn + "': source expression implicitly casts VARIANT to "
                            + lossyCast.get().getDataType().toSql() + " before sink analysis");
        }
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
                if (losesVariantLeaf(
                        sourceFields.get(i).getDataType(), targetFields.get(i).getDataType())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final class TraceContext {
        private final Map<CTEId, LogicalCTEProducer<?>> cteProducers = new HashMap<>();
        private final Set<CTEId> activeCteIds = new HashSet<>();
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
