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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
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
        if (targetColumns.size() != sourcePlan.getOutput().size()) {
            throw new AnalysisException(
                    sinkName + " Variant write target and source columns are not aligned");
        }

        List<Integer> variantOrdinals = new ArrayList<>();
        for (int i = 0; i < targetColumns.size(); i++) {
            DataType targetType = DataType.fromCatalogType(targetColumns.get(i).getType());
            if (VariantType.containsVariant(targetType)) {
                variantOrdinals.add(i);
            }
        }
        if (variantOrdinals.isEmpty()) {
            return;
        }

        TraceContext traceContext = new TraceContext(sourcePlan);
        for (int ordinal : variantOrdinals) {
            Column targetColumn = targetColumns.get(ordinal);
            traceOutputLineage(
                    sourcePlan,
                    Collections.singleton(sourcePlan.getOutput().get(ordinal).getExprId()),
                    sinkName,
                    targetColumn.getName(),
                    traceContext);
        }
    }

    private static void traceOutputLineage(
            Plan plan, Set<ExprId> requiredExprIds, String sinkName, String targetColumn,
            TraceContext context) {
        if (requiredExprIds.isEmpty()) {
            return;
        }
        if (plan instanceof SetOperation) {
            traceSetOperation(
                    plan, (SetOperation) plan, requiredExprIds, sinkName, targetColumn, context);
            return;
        }
        if (plan instanceof LogicalCTEConsumer) {
            traceCteConsumer(
                    (LogicalCTEConsumer) plan, requiredExprIds, sinkName, targetColumn, context);
            return;
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
            validateExpression(namedExpression, sinkName, targetColumn);
            inputExprIds.addAll(namedExpression.getInputSlotExprIds());
            unresolvedExprIds.remove(namedExpression.getExprId());
        }
        inputExprIds.addAll(unresolvedExprIds);

        for (Plan child : plan.children()) {
            Set<ExprId> childOutputExprIds = child.getOutputExprIdSet();
            Set<ExprId> childRequiredExprIds = new HashSet<>(inputExprIds);
            childRequiredExprIds.retainAll(childOutputExprIds);
            traceOutputLineage(
                    child, childRequiredExprIds, sinkName, targetColumn, context);
        }
    }

    private static void traceSetOperation(
            Plan plan, SetOperation setOperation, Set<ExprId> requiredExprIds,
            String sinkName, String targetColumn, TraceContext context) {
        Set<Integer> requiredOrdinals = new HashSet<>();
        List<Slot> outputs = plan.getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            if (requiredExprIds.contains(outputs.get(i).getExprId())) {
                requiredOrdinals.add(i);
            }
        }

        if (plan instanceof LogicalUnion) {
            for (List<NamedExpression> constantRow
                    : ((LogicalUnion) plan).getConstantExprsList()) {
                for (int ordinal : requiredOrdinals) {
                    if (ordinal < constantRow.size()) {
                        validateExpression(constantRow.get(ordinal), sinkName, targetColumn);
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
            traceOutputLineage(
                    plan.child(childIndex), childRequiredExprIds,
                    sinkName, targetColumn, context);
        }
    }

    private static void traceCteConsumer(
            LogicalCTEConsumer consumer, Set<ExprId> requiredExprIds,
            String sinkName, String targetColumn, TraceContext context) {
        LogicalCTEProducer<?> producer = context.cteProducers.get(consumer.getCteId());
        if (producer == null || !context.activeCteIds.add(consumer.getCteId())) {
            return;
        }
        try {
            Set<ExprId> producerExprIds = new HashSet<>();
            for (Map.Entry<Slot, Slot> mapping
                    : consumer.getConsumerToProducerOutputMap().entrySet()) {
                if (requiredExprIds.contains(mapping.getKey().getExprId())) {
                    producerExprIds.add(mapping.getValue().getExprId());
                }
            }
            traceOutputLineage(
                    producer.child(), producerExprIds, sinkName, targetColumn, context);
        } finally {
            context.activeCteIds.remove(consumer.getCteId());
        }
    }

    private static void validateExpression(
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

        private TraceContext(Plan sourcePlan) {
            collectCteProducers(sourcePlan);
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
