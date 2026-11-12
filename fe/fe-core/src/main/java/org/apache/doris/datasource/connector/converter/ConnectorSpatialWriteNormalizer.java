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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.rewrite.PushProjectThroughUnion;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.ProjectProcessor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

/** Restores proven NULL producer values before spatial sink coercion. */
public final class ConnectorSpatialWriteNormalizer {
    private ConnectorSpatialWriteNormalizer() {
    }

    public static LogicalPlan normalize(List<Column> targets, LogicalPlan source, CTEContext cteContext) {
        return normalize(targets, source, IntStream.range(0, targets.size()).boxed()
                .collect(ImmutableList.toImmutableList()), cteContext);
    }

    /** Aligns data targets with source positions when a row-level sink also carries routing columns. */
    public static LogicalPlan normalize(List<Column> targets, LogicalPlan source,
            List<Integer> sourceOrdinals, CTEContext cteContext) {
        if (targets.stream().noneMatch(column -> column.getType().typeContainsSpatialType())) {
            return source;
        }
        if (source instanceof LogicalUnion && ((LogicalUnion) source).getQualifier() == Qualifier.ALL) {
            return normalizeUnionAll(targets, (LogicalUnion) source, sourceOrdinals, cteContext);
        }
        if (source instanceof LogicalSubQueryAlias) {
            LogicalPlan child = (LogicalPlan) source.child(0);
            LogicalPlan normalized = normalize(targets, child, sourceOrdinals, cteContext);
            return normalized == child ? source : (LogicalPlan) source.withChildren(ImmutableList.of(normalized));
        }
        if (source instanceof LogicalProject && isForwardingProject((LogicalProject<?>) source)) {
            Plan input = source.child(0);
            while (input instanceof LogicalSubQueryAlias) {
                input = input.child(0);
            }
            Plan boundary = input;
            while (boundary instanceof LogicalSubQueryAlias
                    || (boundary instanceof LogicalProject && isForwardingProject((LogicalProject<?>) boundary))) {
                boundary = boundary.child(0);
            }
            if (boundary instanceof LogicalUnion && ((LogicalUnion) boundary).getQualifier() == Qualifier.ALL) {
                // Reuse the existing safe forwarding/cast projection rule, including its ExprId handling.
                Optional<Plan> pushed = ProjectProcessor.tryProcessProject(
                        ((LogicalProject<?>) source).getProjects(), input);
                // Forwarding aliases can also repeat or prune outputs without moving calculations.
                if (!pushed.isPresent() && input instanceof LogicalUnion
                        && ((LogicalProject<?>) source).getProjects().stream()
                        .allMatch(project -> project instanceof Slot
                                || (project instanceof Alias && project.child(0) instanceof Slot))) {
                    pushed = Optional.of(PushProjectThroughUnion.doPushProject(
                            ((LogicalProject<?>) source).getProjects(), (LogicalUnion) input));
                }
                if (pushed.isPresent()) {
                    return normalize(targets, (LogicalPlan) pushed.get(), sourceOrdinals, cteContext);
                }
            }
        }
        Resolver resolver = new Resolver(source, cteContext);
        List<NamedExpression> outputs = new ArrayList<>(source.getOutput());
        List<LogicalPlan> wrappers = new ArrayList<>();
        Set<ExprId> sortInputs = new HashSet<>();
        LogicalPlan producerInput = source;
        while (producerInput instanceof LogicalSort || producerInput instanceof LogicalLimit) {
            wrappers.add(producerInput);
            for (Expression expression : producerInput.getExpressions()) {
                // A direct CARDINALITY(map_slot) depends on the entry count, which retagging preserves.
                if (!isMapCardinality(expression)) {
                    for (Slot input : expression.getInputSlots()) {
                        sortInputs.add(input.getExprId());
                    }
                }
            }
            producerInput = (LogicalPlan) producerInput.child(0);
        }
        LogicalProject<?> producer = producerInput instanceof LogicalProject
                && !((LogicalProject<?>) producerInput).isDistinct() ? (LogicalProject<?>) producerInput : null;
        List<NamedExpression> producerOutputs = null;
        Map<ExprId, Slot> replacedSlots = new HashMap<>();
        boolean changed = false;
        for (int i = 0; i < targets.size(); i++) {
            Column target = targets.get(i);
            if (!target.getType().typeContainsSpatialType()) {
                continue;
            }
            int sourceOrdinal = sourceOrdinals.get(i);
            Slot slot = source.getOutput().get(sourceOrdinal);
            Expression value = resolver.resolve(slot, source);
            DataType targetType = DataType.fromCatalogType(target.getType());
            boolean literal = isLiteralConstruction(value) || canRetargetSpatialNulls(value, targetType);
            // SELECT/UPDATE reach the bound sink with CAST(ARRAY(NULL) AS ARRAY<INT>).
            // Reuse MERGE's NULL-only proof before validation, without lifting arbitrary producers.
            if (literal) {
                value = normalizeSpatialNullsBeforeBinding(value, targetType);
            }
            if (!literal && producer != null && !sortInputs.contains(slot.getExprId())
                    && sourceOrdinals.indexOf(sourceOrdinal) == sourceOrdinals.lastIndexOf(sourceOrdinal)) {
                // Retag a constructor where it is computed, so its dynamic ordinary siblings
                // still use the same inputs and are evaluated only once. Do not follow slots
                // through joins, other producers, or a DISTINCT projection.
                NamedExpression project = producer.getProjects().get(sourceOrdinal);
                if (project instanceof Alias && canRetargetSpatialNulls(project.child(0), targetType, true)) {
                    Expression normalized = normalizeSpatialNullsBeforeBinding(project.child(0), targetType, true);
                    if (!normalized.getDataType().equals(slot.getDataType())) {
                        value = normalized;
                        if (producerOutputs == null) {
                            producerOutputs = new ArrayList<>(producer.getProjects());
                        }
                        Alias replacement = new Alias(normalized, slot.getName());
                        producerOutputs.set(sourceOrdinal, replacement);
                        Slot replacementSlot = replacement.toSlot();
                        outputs.set(sourceOrdinal, replacementSlot);
                        replacedSlots.put(slot.getExprId(), replacementSlot);
                    }
                }
            }
            ConnectorSpatialWriteValidator.validate(target, value);
            // Never lift arbitrary producer expressions above their input scope, or
            // replace outer-join NULL padding with a non-NULL container constant.
            if (literal) {
                Expression normalized = normalizeSpatialNulls(value, targetType);
                if (!normalized.getDataType().equals(slot.getDataType())) {
                    // SELECT v,v can feed an ordinary BIGINT and a spatial NULL. Sharing
                    // v's ExprId would let project merging replace the ordinary cast's input.
                    outputs.set(sourceOrdinal, new Alias(normalized, slot.getName()));
                    changed = true;
                }
            }
        }
        LogicalPlan normalizedSource = source;
        if (producerOutputs != null) {
            normalizedSource = producer.withProjects(producerOutputs);
            for (int i = wrappers.size() - 1; i >= 0; i--) {
                LogicalPlan wrapper = wrappers.get(i);
                if (wrapper instanceof LogicalSort) {
                    LogicalSort<?> sort = (LogicalSort<?>) wrapper;
                    List<OrderKey> keys = sort.getOrderKeys().stream().map(key -> {
                        Expression expr = key.getExpr();
                        if (isMapCardinality(expr)) {
                            Slot input = replacedSlots.get(((Slot) expr.child(0)).getExprId());
                            if (input != null) {
                                return key.withExpression(new Cardinality(input));
                            }
                        }
                        return key;
                    }).collect(ImmutableList.toImmutableList());
                    normalizedSource = sort.withOrderKeysAndChild(keys, normalizedSource);
                } else {
                    normalizedSource = (LogicalPlan) wrapper.withChildren(ImmutableList.of(normalizedSource));
                }
            }
        }
        return changed ? new LogicalProject<>(outputs, normalizedSource) : normalizedSource;
    }

    private static boolean isMapCardinality(Expression expression) {
        return expression instanceof Cardinality && expression.child(0) instanceof Slot
                && expression.child(0).getDataType() instanceof MapType;
    }

    private static boolean isForwardingProject(LogicalProject<?> project) {
        return !project.isDistinct() && project.getProjects().stream().allMatch(output -> output instanceof Slot
                || (output instanceof Alias && ExpressionUtils.getExpressionCoveredByCast(output.child(0))
                        instanceof Slot));
    }

    private static LogicalPlan normalizeUnionAll(List<Column> targets, LogicalUnion source,
            List<Integer> sourceOrdinals, CTEContext context) {
        List<Plan> children = new ArrayList<>();
        List<List<SlotReference>> childOutputs = new ArrayList<>();
        boolean changed = !source.getConstantExprsList().isEmpty();
        for (int i = 0; i < source.arity(); i++) {
            LogicalPlan child = (LogicalPlan) source.child(i);
            List<SlotReference> outputs = source.getRegularChildOutput(i);
            LogicalPlan input = child.getOutput().equals(outputs)
                    ? child : new LogicalProject<>(ImmutableList.<NamedExpression>copyOf(outputs), child);
            LogicalPlan normalized = normalize(targets, input, sourceOrdinals, context);
            children.add(normalized);
            childOutputs.add(normalized.getOutput().stream().map(SlotReference.class::cast)
                    .collect(ImmutableList.toImmutableList()));
            changed |= normalized != child;
        }
        // Constant rows use the same proof and CAST validation as ordinary branches.
        // Keep them as separate rows; never replace an entire UNION output with one constant.
        for (List<NamedExpression> row : source.getConstantExprsList()) {
            LogicalPlan normalized = normalize(targets,
                    new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(), row),
                    sourceOrdinals, context);
            children.add(normalized);
            childOutputs.add(normalized.getOutput().stream().map(SlotReference.class::cast)
                    .collect(ImmutableList.toImmutableList()));
        }
        if (!changed) {
            return source;
        }
        List<NamedExpression> outputs = new ArrayList<>();
        for (int i = 0; i < source.getOutput().size(); i++) {
            SlotReference slot = (SlotReference) source.getOutput().get(i);
            DataType type = childOutputs.get(0).get(i).getDataType();
            // A forwarding project can repeat one slot for ordinary and spatial targets.
            // Retagged outputs need distinct identities, just like the scalar projection path.
            outputs.add(slot.getDataType().equals(type) ? slot : slot
                    .withExprId(StatementScopeIdGenerator.newExprId()).withNullableAndDataType(slot.nullable(), type));
        }
        return source.withNewOutputsChildrenAndConstExprsList(outputs, children, childOutputs, ImmutableList.of());
    }

    private static boolean isLiteralConstruction(Expression value) {
        // Only proven NULL-only casts can be retargeted; actual values and invalid source
        // array-depth conversions must retain their original identity and errors.
        return value instanceof Literal || (value.getClass() == Cast.class && nullArrayDepth(value) >= 0)
                || ((value instanceof Array || value instanceof CreateMap
                || value instanceof CreateStruct || value instanceof CreateNamedStruct)
                && value.children().stream().allMatch(ConnectorSpatialWriteNormalizer::isLiteralConstruction));
    }

    private static final class Resolver {
        private final CTEContext context;
        private final Map<CTEId, Plan> producers = new HashMap<>();
        private final Set<CTEId> activeCtes = new HashSet<>();
        private final Map<Plan, Map<ExprId, Expression>> resolvedSlots = new IdentityHashMap<>();

        private Resolver(Plan source, CTEContext context) {
            this.context = context;
            collectProducers(source);
        }

        private void collectProducers(Plan source) {
            if (source instanceof LogicalCTEProducer) {
                LogicalCTEProducer<?> producer = (LogicalCTEProducer<?>) source;
                producers.put(producer.getCteId(), producer.child());
            }
            source.children().forEach(this::collectProducers);
        }

        private Expression resolve(Expression value, Plan source) {
            if (value instanceof Alias) {
                return resolve(value.child(0), source);
            }
            if (value instanceof Slot) {
                return resolveSlot((Slot) value, source);
            }
            // Only restore constant NULL construction. Expanding arbitrary IF(g, g)
            // producer chains duplicates shared CTE lineage exponentially.
            if (!(value instanceof Cast || value instanceof Array || value instanceof CreateMap
                    || value instanceof CreateStruct || value instanceof CreateNamedStruct)) {
                return value;
            }
            List<Expression> children = value.children().stream()
                    .map(child -> resolve(child, source))
                    .collect(ImmutableList.toImmutableList());
            // SELECT reaches sink binding before constant folding: CAST(NULL AS INT)
            // and CAST(NULL AS GEOMETRY('EPSG:3857')) are still value-independent NULLs.
            if (value instanceof Cast && children.get(0).isNullLiteral()) {
                // A bound SELECT/UPDATE can still contain GEOGRAPHY -> INT. Retain its
                // analysis error before NULL retagging and project merging erase the cast.
                validateNullCastChain(value);
                return new NullLiteral(value.getDataType());
            }
            return children.equals(value.children()) ? value : value.withChildren(children);
        }

        private Expression resolveSlot(Slot slot, Plan source) {
            Map<ExprId, Expression> cached = resolvedSlots.computeIfAbsent(source, ignored -> new HashMap<>());
            Expression result = cached.get(slot.getExprId());
            if (result == null) {
                result = traceSlot(slot, source);
                cached.put(slot.getExprId(), result);
            }
            return result;
        }

        private Expression traceSlot(Slot slot, Plan source) {
            if (source instanceof LogicalProject || source instanceof LogicalOneRowRelation) {
                List<NamedExpression> projects = source instanceof LogicalProject
                        ? ((LogicalProject<?>) source).getProjects() : ((LogicalOneRowRelation) source).getProjects();
                for (NamedExpression project : projects) {
                    if (project.getExprId().equals(slot.getExprId())) {
                        Plan input = source instanceof LogicalProject ? source.child(0) : source;
                        // A one-row correlated slot has no producer definition here.
                        return project instanceof Slot && input == source
                                ? slot : resolve(project, input);
                    }
                }
            } else if (source instanceof LogicalCTEConsumer) {
                LogicalCTEConsumer consumer = (LogicalCTEConsumer) source;
                Optional<Plan> producer = Optional.ofNullable(producers.get(consumer.getCteId()));
                if (!producer.isPresent()) {
                    producer = context.findCTEContext(consumer.getName())
                            .filter(cte -> cte.getCteId().equals(consumer.getCteId()))
                            .flatMap(cte -> cte.getAnalyzedCTEPlan(consumer.getName())).map(Plan.class::cast);
                }
                // Recursive CTEs do not prove constant provenance through their cycle.
                if (producer.isPresent() && activeCtes.add(consumer.getCteId())) {
                    try {
                        // An analyzed outer CTE can own an inner WITH whose producers
                        // are absent from the sink's context and initial source tree.
                        collectProducers(producer.get());
                        return resolve(consumer.getProducerSlot(slot), producer.get());
                    } finally {
                        activeCtes.remove(consumer.getCteId());
                    }
                }
            } else if (source instanceof LogicalCTEAnchor) {
                return resolveSlot(slot, source.child(1));
            } else if (source instanceof LogicalSubQueryAlias || source instanceof LogicalFilter
                    || source instanceof LogicalLimit || source instanceof LogicalSort
                    || source instanceof LogicalCTE) {
                return resolveSlot(slot, source.child(0));
            }
            // Joins, grouping sets and set operations may add NULLs or combine values;
            // their output type alone is not evidence of a constant value.
            return slot;
        }
    }

    public static Expression normalizeSpatialNullsBeforeBinding(Expression input, DataType target) {
        return normalizeSpatialNullsBeforeBinding(input, target, false);
    }

    /** Retags proven NULLs in a bound write value while retaining its ordinary children. */
    public static Expression normalizeBoundSpatialNulls(Expression input, DataType target) {
        return canRetargetSpatialNulls(input, target, true)
                ? normalizeSpatialNullsBeforeBinding(input, target, true) : input;
    }

    /** Casts a MERGE branch in place so only the selected branch is evaluated. */
    public static Expression castSpatialMergeBranch(Expression input, DataType target) {
        Expression normalized = normalizeSpatialNullsBeforeBinding(input, target);
        if (normalized == input && input.getClass() == Cast.class && input.getDataType() instanceof MapType
                && target instanceof MapType && !input.getDataType().equals(target)
                && canRetargetSpatialNulls(input, target, true)) {
            // The key is still in the MERGE input scope. Let the analyzer validate the original
            // MAP cast before changing its NULL leaves and applying the sink type.
            return new SpatialMergeBranchCast(input, target);
        }
        return TypeCoercionUtils.castUnbound(normalized, target);
    }

    /** Analysis-only cast for the MERGE branch above. */
    public static final class SpatialMergeBranchCast extends Cast {
        private SpatialMergeBranchCast(Expression child, DataType target) {
            super(child, target);
        }

        @Override
        public Cast withChildren(List<Expression> children) {
            return new SpatialMergeBranchCast(children.get(0), getDataType());
        }
    }

    private static Expression normalizeSpatialNullsBeforeBinding(Expression input, DataType target,
            boolean sameProducer) {
        if (!target.toCatalogDataType().typeContainsSpatialType()) {
            return input;
        }
        Expression origin = input;
        while (origin instanceof Cast) {
            origin = origin.child(0);
        }
        if (origin.isNullLiteral()) {
            validateNullCastChain(input);
            return new NullLiteral(target);
        }
        // Bound container constructors can already be literals. Retain their non-NULL
        // containers and ordinary values while retagging only proven NULL spatial paths.
        if (input instanceof Literal && canRetargetSpatialNulls(input, target, sameProducer)) {
            return normalizeSpatialNulls(input, target);
        }
        // MERGE eagerly casts each branch to its sink type. An explicit ARRAY<INT>
        // cast around ARRAY(NULL) must not hide those NULL leaves from retagging.
        // Only NULL-only arrays are value-independent; keep casts of actual values intact.
        if (input.getClass() == Cast.class && input.getDataType() instanceof ArrayType
                && target instanceof ArrayType) {
            Cast cast = (Cast) input;
            int depth = nullArrayDepth(cast.child());
            // CAST(ARRAY(ARRAY(NULL)) AS ARRAY<INT>) must still fail: replacing its
            // source target must not erase a non-NULL inner array or a text conversion.
            if (depth >= 0 && depth <= arrayDepth(cast.getDataType()) && depth <= arrayDepth(target)) {
                // NULL leaves do not make ARRAY<GEOMETRY> -> ARRAY<INT> a legal user cast.
                // Bind the untouched container first to retain its constructor and CAST errors.
                ExpressionAnalyzer.analyzeFunction(null, null, cast);
                return new Cast(normalizeSpatialNullsBeforeBinding(cast.child(), target, sameProducer), target,
                        cast.isExplicitType(), cast.isStrict());
            }
        }
        if (input.getClass() == Cast.class && canRetargetSpatialNulls(input, target, sameProducer)) {
            Cast cast = (Cast) input;
            // MAP/STRUCT NULL retagging must also preserve the original container's errors.
            ExpressionAnalyzer.analyzeFunction(null, null, cast);
            // CAST(MAP('k', NULL:INT) AS MAP<STRING,INT>) hides NULL provenance.
            // Patch only spatial paths: replacing ordinary key/field types could erase
            // an intermediate cast error such as STRING -> TINYINT -> STRING.
            DataType patched = spatialLiteralType(cast.getDataType(), target);
            return new Cast(normalizeSpatialNullsBeforeBinding(cast.child(), patched, sameProducer), patched,
                    cast.isExplicitType(), cast.isStrict());
        }
        String constructor = "";
        if (input instanceof UnboundFunction) {
            UnboundFunction function = (UnboundFunction) input;
            // Qualified or DISTINCT/skew calls are not these built-in container constructors.
            if (function.getDbName() != null || function.isDistinct() || function.isSkew()) {
                return input;
            }
            constructor = function.getName();
        }
        List<Expression> children = new ArrayList<>(input.children());
        if ((input instanceof Array || constructor.equalsIgnoreCase("array")) && target instanceof ArrayType) {
            children.replaceAll(child -> normalizeSpatialNullsBeforeBinding(child,
                    ((ArrayType) target).getItemType(), sameProducer));
        } else if ((input instanceof CreateMap || constructor.equalsIgnoreCase("map")) && target instanceof MapType
                && input.arity() % 2 == 0) {
            MapType map = (MapType) target;
            for (int i = 0; i < children.size(); i++) {
                children.set(i, normalizeSpatialNullsBeforeBinding(children.get(i),
                        i % 2 == 0 ? map.getKeyType() : map.getValueType(), sameProducer));
            }
        } else if (target instanceof StructType) {
            boolean named = input instanceof CreateNamedStruct || constructor.equalsIgnoreCase("named_struct");
            int step = named ? 2 : 1;
            StructType struct = (StructType) target;
            // Wrong field counts must retain the original binder's error, not index a target field.
            if (!(named || input instanceof CreateStruct || constructor.equalsIgnoreCase("struct"))
                    || input.arity() != step * struct.getFields().size()) {
                return input;
            }
            for (int i = step - 1; i < children.size(); i += step) {
                children.set(i, normalizeSpatialNullsBeforeBinding(children.get(i),
                        struct.getFields().get(i / step).getDataType(), sameProducer));
            }
        }
        if (children.equals(input.children())) {
            return input;
        }
        return rebuildConstructor(input, children);
    }

    private static void validateNullCastChain(Expression input) {
        if (input instanceof Cast) {
            Cast cast = (Cast) input;
            validateNullCastChain(cast.child());
            // NULL provenance must not erase an unsupported cast such as GEOGRAPHY -> INT.
            TypeCoercionUtils.checkCanCastTo(cast.child().getDataType(), cast.getDataType());
        }
    }

    private static Expression rebuildConstructor(Expression input, List<Expression> children) {
        // Bound withChildren preserves a cached signature such as ARRAY<INT>.
        // New typed NULL leaves must recompute the built-in constructor's signature.
        if (input instanceof Array) {
            return new Array(children);
        } else if (input instanceof CreateMap) {
            return new CreateMap(children.toArray(new Expression[0]));
        } else if (input instanceof CreateNamedStruct) {
            return new CreateNamedStruct(children.toArray(new Expression[0]));
        } else if (input instanceof CreateStruct) {
            return new CreateStruct(children.toArray(new Expression[0]));
        }
        return input.withChildren(children);
    }

    private static boolean canRetargetSpatialNulls(Expression value, DataType target) {
        return canRetargetSpatialNulls(value, target, false);
    }

    /** Proves NULL on spatial paths; ordinary expressions can stay only in their own producer. */
    private static boolean canRetargetSpatialNulls(Expression value, DataType target, boolean sameProducer) {
        if (!target.toCatalogDataType().typeContainsSpatialType()) {
            return sameProducer || isLiteralConstruction(value) || (value.getClass() == Cast.class
                    && canRetargetSpatialNulls(value.child(0), target, false));
        }
        if (value.isNullLiteral()) {
            return true;
        }
        if (value.getClass() == Cast.class) {
            Cast cast = (Cast) value;
            // A text bridge or incompatible container shape must retain its cast error.
            return sameSpatialContainerShape(cast.getDataType(), target)
                    && canRetargetSpatialNulls(cast.child(), spatialLiteralType(cast.getDataType(), target), sameProducer);
        }
        String constructor = "";
        if (value instanceof UnboundFunction) {
            UnboundFunction function = (UnboundFunction) value;
            if (function.getDbName() != null || function.isDistinct() || function.isSkew()) {
                return false;
            }
            constructor = function.getName();
        }
        if (target instanceof ArrayType && (value instanceof ArrayLiteral || value instanceof Array
                || constructor.equalsIgnoreCase("array"))) {
            List<? extends Expression> elements = value instanceof ArrayLiteral
                    ? ((ArrayLiteral) value).getValue() : value.children();
            return elements.stream().allMatch(child ->
                    canRetargetSpatialNulls(child, ((ArrayType) target).getItemType(), sameProducer));
        }
        if (target instanceof MapType) {
            MapType map = (MapType) target;
            if (value instanceof MapLiteral) {
                return ((MapLiteral) value).getValue().entrySet().stream().allMatch(entry ->
                        canRetargetSpatialNulls(entry.getKey(), map.getKeyType(), sameProducer)
                                && canRetargetSpatialNulls(entry.getValue(), map.getValueType(), sameProducer));
            }
            if ((value instanceof CreateMap || constructor.equalsIgnoreCase("map")) && value.arity() % 2 == 0) {
                return IntStream.range(0, value.arity()).allMatch(i -> canRetargetSpatialNulls(value.child(i),
                        i % 2 == 0 ? map.getKeyType() : map.getValueType(), sameProducer));
            }
        } else if (target instanceof StructType) {
            StructType struct = (StructType) target;
            boolean named = value instanceof CreateNamedStruct || constructor.equalsIgnoreCase("named_struct");
            List<? extends Expression> fields = value instanceof StructLiteral
                    ? ((StructLiteral) value).getValue() : value.children();
            int step = named ? 2 : 1;
            if ((value instanceof StructLiteral || value instanceof CreateStruct || named
                    || constructor.equalsIgnoreCase("struct")) && fields.size() == step * struct.getFields().size()) {
                // Match the constructor's literal-name contract before scope-free validation.
                return IntStream.range(0, struct.getFields().size()).allMatch(i ->
                        (!named || fields.get(i * step) instanceof StringLikeLiteral)
                                && canRetargetSpatialNulls(fields.get(i * step + step - 1),
                                        struct.getFields().get(i).getDataType(), sameProducer));
            }
        }
        return false;
    }

    private static boolean sameSpatialContainerShape(DataType source, DataType target) {
        if (!target.toCatalogDataType().typeContainsSpatialType()) {
            return true;
        }
        if (target instanceof GeometryType || target instanceof GeographyType) {
            return !source.isComplexType();
        }
        if (source instanceof ArrayType && target instanceof ArrayType) {
            return sameSpatialContainerShape(((ArrayType) source).getItemType(), ((ArrayType) target).getItemType());
        }
        if (source instanceof MapType && target instanceof MapType) {
            return sameSpatialContainerShape(((MapType) source).getKeyType(), ((MapType) target).getKeyType())
                    && sameSpatialContainerShape(((MapType) source).getValueType(), ((MapType) target).getValueType());
        }
        if (source instanceof StructType && target instanceof StructType) {
            StructType from = (StructType) source;
            StructType to = (StructType) target;
            return from.getFields().size() == to.getFields().size()
                    && IntStream.range(0, from.getFields().size()).allMatch(i -> sameSpatialContainerShape(
                            from.getFields().get(i).getDataType(), to.getFields().get(i).getDataType()));
        }
        return false;
    }

    /** Minimum array depth of a proven NULL-only value; -1 means the proof failed. */
    private static int nullArrayDepth(Expression value) {
        Expression origin = value;
        while (origin instanceof Cast) {
            origin = origin.child(0);
        }
        if (origin.isNullLiteral()) {
            return 0;
        }
        if (value.getClass() == Cast.class) {
            // Do not cross ARRAY -> STRING -> ARRAY: text parsing is not NULL provenance.
            int depth = nullArrayDepth(value.child(0));
            return depth >= 0 && depth <= arrayDepth(value.getDataType()) ? depth : -1;
        }
        List<? extends Expression> children;
        if (value instanceof ArrayLiteral) {
            children = ((ArrayLiteral) value).getValue();
        } else if (value instanceof Array) {
            children = value.children();
        } else if (value instanceof UnboundFunction) {
            UnboundFunction function = (UnboundFunction) value;
            if (function.getDbName() != null || function.isDistinct() || function.isSkew()
                    || !function.getName().equalsIgnoreCase("array")) {
                return -1;
            }
            children = function.children();
        } else {
            return -1;
        }
        int depth = 0;
        for (Expression child : children) {
            int childDepth = nullArrayDepth(child);
            if (childDepth < 0) {
                return -1;
            }
            depth = Math.max(depth, childDepth);
        }
        return depth + 1;
    }

    private static int arrayDepth(DataType type) {
        int depth = 0;
        while (type instanceof ArrayType) {
            depth++;
            type = ((ArrayType) type).getItemType();
        }
        return depth;
    }

    public static Expression normalizeSpatialNulls(Expression input, DataType target) {
        if (!target.toCatalogDataType().typeContainsSpatialType()) {
            return input;
        }
        if (input.isNullLiteral()) {
            return new NullLiteral(target);
        }
        if (input instanceof Literal) {
            // Preserve ordinary types and source field nullability for the subsequent normal CAST.
            return ((Literal) input).checkedCastTo(spatialLiteralType(input.getDataType(), target));
        }
        // Binding folds CAST(NULL), but does not fold every container constructor to a literal.
        // Rebuild just these constructors after identity validation, retaining names and ordinary siblings.
        if (input instanceof Array && target instanceof ArrayType) {
            return rebuildConstructor(input, input.children().stream()
                    .map(child -> normalizeSpatialNulls(child, ((ArrayType) target).getItemType()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if (input instanceof CreateMap && target instanceof MapType) {
            MapType map = (MapType) target;
            return rebuildConstructor(input, IntStream.range(0, input.arity()).mapToObj(i -> normalizeSpatialNulls(
                    input.child(i), i % 2 == 0 ? map.getKeyType() : map.getValueType()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if ((input instanceof CreateStruct || input instanceof CreateNamedStruct) && target instanceof StructType) {
            StructType struct = (StructType) target;
            int step = input instanceof CreateNamedStruct ? 2 : 1;
            List<Expression> children = IntStream.range(0, input.arity())
                    .mapToObj(i -> step == 2 && i % 2 == 0 ? input.child(i)
                            : normalizeSpatialNulls(input.child(i), struct.getFields().get(i / step).getDataType()))
                    .collect(ImmutableList.toImmutableList());
            return rebuildConstructor(input, children);
        }
        return input;
    }

    private static DataType spatialLiteralType(DataType source, DataType target) {
        // Ordinary sibling structs may have incompatible shapes; leave them to normal CAST validation.
        if (!target.toCatalogDataType().typeContainsSpatialType()) {
            return source;
        }
        if (target instanceof GeometryType || target instanceof GeographyType) {
            return target;
        } else if (source instanceof ArrayType && target instanceof ArrayType) {
            return ArrayType.of(spatialLiteralType(((ArrayType) source).getItemType(),
                    ((ArrayType) target).getItemType()));
        } else if (source instanceof MapType && target instanceof MapType) {
            return MapType.of(spatialLiteralType(((MapType) source).getKeyType(), ((MapType) target).getKeyType()),
                    spatialLiteralType(((MapType) source).getValueType(), ((MapType) target).getValueType()));
        } else if (source instanceof StructType && target instanceof StructType) {
            StructType sourceStruct = (StructType) source;
            StructType targetStruct = (StructType) target;
            // A typed NULL element may carry a different struct shape; retain its type for CAST.
            if (sourceStruct.getFields().size() != targetStruct.getFields().size()) {
                return source;
            }
            return new StructType(IntStream.range(0, sourceStruct.getFields().size())
                    .mapToObj(i -> sourceStruct.getFields().get(i).withDataType(spatialLiteralType(
                            sourceStruct.getFields().get(i).getDataType(),
                            targetStruct.getFields().get(i).getDataType())))
                    .collect(ImmutableList.toImmutableList()));
        }
        return source;
    }

}
