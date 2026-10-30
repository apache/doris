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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
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
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;
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
        Resolver resolver = new Resolver(source, cteContext);
        List<NamedExpression> outputs = new ArrayList<>(source.getOutput());
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
        return changed ? new LogicalProject<>(outputs, source) : source;
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
        if (input instanceof Literal && canRetargetSpatialNulls(input, target)) {
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
                return new Cast(normalizeSpatialNullsBeforeBinding(cast.child(), target), target,
                        cast.isExplicitType(), cast.isStrict());
            }
        }
        if (input.getClass() == Cast.class && canRetargetSpatialNulls(input, target)) {
            Cast cast = (Cast) input;
            // CAST(MAP('k', NULL:INT) AS MAP<STRING,INT>) hides NULL provenance.
            // Patch only spatial paths: replacing ordinary key/field types could erase
            // an intermediate cast error such as STRING -> TINYINT -> STRING.
            DataType patched = spatialLiteralType(cast.getDataType(), target);
            return new Cast(normalizeSpatialNullsBeforeBinding(cast.child(), patched), patched,
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
                    ((ArrayType) target).getItemType()));
        } else if ((input instanceof CreateMap || constructor.equalsIgnoreCase("map")) && target instanceof MapType
                && input.arity() % 2 == 0) {
            MapType map = (MapType) target;
            for (int i = 0; i < children.size(); i++) {
                children.set(i, normalizeSpatialNullsBeforeBinding(children.get(i),
                        i % 2 == 0 ? map.getKeyType() : map.getValueType()));
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
                        struct.getFields().get(i / step).getDataType()));
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

    /** Proves NULL on spatial paths without lifting dynamic ordinary siblings. */
    private static boolean canRetargetSpatialNulls(Expression value, DataType target) {
        if (!target.toCatalogDataType().typeContainsSpatialType()) {
            // Preserve a constant ordinary cast rather than evaluating or removing it.
            // A Slot/UDF sibling still cannot be lifted out of its producer scope.
            return isLiteralConstruction(value) || (value.getClass() == Cast.class
                    && canRetargetSpatialNulls(value.child(0), target));
        }
        if (value.isNullLiteral()) {
            return true;
        }
        if (value.getClass() == Cast.class) {
            Cast cast = (Cast) value;
            // A text bridge or incompatible container shape must retain its cast error.
            return sameSpatialContainerShape(cast.getDataType(), target)
                    && canRetargetSpatialNulls(cast.child(), spatialLiteralType(cast.getDataType(), target));
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
                    canRetargetSpatialNulls(child, ((ArrayType) target).getItemType()));
        }
        if (target instanceof MapType) {
            MapType map = (MapType) target;
            if (value instanceof MapLiteral) {
                return ((MapLiteral) value).getValue().entrySet().stream().allMatch(entry ->
                        canRetargetSpatialNulls(entry.getKey(), map.getKeyType())
                                && canRetargetSpatialNulls(entry.getValue(), map.getValueType()));
            }
            if ((value instanceof CreateMap || constructor.equalsIgnoreCase("map")) && value.arity() % 2 == 0) {
                return IntStream.range(0, value.arity()).allMatch(i -> canRetargetSpatialNulls(value.child(i),
                        i % 2 == 0 ? map.getKeyType() : map.getValueType()));
            }
        } else if (target instanceof StructType) {
            StructType struct = (StructType) target;
            boolean named = value instanceof CreateNamedStruct || constructor.equalsIgnoreCase("named_struct");
            List<? extends Expression> fields = value instanceof StructLiteral
                    ? ((StructLiteral) value).getValue() : value.children();
            int step = named ? 2 : 1;
            if ((value instanceof StructLiteral || value instanceof CreateStruct || named
                    || constructor.equalsIgnoreCase("struct")) && fields.size() == step * struct.getFields().size()) {
                return IntStream.range(0, struct.getFields().size()).allMatch(i -> canRetargetSpatialNulls(
                        fields.get(i * step + step - 1), struct.getFields().get(i).getDataType()));
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
