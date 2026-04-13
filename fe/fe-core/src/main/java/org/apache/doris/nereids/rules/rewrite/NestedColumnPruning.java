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

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.ColumnAccessPathType;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <li> 1. prune the data type of struct/map
 *
 * <p> for example, column s is a struct&lt;id: int, value: double&gt;,
 *     and `select struct(s, 'id') from tbl` will return the data type: `struct&lt;id: int&gt;`
 * </p>
 * </li>
 *
 * <li> 2. collect the access paths
 * <p> for example, select struct(s, 'id'), struct(s, 'data') from tbl` will collect the access path:
 *     [s.id, s.data]
 * </p>
 * </li>
 */
public class NestedColumnPruning implements CustomRewriter {
    public static final Logger LOG = LogManager.getLogger(NestedColumnPruning.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        try {
            StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
            SessionVariable sessionVariable = statementContext.getConnectContext().getSessionVariable();
            if (!sessionVariable.enablePruneNestedColumns
                    || (!statementContext.hasNestedColumns()
                        && !containsVariant(plan)
                        && !(containsStringLength(plan)))) {
                return plan;
            }

            AccessPathPlanCollector collector = new AccessPathPlanCollector();
            Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths = collector.collect(plan, statementContext);
            Map<Integer, AccessPathInfo> slotToResult = pruneDataType(slotToAccessPaths);

            if (!slotToResult.isEmpty()) {
                Map<Integer, AccessPathInfo> slotIdToPruneType = Maps.newLinkedHashMap();
                for (Entry<Integer, AccessPathInfo> kv : slotToResult.entrySet()) {
                    Integer slotId = kv.getKey();
                    AccessPathInfo accessPathInfo = kv.getValue();
                    slotIdToPruneType.put(slotId, accessPathInfo);
                }
                return new SlotTypeReplacer(slotIdToPruneType, plan).replace();
            }
            return plan;
        } catch (Throwable t) {
            LOG.warn("NestedColumnPruning failed.", t);
            return plan;
        }
    }

    /** Returns true when the plan tree contains length() applied to a string-type expression.
     *  Used in the early-exit guard so that string offset optimizations are not skipped even
     *  when no nested (struct/array/map) or variant columns are present. */
    private static boolean containsStringLength(Plan plan) {
        AtomicBoolean found = new AtomicBoolean(false);
        plan.foreachUp(node -> {
            if (found.get()) {
                return;
            }
            Plan current = (Plan) node;
            for (Expression expression : current.getExpressions()) {
                if (expressionContainsStringLength(expression)) {
                    found.set(true);
                    return;
                }
            }
        });
        return found.get();
    }

    private static boolean expressionContainsStringLength(Expression expr) {
        if (expr instanceof Length && expr.child(0).getDataType().isStringLikeType()) {
            return true;
        }
        if (expr instanceof Cardinality) {
            DataType argType = expr.child(0).getDataType();
            if (argType.isArrayType() || argType.isMapType()) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (expressionContainsStringLength(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsVariant(Plan plan) {
        AtomicBoolean hasVariant = new AtomicBoolean(false);
        plan.foreachUp(node -> {
            if (hasVariant.get()) {
                return;
            }
            Plan current = (Plan) node;
            for (Expression expression : current.getExpressions()) {
                if (expression.getDataType().isVariantType()
                        || expression.getInputSlots().stream()
                                .anyMatch(slot -> slot.getDataType().isVariantType())) {
                    hasVariant.set(true);
                    return;
                }
            }
        });
        return hasVariant.get();
    }

    private static Map<Integer, AccessPathInfo> pruneDataType(
            Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths) {
        Map<Integer, AccessPathInfo> result = new LinkedHashMap<>();
        Map<Slot, DataTypeAccessTree> slotIdToAllAccessTree = new LinkedHashMap<>();
        Map<Slot, DataTypeAccessTree> slotIdToPredicateAccessTree = new LinkedHashMap<>();
        Map<Slot, DataType> variantSlots = new LinkedHashMap<>();

        Comparator<Pair<ColumnAccessPathType, List<String>>> pathComparator = Comparator.comparing(
                l -> StringUtils.join(l.second, "."));

        Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> allAccessPaths = TreeMultimap.create(
                Comparator.naturalOrder(), pathComparator);
        Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> predicateAccessPaths = TreeMultimap.create(
                Comparator.naturalOrder(), pathComparator);

        // first: build access data type tree
        for (Entry<Slot, List<CollectAccessPathResult>> kv : slotToAccessPaths.entrySet()) {
            Slot slot = kv.getKey();
            List<CollectAccessPathResult> collectAccessPathResults = kv.getValue();
            if (slot.getDataType() instanceof VariantType) {
                variantSlots.put(slot, slot.getDataType());
                for (CollectAccessPathResult collectAccessPathResult : collectAccessPathResults) {
                    List<String> path = collectAccessPathResult.getPath();
                    ColumnAccessPathType pathType = collectAccessPathResult.getType();
                    allAccessPaths.put(slot.getExprId().asInt(), Pair.of(pathType, path));
                    if (collectAccessPathResult.isPredicate()) {
                        predicateAccessPaths.put(
                                slot.getExprId().asInt(), Pair.of(pathType, path)
                        );
                    }
                }
                continue;
            }
            for (CollectAccessPathResult collectAccessPathResult : collectAccessPathResults) {
                List<String> path = collectAccessPathResult.getPath();
                ColumnAccessPathType pathType = collectAccessPathResult.getType();
                DataTypeAccessTree allAccessTree = slotIdToAllAccessTree.computeIfAbsent(
                        slot, i -> DataTypeAccessTree.ofRoot(slot, pathType)
                );
                allAccessTree.setAccessByPath(path, 0, pathType);
                allAccessPaths.put(slot.getExprId().asInt(), Pair.of(pathType, path));

                if (collectAccessPathResult.isPredicate()) {
                    DataTypeAccessTree predicateAccessTree = slotIdToPredicateAccessTree.computeIfAbsent(
                            slot, i -> DataTypeAccessTree.ofRoot(slot, pathType)
                    );
                    predicateAccessTree.setAccessByPath(path, 0, pathType);
                    predicateAccessPaths.put(
                            slot.getExprId().asInt(), Pair.of(pathType, path)
                    );
                }
            }
        }

        // second: build non-predicate access paths
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToAllAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            DataTypeAccessTree accessTree = kv.getValue();
            DataType prunedDataType = accessTree.pruneDataType().orElse(slot.getDataType());

            if (slot.getDataType().isStringLikeType()) {
                if (accessTree.hasStringOffsetOnlyAccess()) {
                    // Offset-only access (e.g. length(str_col)): type stays varchar,
                    // but we must still send the access path to BE so it skips the char data.
                    List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
                    result.put(slot.getExprId().asInt(),
                            new AccessPathInfo(slot.getDataType(), allPaths, new ArrayList<>()));
                }
                // direct access (accessAll=true) or other: skip — no type change, no access paths needed.
                continue;
            }

            if ((slot.getDataType().isArrayType() || slot.getDataType().isMapType())
                    && accessTree.hasStringOffsetOnlyAccess()) {
                // Offset-only access (e.g. length(arr_col) / length(map_col)): type stays unchanged,
                // but we must send the OFFSET access path to BE so it skips element/key-value data.
                List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
                result.put(slot.getExprId().asInt(),
                        new AccessPathInfo(slot.getDataType(), allPaths, new ArrayList<>()));
                continue;
            }

            if (slot.getDataType().isMapType() && accessTree.hasMapValueOffsetOnlyAccess()) {
                // length(map_col['key']): keys read in full (element lookup) + values offset-only.
                // Emit [col, KEYS] and [col, VALUES, OFFSET] directly instead of the collected
                // [col, *, OFFSET] path which the BE cannot interpret for split key/value access.
                String colName = slot.getName().toLowerCase();
                ColumnAccessPath keysColumnPath = ColumnAccessPath.data(
                        new ArrayList<>(ImmutableList.of(colName, AccessPathInfo.ACCESS_MAP_KEYS)));

                ColumnAccessPath valsOffsetColumnPath = ColumnAccessPath.data(
                        new ArrayList<>(ImmutableList.of(colName, AccessPathInfo.ACCESS_MAP_VALUES,
                                AccessPathInfo.ACCESS_STRING_OFFSET)));

                result.put(slot.getExprId().asInt(), new AccessPathInfo(
                        slot.getDataType(),
                        ImmutableList.of(keysColumnPath, valsOffsetColumnPath),
                        new ArrayList<>()));
                continue;
            }

            // For array/map columns that are NOT in offset-only mode, strip OFFSET-suffix paths
            // when a non-OFFSET path also exists for the same slot. This handles cases like
            // `select cardinality(arr), arr[1]` where the OFFSET path from cardinality() is
            // redundant because full element data is also needed.
            // If the ONLY paths for a slot end in OFFSET (e.g. cardinality(arr[0].field) alone),
            // keep them — they carry meaningful nested-access semantics.
            if (slot.getDataType().isArrayType() || slot.getDataType().isMapType()) {
                int slotId = slot.getExprId().asInt();
                boolean hasNonOffsetPath = allAccessPaths.get(slotId).stream().anyMatch(p -> {
                    List<String> path = p.second;
                    return path.isEmpty()
                            || !AccessPathInfo.ACCESS_STRING_OFFSET.equals(path.get(path.size() - 1));
                });
                if (hasNonOffsetPath) {
                    allAccessPaths.get(slotId).removeIf(p -> {
                        List<String> path = p.second;
                        return !path.isEmpty()
                                && AccessPathInfo.ACCESS_STRING_OFFSET.equals(
                                        path.get(path.size() - 1));
                    });
                }
            }
            List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
            result.put(slot.getExprId().asInt(),
                    new AccessPathInfo(prunedDataType, allPaths, new ArrayList<>()));
        }

        for (Entry<Slot, DataType> kv : variantSlots.entrySet()) {
            Slot slot = kv.getKey();
            List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
            result.put(slot.getExprId().asInt(),
                    new AccessPathInfo(slot.getDataType(), allPaths, new ArrayList<>()));
        }

        // third: build predicate access path
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToPredicateAccessTree.entrySet()) {
            Slot slot = kv.getKey();

            List<ColumnAccessPath> predicatePaths =
                    buildColumnAccessPaths(slot, predicateAccessPaths);
            AccessPathInfo accessPathInfo = result.get(slot.getExprId().asInt());
            if (accessPathInfo != null) {
                accessPathInfo.getPredicateAccessPaths().addAll(predicatePaths);
            }
        }

        for (Entry<Slot, DataType> kv : variantSlots.entrySet()) {
            Slot slot = kv.getKey();
            List<ColumnAccessPath> predicatePaths =
                    buildColumnAccessPaths(slot, predicateAccessPaths);
            AccessPathInfo accessPathInfo = result.get(slot.getExprId().asInt());
            if (accessPathInfo != null) {
                accessPathInfo.getPredicateAccessPaths().addAll(predicatePaths);
            }
        }

        return result;
    }

    private static List<ColumnAccessPath> buildColumnAccessPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> accessPaths) {
        List<ColumnAccessPath> paths = new ArrayList<>();
        boolean accessWholeColumn = false;
        ColumnAccessPathType accessWholeColumnType = ColumnAccessPathType.META;
        for (Pair<ColumnAccessPathType, List<String>> pathInfo : accessPaths.get(slot.getExprId().asInt())) {
            if (pathInfo == null) {
                throw new AnalysisException("This is a bug, please report this");
            }
            paths.add(new ColumnAccessPath(pathInfo.first, new ArrayList<>(pathInfo.second)));
            // only retain access the whole root
            if (pathInfo.second.size() == 1) {
                accessWholeColumn = true;
                if (pathInfo.first == ColumnAccessPathType.DATA) {
                    accessWholeColumnType = ColumnAccessPathType.DATA;
                }
            } else {
                accessWholeColumnType = ColumnAccessPathType.DATA;
            }
        }
        if (accessWholeColumn) {
            SlotReference slotReference = (SlotReference) slot;
            String wholeColumnName = slotReference.getOriginalColumn().get().getName();
            return ImmutableList.of(new ColumnAccessPath(accessWholeColumnType, ImmutableList.of(wholeColumnName)));
        }
        return paths;
    }

    /** DataTypeAccessTree */
    public static class DataTypeAccessTree {
        // type of this level
        private DataType type;
        // is the root column?
        private boolean isRoot;
        // if access 's.a.b' the node 's' and 'a' has accessPartialChild, and node 'b' has accessAll
        private boolean accessPartialChild;
        private boolean accessAll;
        // True when this string-typed node is accessed ONLY via the offset array
        // (e.g. length(str_col) or length(element_at(c_struct,'f3'))).
        // When this flag is set and accessAll is NOT set, pruneDataType() returns BigIntType
        // to signal that the BE only needs to read the offset array, not the chars data.
        private boolean isStringOffsetOnly;
        // for the future, only access the meta of the column,
        // e.g. `is not null` can only access the column's offset, not need to read the data
        private ColumnAccessPathType pathType;
        // the children of the column, for example, column s is `struct<a:int, b:int>`,
        // then node 's' has two children: 'a' and 'b', and the key is the column name
        private Map<String, DataTypeAccessTree> children = new LinkedHashMap<>();

        public DataTypeAccessTree(DataType type, ColumnAccessPathType pathType) {
            this(false, type, pathType);
        }

        public DataTypeAccessTree(boolean isRoot, DataType type, ColumnAccessPathType pathType) {
            this.isRoot = isRoot;
            this.type = type;
            this.pathType = pathType;
        }

        public DataType getType() {
            return type;
        }

        public boolean isRoot() {
            return isRoot;
        }

        public boolean isAccessPartialChild() {
            return accessPartialChild;
        }

        public boolean isAccessAll() {
            return accessAll;
        }

        public ColumnAccessPathType getPathType() {
            return pathType;
        }

        public Map<String, DataTypeAccessTree> getChildren() {
            return children;
        }

        /**
         * True when a MAP column is accessed as {@code length(map_col['key'])}: the keys must
         * be read in full (for the element lookup) while the values only need the offset array
         * (since only their length, not their content, is used).
         * Expected access paths: [col, KEYS] and [col, VALUES, OFFSET].
         */
        public boolean hasMapValueOffsetOnlyAccess() {
            if (!isRoot) {
                return false;
            }
            DataTypeAccessTree child = children.values().iterator().next();
            if (!child.type.isMapType() || child.accessAll) {
                return false;
            }
            DataTypeAccessTree keysChild = child.children.get(AccessPathInfo.ACCESS_MAP_KEYS);
            DataTypeAccessTree valsChild = child.children.get(AccessPathInfo.ACCESS_MAP_VALUES);
            // Keys must be fully accessed (element-at lookup).
            if (!keysChild.accessAll) {
                return false;
            }
            // Values must be accessed offset-only (no deeper element reads).
            if (!valsChild.isStringOffsetOnly || valsChild.accessAll) {
                return false;
            }
            if (valsChild.type.isStringLikeType()) {
                // String value: accessAll check above is sufficient.
                return true;
            }
            if (valsChild.type.isArrayType()) {
                // Array value (e.g. MAP<STRING, ARRAY<INT>>): verify no element was read directly
                // (e.g. map_col['k'][0] would set allChild.accessAll=true).
                DataTypeAccessTree allChild = valsChild.children.get(AccessPathInfo.ACCESS_ALL);
                return !allChild.accessAll && !allChild.accessPartialChild;
            }
            return true;
        }

        /** True when the column is accessed ONLY via the offset array (e.g. length(str_col),
         *  length(arr_col), length(map_col)), meaning the type must not change but an access
         *  path still needs to be sent to BE so it can skip the char/element data. */
        public boolean hasStringOffsetOnlyAccess() {
            if (isRoot) {
                DataTypeAccessTree child = children.values().iterator().next();
                if (!child.isStringOffsetOnly || child.accessAll) {
                    return false;
                }
                if (child.type.isStringLikeType()) {
                    return true;
                }
                if (child.type.isArrayType()) {
                    // True only if no element was accessed (element_at / explode etc.)
                    DataTypeAccessTree allChild = child.children.get(AccessPathInfo.ACCESS_ALL);
                    return !allChild.accessAll && !allChild.accessPartialChild;
                }
                if (child.type.isMapType()) {
                    // True only if neither keys nor values were accessed directly
                    DataTypeAccessTree keysChild = child.children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                    DataTypeAccessTree valsChild = child.children.get(AccessPathInfo.ACCESS_MAP_VALUES);
                    return !keysChild.accessAll && !keysChild.accessPartialChild
                            && !valsChild.accessAll && !valsChild.accessPartialChild;
                }
                return false;
            }
            return type.isStringLikeType() && isStringOffsetOnly && !accessAll;
        }

        /** pruneCastType */
        public DataType pruneCastType(DataTypeAccessTree origin, DataTypeAccessTree cast) {
            if (type instanceof StructType) {
                Map<String, String> nameMapping = new LinkedHashMap<>();
                List<String> castNames = new ArrayList<>(cast.children.keySet());
                int i = 0;
                for (String s : origin.children.keySet()) {
                    nameMapping.put(s, castNames.get(i++));
                }
                List<StructField> mappingFields = new ArrayList<>();
                StructType originCastStructType = (StructType) cast.type;
                for (Entry<String, DataTypeAccessTree> kv : children.entrySet()) {
                    String originName = kv.getKey();
                    String mappingName = nameMapping.getOrDefault(originName, originName);
                    DataTypeAccessTree originPrunedTree = kv.getValue();
                    DataType mappingType = originPrunedTree.pruneCastType(
                            origin.children.get(originName),
                            cast.children.get(mappingName)
                    );
                    StructField originCastField = originCastStructType.getField(mappingName);
                    mappingFields.add(
                            new StructField(mappingName, mappingType,
                                    originCastField.isNullable(), originCastField.getComment())
                    );
                }
                return new StructType(mappingFields);
            } else if (type instanceof ArrayType) {
                return ArrayType.of(
                        children.values().iterator().next().pruneCastType(
                                origin.children.values().iterator().next(),
                                cast.children.values().iterator().next()
                        ),
                        ((ArrayType) cast.type).containsNull()
                );
            } else if (type instanceof MapType) {
                return MapType.of(
                        children.get(AccessPathInfo.ACCESS_MAP_KEYS)
                                .pruneCastType(
                                        origin.children.get(AccessPathInfo.ACCESS_MAP_KEYS),
                                        cast.children.get(AccessPathInfo.ACCESS_MAP_KEYS)
                                ),
                        children.get(AccessPathInfo.ACCESS_MAP_VALUES)
                                .pruneCastType(
                                        origin.children.get(AccessPathInfo.ACCESS_MAP_VALUES),
                                        cast.children.get(AccessPathInfo.ACCESS_MAP_VALUES)
                                )
                );
            } else {
                return cast.type;
            }
        }

        /** replacePathByAnotherTree */
        public boolean replacePathByAnotherTree(DataTypeAccessTree cast, List<String> path, int index) {
            if (index >= path.size()) {
                return true;
            }
            if (cast.type instanceof StructType) {
                List<StructField> fields = ((StructType) cast.type).getFields();
                for (int i = 0; i < fields.size(); i++) {
                    String castFieldName = path.get(index);
                    if (fields.get(i).getName().equalsIgnoreCase(castFieldName)) {
                        String originFieldName = ((StructType) type).getFields().get(i).getName();
                        path.set(index, originFieldName);
                        return children.get(originFieldName).replacePathByAnotherTree(
                                cast.children.get(castFieldName), path, index + 1
                        );
                    }
                }
            } else if (cast.type instanceof ArrayType) {
                return children.values().iterator().next().replacePathByAnotherTree(
                        cast.children.values().iterator().next(), path, index + 1);
            } else if (cast.type instanceof MapType) {
                String fieldName = path.get(index);
                return children.get(AccessPathInfo.ACCESS_MAP_VALUES).replacePathByAnotherTree(
                        cast.children.get(fieldName), path, index + 1
                );
            }
            return false;
        }

        /** setAccessByPath */
        public void setAccessByPath(List<String> path, int accessIndex, ColumnAccessPathType pathType) {
            if (accessIndex >= path.size()) {
                accessAll = true;
                return;
            } else {
                accessPartialChild = true;
            }

            if (pathType == ColumnAccessPathType.DATA) {
                this.pathType = ColumnAccessPathType.DATA;
            }

            if (this.type.isStructType()) {
                String fieldName = path.get(accessIndex).toLowerCase();
                DataTypeAccessTree child = children.get(fieldName);
                if (child != null) {
                    child.setAccessByPath(path, accessIndex + 1, pathType);
                } else {
                    // can not find the field
                    accessAll = true;
                }
                return;
            } else if (this.type.isArrayType()) {
                if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_STRING_OFFSET)) {
                    // length(array_col) — only the offset array is needed, not element data.
                    isStringOffsetOnly = true;
                    return;
                }
                DataTypeAccessTree child = children.get(AccessPathInfo.ACCESS_ALL);
                if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_ALL)) {
                    // enter this array and skip next *
                    child.setAccessByPath(path, accessIndex + 1, pathType);
                }
                return;
            } else if (this.type.isMapType()) {
                String fieldName = path.get(accessIndex);
                if (fieldName.equals(AccessPathInfo.ACCESS_STRING_OFFSET)) {
                    // length(map_col) — only the offset array is needed, not key/value data.
                    isStringOffsetOnly = true;
                    return;
                }
                if (fieldName.equals(AccessPathInfo.ACCESS_ALL)) {
                    // access value by the key, so we should access key and access value, then prune the value's type.
                    // e.g. map_column['id'] should access the keys, and access the values
                    DataTypeAccessTree keysChild = children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                    DataTypeAccessTree valuesChild = children.get(AccessPathInfo.ACCESS_MAP_VALUES);
                    keysChild.accessAll = true;
                    valuesChild.setAccessByPath(path, accessIndex + 1, pathType);
                    return;
                } else if (fieldName.equals(AccessPathInfo.ACCESS_MAP_KEYS)) {
                    // only access the keys and not need enter keys, because it must be primitive type.
                    // e.g. map_keys(map_column)
                    DataTypeAccessTree keysChild = children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                    keysChild.accessAll = true;
                    return;
                } else if (fieldName.equals(AccessPathInfo.ACCESS_MAP_VALUES)) {
                    // only access the values without keys, and maybe prune the value's data type.
                    // e.g. map_values(map_columns)[0] will access the array of values first,
                    //      and then access the array, so the access path is ['VALUES', '*']
                    DataTypeAccessTree valuesChild = children.get(AccessPathInfo.ACCESS_MAP_VALUES);
                    valuesChild.setAccessByPath(path, accessIndex + 1, pathType);
                    return;
                }
            } else if (type.isStringLikeType()) {
                // String leaf accessed via the offset array (e.g. path ends in "offset").
                // Mark offset-only so pruneDataType() can return BigIntType instead of full data.
                if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_STRING_OFFSET)) {
                    isStringOffsetOnly = true;
                    return; // do NOT set accessAll — offset-only is distinguishable from full access
                }
                // Any other sub-path on a string column means full data is needed.
                accessAll = true;
                return;
            } else if (isRoot) {
                children.get(path.get(accessIndex).toLowerCase()).setAccessByPath(path, accessIndex + 1, pathType);
                return;
            }
            throw new AnalysisException("unsupported data type: " + this.type);
        }

        public static DataTypeAccessTree ofRoot(Slot slot, ColumnAccessPathType pathType) {
            DataTypeAccessTree child = of(slot.getDataType(), pathType);
            DataTypeAccessTree root = new DataTypeAccessTree(true, NullType.INSTANCE, pathType);
            root.children.put(slot.getName().toLowerCase(), child);
            return root;
        }

        /** of */
        public static DataTypeAccessTree of(DataType type, ColumnAccessPathType pathType) {
            DataTypeAccessTree root = new DataTypeAccessTree(type, pathType);
            if (type instanceof StructType) {
                StructType structType = (StructType) type;
                for (Entry<String, StructField> kv : structType.getNameToFields().entrySet()) {
                    root.children.put(kv.getKey().toLowerCase(), of(kv.getValue().getDataType(), pathType));
                }
            } else if (type instanceof ArrayType) {
                root.children.put(AccessPathInfo.ACCESS_ALL, of(((ArrayType) type).getItemType(), pathType));
            } else if (type instanceof MapType) {
                root.children.put(AccessPathInfo.ACCESS_MAP_KEYS, of(((MapType) type).getKeyType(), pathType));
                root.children.put(AccessPathInfo.ACCESS_MAP_VALUES, of(((MapType) type).getValueType(), pathType));
            }
            return root;
        }

        /** pruneDataType */
        public Optional<DataType> pruneDataType() {
            if (isRoot) {
                return children.values().iterator().next().pruneDataType();
            } else if (accessAll) {
                return Optional.of(type);
            } else if (isStringOffsetOnly) {
                // Only the offset array is accessed (e.g. length(str_col)).
                return Optional.of(type);
            } else if (!accessPartialChild) {
                return Optional.empty();
            }

            List<Pair<String, DataType>> accessedChildren = new ArrayList<>();

            if (type instanceof StructType) {
                for (Entry<String, DataTypeAccessTree> kv : children.entrySet()) {
                    DataTypeAccessTree childTypeTree = kv.getValue();
                    Optional<DataType> childDataType = childTypeTree.pruneDataType();
                    if (childDataType.isPresent()) {
                        accessedChildren.add(Pair.of(kv.getKey(), childDataType.get()));
                    }
                }
            } else if (type instanceof ArrayType) {
                Optional<DataType> childDataType = children.get(AccessPathInfo.ACCESS_ALL).pruneDataType();
                if (childDataType.isPresent()) {
                    accessedChildren.add(Pair.of(AccessPathInfo.ACCESS_ALL, childDataType.get()));
                }
            } else if (type instanceof MapType) {
                DataType prunedValueType = children.get(AccessPathInfo.ACCESS_MAP_VALUES)
                        .pruneDataType()
                        .orElse(((MapType) type).getValueType());
                // can not prune keys but can prune values
                accessedChildren.add(Pair.of(AccessPathInfo.ACCESS_MAP_KEYS, ((MapType) type).getKeyType()));
                accessedChildren.add(Pair.of(AccessPathInfo.ACCESS_MAP_VALUES, prunedValueType));
            }
            if (accessedChildren.isEmpty()) {
                return Optional.of(type);
            }

            return Optional.of(pruneDataType(type, accessedChildren));
        }

        private DataType pruneDataType(DataType dataType, List<Pair<String, DataType>> newChildrenTypes) {
            if (dataType instanceof StructType) {
                // prune struct fields
                StructType structType = (StructType) dataType;
                Map<String, StructField> nameToFields = structType.getNameToFields();
                List<StructField> newFields = new ArrayList<>();
                for (Pair<String, DataType> kv : newChildrenTypes) {
                    String fieldName = kv.first;
                    StructField originField = nameToFields.get(fieldName);
                    DataType prunedType = kv.second;
                    newFields.add(new StructField(
                            originField.getName(), prunedType, originField.isNullable(), originField.getComment()
                    ));
                }
                return new StructType(newFields);
            } else if (dataType instanceof ArrayType) {
                return ArrayType.of(newChildrenTypes.get(0).second, ((ArrayType) dataType).containsNull());
            } else if (dataType instanceof MapType) {
                return MapType.of(newChildrenTypes.get(0).second, newChildrenTypes.get(1).second);
            } else {
                throw new AnalysisException("unsupported data type: " + dataType);
            }
        }
    }
}
