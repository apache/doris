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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NestedColumnPrunable;
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
import java.util.Collection;
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
                        && !containsStringLength(plan)
                        && !containsNullCheck(plan))) {
                return plan;
            }
            AccessPathPlanCollector collector = new AccessPathPlanCollector();
            // MV rewrite fragments must not be perturbed by metadata-only paths
            // (NULL/OFFSET) that would otherwise prune the slot to a narrower type.
            // Skip collecting them at the source instead of working around them downstream.
            collector.setSkipMetaPath(
                    jobContext.getCascadesContext().isMaterializedViewRewritePlanFragment());
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

    /** Returns true when the plan tree contains IS NULL or IS NOT NULL on a nullable slot. */
    private static boolean containsNullCheck(Plan plan) {
        AtomicBoolean found = new AtomicBoolean(false);
        plan.foreachUp(node -> {
            if (found.get()) {
                return;
            }
            Plan current = (Plan) node;
            for (Expression expression : current.getExpressions()) {
                if (expressionContainsNullCheck(expression)) {
                    found.set(true);
                    return;
                }
            }
        });
        return found.get();
    }

    private static boolean expressionContainsNullCheck(Expression expr) {
        if (expr instanceof IsNull && expr.child(0).nullable()) {
            return true;
        }
        if (expr instanceof Not && expr.child(0) instanceof IsNull
                && expr.child(0).child(0).nullable()) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (expressionContainsNullCheck(child)) {
                return true;
            }
        }
        return false;
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

        // phase 1.5: for slots with meta paths, expand map-star paths and strip
        // redundant meta paths. Strip predicate first using the COMPLETE
        // allAccessPaths as covering, then strip allAccessPaths self-covering.
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToAllAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            DataTypeAccessTree accessTree = kv.getValue();
            if (!accessTree.hasOffsetPath() && !accessTree.hasNullPath()) {
                continue;
            }
            int slotId = slot.getExprId().asInt();
            // Expand both sets before stripping so covering is complete.
            expandMapStarPaths(slot, allAccessPaths);
            expandMapStarPaths(slot, predicateAccessPaths);
            MetaPathStriper.strip(slotId, predicateAccessPaths, allAccessPaths);
            MetaPathStriper.strip(slotId, allAccessPaths, allAccessPaths);
        }

        // second: build non-predicate access paths
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToAllAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            DataTypeAccessTree accessTree = kv.getValue();
            DataType prunedDataType = accessTree.pruneDataType().orElse(slot.getDataType());
            List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
            if (shouldSkipAccessInfo(slot, prunedDataType, allPaths)) {
                continue;
            }
            result.put(slot.getExprId().asInt(),
                    new AccessPathInfo(prunedDataType, allPaths, new ArrayList<>()));
        }

        for (Entry<Slot, DataType> kv : variantSlots.entrySet()) {
            Slot slot = kv.getKey();
            List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
            result.put(slot.getExprId().asInt(),
                    new AccessPathInfo(slot.getDataType(), allPaths, new ArrayList<>()));
        }

        // third: build predicate access path (strip already done in phase 1.5)
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToPredicateAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            List<ColumnAccessPath> predicatePaths =
                    buildColumnAccessPaths(slot, predicateAccessPaths);
            AccessPathInfo accessPathInfo = result.get(slot.getExprId().asInt());
            if (accessPathInfo != null) {
                retainPredicatePathsInFinalAllAccessPaths(
                        predicatePaths, accessPathInfo.getAllAccessPaths());
                accessPathInfo.getPredicateAccessPaths().addAll(predicatePaths);
            }
        }

        for (Entry<Slot, DataType> kv : variantSlots.entrySet()) {
            Slot slot = kv.getKey();
            List<ColumnAccessPath> predicatePaths =
                    buildColumnAccessPaths(slot, predicateAccessPaths);
            AccessPathInfo accessPathInfo = result.get(slot.getExprId().asInt());
            if (accessPathInfo != null) {
                retainPredicatePathsInFinalAllAccessPaths(
                        predicatePaths, accessPathInfo.getAllAccessPaths());
                accessPathInfo.getPredicateAccessPaths().addAll(predicatePaths);
            }
        }

        return result;
    }

    /**
     * Keep predicate access paths as a subset of final all access paths after NULL/OFFSET cleanup.
     * Predicate paths are built from filter expressions first, but later all-path rewrites may drop
     * metadata-only paths or collapse paths to whole-column access. Any predicate path not present
     * in final all paths must be removed before sending access info to BE.
     *
     * <p>Examples:
     * <ul>
     *   <li>All paths {@code [s]}, predicate paths {@code [s.city.NULL]} becomes no predicate
     *       paths after parent NULL removal.</li>
     *   <li>All paths {@code [s.city.NULL, s.zip]}, predicate paths
     *       {@code [s.NULL, s.city.NULL]} becomes {@code [s.city.NULL]}.</li>
     * </ul>
     */
    private static void retainPredicatePathsInFinalAllAccessPaths(
            List<ColumnAccessPath> predicatePaths, List<ColumnAccessPath> allPaths) {
        if (predicatePaths.isEmpty()) {
            return;
        }

        List<ColumnAccessPath> toRemove = new ArrayList<>();
        for (ColumnAccessPath predicatePath : predicatePaths) {
            if (!allPaths.contains(predicatePath)) {
                toRemove.add(predicatePath);
            }
        }
        predicatePaths.removeAll(toRemove);
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
            return new ArrayList<>(
                    ImmutableList.of(new ColumnAccessPath(accessWholeColumnType, ImmutableList.of(wholeColumnName))));
        }
        return paths;
    }

    private static boolean shouldSkipAccessInfo(
            Slot slot, DataType prunedDataType, List<ColumnAccessPath> allPaths) {
        if (!prunedDataType.equals(slot.getDataType())) {
            return false;
        }
        if (slot.getDataType() instanceof NestedColumnPrunable || slot.getDataType().isVariantType()) {
            return false;
        }

        // Only scalar / string-like types reach here (NestedColumnPrunable and Variant
        // returned false above). A single [col] path means the entire column is read;
        // no access info needs to be sent to BE.
        if (allPaths.size() != 1) {
            return false;
        }
        List<String> path = allPaths.get(0).getPath();
        return path.size() == 1;
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
        // Cached marker set by setAccessByPath() when a path component is OFFSET.
        // Avoids scanning the multimap: hasStringOffsetOnlyAccess() reads this flag in
        // O(1) instead of checking every path for an OFFSET suffix. Used for array, map,
        // and string-like types (they share offset-based storage in BE).
        // When set without accessAll, pruneDataType() keeps the node's type so that BE
        // reads only the offset structure, skipping element / key-value / chars data.
        private boolean hasOffsetPath;
        // Cached marker set by setAccessByPath() when a path component is NULL.
        // Same purpose as hasOffsetPath — O(1) flag read instead of multimap scan
        // in hasNullCheckOnlyAccess(). When set without accessAll, BE reads only the
        // null bitmap, skipping actual column data.
        private boolean hasNullPath;
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
         * recursively search in the tree, if any node hasOffsetPath
         */
        public boolean hasOffsetPath() {
            if (hasOffsetPath) {
                return true;
            }
            for (DataTypeAccessTree child : children.values()) {
                if (child.hasOffsetPath()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * recursively search in the tree, if any node hasNullPath
         */
        public boolean hasNullPath() {
            if (hasNullPath) {
                return true;
            }
            for (DataTypeAccessTree child : children.values()) {
                if (child.hasNullPath()) {
                    return true;
                }
            }
            return false;
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
                        )
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
            }

            if (pathType == ColumnAccessPathType.DATA) {
                this.pathType = ColumnAccessPathType.DATA;
            }

            // NULL path component: the column is accessed only via IS NULL / IS NOT NULL.
            // Mark null-check-only and return without setting accessAll or accessPartialChild,
            // so that parent nodes can distinguish "null-only leaf" from "has real sub-access".
            if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_NULL)) {
                hasNullPath = true;
                return;
            }

            accessPartialChild = true;

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
                if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_OFFSET)) {
                    // length(array_col) — only the offset array is needed, not element data.
                    hasOffsetPath = true;
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
                if (fieldName.equals(AccessPathInfo.ACCESS_OFFSET)) {
                    // length(map_col) — only the offset array is needed, not key/value data.
                    hasOffsetPath = true;
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
                    // Access the keys sub-column. Delegate to child so that trailing path
                    // components (e.g. NULL for IS NULL) are processed correctly.
                    // When no trailing component exists, setAccessByPath reaches end-of-path
                    // and sets accessAll = true, preserving the original behavior.
                    DataTypeAccessTree keysChild = children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                    keysChild.setAccessByPath(path, accessIndex + 1, pathType);
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
                if (path.get(accessIndex).equals(AccessPathInfo.ACCESS_OFFSET)) {
                    hasOffsetPath = true;
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
            } else if (hasOffsetPath && !accessPartialChild) {
                // Only the offset array is accessed (e.g. length(str_col)).
                return Optional.of(type);
            } else if (hasNullPath && !accessPartialChild) {
                // Only the null flag is accessed (e.g. col IS NULL / element_at(s,'f') IS NULL).
                // Return the node's type so that parent nodes include this child in their pruned type,
                // while the access path (ending in NULL) tells BE to skip actual data reading.
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
                return ArrayType.of(newChildrenTypes.get(0).second);
            } else if (dataType instanceof MapType) {
                return MapType.of(newChildrenTypes.get(0).second, newChildrenTypes.get(1).second);
            } else {
                throw new AnalysisException("unsupported data type: " + dataType);
            }
        }
    }

    /**
     * Expand map-level {@code *} wildcards into {@code KEYS} + {@code VALUES}
     * variants.  For n map-level stars in a single path, n+1 paths are
     * emitted: one all-VALUES path plus one KEYS-terminating path per star
     * position.  Array-level stars are left unchanged.
     *
     * <p>Paths with no map-level star are kept as-is.
     */
    private static void expandMapStarPaths(
            Slot slot,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> accessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> slotPaths = accessPaths.get(slotId);
        if (slotPaths.isEmpty()) {
            return;
        }
        DataType slotType = slot.getDataType();

        List<Pair<ColumnAccessPathType, List<String>>> toAdd = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();

        for (Pair<ColumnAccessPathType, List<String>> p : slotPaths) {
            List<String> path = p.second;
            List<Integer> positions = new ArrayList<>();
            findMapStarPositions(path, slotType, positions);
            if (positions.isEmpty()) {
                continue;
            }
            toRemove.add(p);
            toAdd.addAll(expandOnePath(p.first, path, positions));
        }

        slotPaths.removeAll(toRemove);
        slotPaths.addAll(toAdd);
    }

    private static void findMapStarPositions(
            List<String> path, DataType slotType, List<Integer> positions) {
        DataType current = slotType;
        for (int i = 1; i < path.size(); i++) {
            String component = path.get(i);
            if (current.isStructType()) {
                StructField field = ((StructType) current).getField(component);
                if (field == null) {
                    break;
                }
                current = field.getDataType();
            } else if (current.isArrayType()) {
                if (!AccessPathInfo.ACCESS_ALL.equals(component)) {
                    break;
                }
                current = ((ArrayType) current).getItemType();
            } else if (current.isMapType()) {
                MapType mapType = (MapType) current;
                if (AccessPathInfo.ACCESS_ALL.equals(component)) {
                    positions.add(i);
                    current = mapType.getValueType();
                } else if (AccessPathInfo.ACCESS_MAP_KEYS.equals(component)) {
                    current = mapType.getKeyType();
                } else if (AccessPathInfo.ACCESS_MAP_VALUES.equals(component)) {
                    current = mapType.getValueType();
                } else {
                    current = mapType.getValueType();
                }
            } else {
                break;
            }
        }
    }

    private static List<Pair<ColumnAccessPathType, List<String>>> expandOnePath(
            ColumnAccessPathType type, List<String> path, List<Integer> positions) {
        int n = positions.size();
        List<Pair<ColumnAccessPathType, List<String>>> result = new ArrayList<>(n + 1);

        // All-VALUES path: replace every map * with VALUES
        List<String> allValues = new ArrayList<>(path);
        for (int pos : positions) {
            allValues.set(pos, AccessPathInfo.ACCESS_MAP_VALUES);
        }
        result.add(Pair.of(type, allValues));

        // KEYS-terminating path for each position
        for (int i = 0; i < n; i++) {
            int keysPos = positions.get(i);
            List<String> keysPath = new ArrayList<>();
            for (int j = 0; j < keysPos; j++) {
                String component = path.get(j);
                if (positions.contains(j)) {
                    component = AccessPathInfo.ACCESS_MAP_VALUES;
                }
                keysPath.add(component);
            }
            keysPath.add(AccessPathInfo.ACCESS_MAP_KEYS);
            result.add(Pair.of(type, keysPath));
        }

        return result;
    }
}
