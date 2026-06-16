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
            Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths = collector.collect(plan, statementContext);
            Map<Integer, AccessPathInfo> slotToResult = pruneDataType(slotToAccessPaths,
                    jobContext.getCascadesContext().isMaterializedViewRewritePlanFragment());

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
            Map<Slot, List<CollectAccessPathResult>> slotToAccessPaths,
            boolean ignoreMetaPath) {
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
            if (ignoreMetaPath && containsMetaPath(collectAccessPathResults)) {
                // An MV rewrite child context optimizes a temporary plan fragment rather
                // than the final plan. A nested metadata-only path such as
                // [s, city, NULL] or [s, city, OFFSET] would otherwise prune the scan slot
                // to only that nested field, while the final MV rewritten plan may still
                // reuse the same slot as a full complex value or need another child. Drop
                // access-info for the whole slot instead of just removing that path:
                // predicate expressions inside this fragment still reference the original
                // slot shape, so partial pruning after deleting the predicate-only path
                // could make the fragment itself inconsistent.
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
            normalizeMapValueMetaOnlyAccessPaths(slot, accessTree, allAccessPaths);

            if (accessTree.hasOffsetOnlyAccess() || accessTree.hasNullOnlyAccess()) {
                if (ignoreMetaPath) {
                    continue;
                }
                stripCoveredMetaSuffixPaths(slot, allAccessPaths, allAccessPaths);
                List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
                result.put(slot.getExprId().asInt(),
                        new AccessPathInfo(slot.getDataType(), allPaths, new ArrayList<>()));
                continue;
            }

            if (slot.getDataType().isStringLikeType()) {
                // direct access (accessAll=true) or other: skip — no type change, no access paths needed.
                continue;
            }

            stripCoveredMetaSuffixPaths(slot, allAccessPaths, allAccessPaths);
            List<ColumnAccessPath> allPaths = buildColumnAccessPaths(slot, allAccessPaths);
            if (shouldSkipAccessInfo(slot, prunedDataType, allPaths, predicateAccessPaths)) {
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

        // third: build predicate access path
        for (Entry<Slot, DataTypeAccessTree> kv : slotIdToPredicateAccessTree.entrySet()) {
            Slot slot = kv.getKey();
            normalizeMapValueMetaOnlyAccessPaths(slot, kv.getValue(), predicateAccessPaths);
            stripCoveredMetaSuffixPaths(slot, predicateAccessPaths, allAccessPaths);
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

    private static boolean containsMetaPath(
            List<CollectAccessPathResult> collectAccessPathResults) {
        for (CollectAccessPathResult collectAccessPathResult : collectAccessPathResults) {
            if (isMetaPath(collectAccessPathResult.getPath())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isMetaPath(List<String> path) {
        if (path.isEmpty()) {
            return false;
        }
        String lastComponent = path.get(path.size() - 1);
        return AccessPathInfo.ACCESS_NULL.equals(lastComponent)
                || AccessPathInfo.ACCESS_OFFSET.equals(lastComponent);
    }

    private static void normalizeMapValueMetaOnlyAccessPaths(
            Slot slot, DataTypeAccessTree accessTree,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> accessPaths) {
        int slotId = slot.getExprId().asInt();
        normalizeMapValueMetaPathHelper(slotId,
                accessTree.collectMapValueMetaOnlyAccessPaths(AccessPathInfo.ACCESS_OFFSET),
                accessPaths, AccessPathInfo.ACCESS_OFFSET);
        normalizeMapValueMetaPathHelper(slotId,
                accessTree.collectMapValueMetaOnlyAccessPaths(AccessPathInfo.ACCESS_NULL),
                accessPaths, AccessPathInfo.ACCESS_NULL);
    }

    /**
     * Normalize map value meta-only (OFFSET or NULL) star access paths for a single meta type.
     * <p>For each map prefix where {@code [prefix, *, META]} exists, replaces it with
     * {@code [prefix, KEYS]} plus {@code [prefix, VALUES, META]}, so that keys are read fully
     * for element lookup while values only read the metadata.
     */
    private static void normalizeMapValueMetaPathHelper(
            int slotId, List<List<String>> mapPrefixes,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> accessPaths,
            String metaSuffix) {
        if (mapPrefixes.isEmpty()) {
            return;
        }

        Collection<Pair<ColumnAccessPathType, List<String>>> slotPaths = accessPaths.get(slotId);
        List<Pair<ColumnAccessPathType, List<String>>> pathsToRemove = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> pathsToAdd = new ArrayList<>();
        for (List<String> mapPrefix : mapPrefixes) {
            List<String> starMetaPath = new ArrayList<>(mapPrefix);
            starMetaPath.add(AccessPathInfo.ACCESS_ALL);
            starMetaPath.add(metaSuffix);

            for (Pair<ColumnAccessPathType, List<String>> p : slotPaths) {
                if (!p.second.equals(starMetaPath)) {
                    continue;
                }
                pathsToRemove.add(p);

                List<String> keysPath = new ArrayList<>(mapPrefix);
                keysPath.add(AccessPathInfo.ACCESS_MAP_KEYS);
                pathsToAdd.add(Pair.of(p.first, keysPath));

                List<String> valuesMetaPath = new ArrayList<>(mapPrefix);
                valuesMetaPath.add(AccessPathInfo.ACCESS_MAP_VALUES);
                valuesMetaPath.add(metaSuffix);
                pathsToAdd.add(Pair.of(p.first, valuesMetaPath));
            }
        }
        slotPaths.removeAll(pathsToRemove);
        slotPaths.addAll(pathsToAdd);
    }

    /**
     * Strip redundant metadata-only NULL/OFFSET paths while keeping enough real paths for BE readers.
     *
     * <p>The strip cases are:
     * <ul>
     *   <li>Map element lookup with value meta-only (OFFSET or NULL) access is normalized first,
     *       e.g. {@code [m, *, OFFSET]} becomes {@code [m, KEYS]} plus
     *       {@code [m, VALUES, OFFSET]}, and {@code [m, *, NULL]} becomes
     *       {@code [m, KEYS]} plus {@code [m, VALUES, NULL]}. This is handled by
     *       {@link #normalizeMapValueMetaOnlyAccessPaths}.</li>
     *   <li>Full/data access covers metadata access, e.g. {@code [a]} removes
     *       {@code [a, NULL]} and {@code [a, OFFSET]}. This is handled by
     *       {@link #stripExactCoveredDataSkippingSuffixPaths}.</li>
     *   <li>A deeper data access covers an upper metadata path, e.g. {@code [a, b, c]}
     *       removes {@code [a, b, NULL]}, and {@code [a, *, field]} removes
     *       {@code [a, OFFSET]}. Exact NULL/data coverage is handled by
     *       {@link #stripNullSuffixPaths}; OFFSET/container coverage is handled by
     *       {@link #stripCoveredOffsetByDataPaths}.</li>
     *   <li>A deeper OFFSET access covers an upper OFFSET path, e.g.
     *       {@code [a, *, OFFSET]} removes {@code [a, OFFSET]}. This is handled by
     *       {@link #stripCoveredOffsetByDeeperOffsetPaths}.</li>
     *   <li>OFFSET access covers NULL access with the same prefix, e.g.
     *       {@code [a, OFFSET]} removes {@code [a, NULL]}. This is handled by
     *       {@link #stripNullSuffixPaths}.</li>
     *   <li>A deeper NULL access covers an upper NULL path, e.g.
     *       {@code [a, *, NULL]} removes {@code [a, NULL]}. This is handled by
     *       {@link #stripNullSuffixPaths}.</li>
     *   <li>Map value-side coverage may need a supplemental key path, e.g.
     *       {@code [m, *, OFFSET]} plus {@code [m, VALUES]} becomes
     *       {@code [m, KEYS]} plus {@code [m, VALUES]}. This is handled by
     *       {@link #stripCoveredOffsetByDataPaths} via {@link #compareOffsetPrefixCoverage}
     *       and {@link #buildMapKeysOnlyPath}.</li>
     *   <li>Array NULL-only paths covered by value/data access may also need supplemental
     *       map keys, e.g. {@code [m, *, NULL]} plus {@code [m, VALUES, *, field]}
     *       becomes {@code [m, KEYS]} plus {@code [m, VALUES, *, field]}. This is
     *       handled by {@link #stripCoveredArrayNullSuffixPaths}.</li>
     * </ul>
     */
    private static void stripCoveredMetaSuffixPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        stripExactCoveredDataSkippingSuffixPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripCoveredOffsetByDataPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripCoveredOffsetByDeeperOffsetPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripCoveredArrayNullSuffixPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripNullSuffixPaths(slot, targetAccessPaths);
    }

    /**
     * Decide whether an OFFSET-suffix path can be removed because another path already covers
     * the same container.
     *
     * <p>For map element_at paths, {@code *} means "read keys fully, then follow the rest of
     * the path on the value side". So a VALUES path can cover the value-side OFFSET access,
     * but it does NOT cover the key lookup requirement. In that case we remove the OFFSET path
     * and add a KEYS-only path instead.
     */
    private static OffsetPathRewrite analyzeOffsetPathRewrite(
            DataType slotType, List<String> path, List<List<String>> coveringPaths) {
        if (path.isEmpty()
                || !AccessPathInfo.ACCESS_OFFSET.equals(path.get(path.size() - 1))) {
            return OffsetPathRewrite.keep();
        }
        List<String> prefix = path.subList(0, path.size() - 1);
        // Filter out the path itself to prevent self-coverage removal.
        // A deeper OFFSET path (e.g. [aa, *, OFFSET]) covering a shallower one
        // (e.g. [aa, OFFSET]) is still handled — they are different paths.
        List<List<String>> filteredCoveringPaths = new ArrayList<>();
        for (List<String> p : coveringPaths) {
            if (!p.equals(path)) {
                filteredCoveringPaths.add(p);
            }
        }
        return analyzePrefixCoverage(slotType, prefix, filteredCoveringPaths);
    }

    private static OffsetPathRewrite analyzePrefixCoverage(
            DataType slotType, List<String> prefix, List<List<String>> coveringPaths) {
        List<List<String>> supplementalPaths = new ArrayList<>();
        for (List<String> coveringPath : coveringPaths) {
            OffsetPathRewrite candidate = compareOffsetPrefixCoverage(slotType, prefix, coveringPath);
            if (!candidate.shouldRemoveOffsetPath()) {
                continue;
            }
            if (candidate.getSupplementalPaths().isEmpty()) {
                return OffsetPathRewrite.remove();
            }
            supplementalPaths.addAll(candidate.getSupplementalPaths());
        }
        if (supplementalPaths.isEmpty()) {
            return OffsetPathRewrite.keep();
        }
        return OffsetPathRewrite.rewriteWithSupplementalPaths(supplementalPaths);
    }

    /**
     * Remove OFFSET-only paths from {@code targetAccessPaths} when non-meta data paths in
     * {@code coveringAccessPaths} already read the same array/map/string container or a child
     * under it.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code [arr.OFFSET, arr.*.field]} becomes {@code [arr.*.field]} because the array
     *       child read must keep BE on the normal data iterator path.</li>
     *   <li>{@code [map.*.OFFSET, map.VALUES]} becomes {@code [map.KEYS, map.VALUES]} because
     *       {@code map['k']} still needs full keys for lookup, while values cover the offset.</li>
     * </ul>
     */
    private static void stripCoveredOffsetByDataPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }

        List<List<String>> dataPaths = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : coveringAccessPaths.get(slotId)) {
            List<String> path = p.second;
            if (!path.isEmpty() && !isMetaPath(path)) {
                dataPaths.add(path);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> path = p.second;
            if (!path.isEmpty() && !isMetaPath(path)) {
                dataPaths.add(path);
            }
        }

        stripCoveredOffsetByPaths(slot, targetAccessPaths, dataPaths);
    }

    /**
     * Remove upper OFFSET-only paths when a deeper OFFSET path traverses the same container.
     *
     * <p>Example: {@code [a, *, OFFSET]} removes {@code [a, OFFSET]} because reading the inner
     * array offset requires traversing the outer array structure.
     */
    private static void stripCoveredOffsetByDeeperOffsetPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }

        List<List<String>> deeperOffsetPaths = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : coveringAccessPaths.get(slotId)) {
            List<String> path = p.second;
            if (isDeeperOffsetPath(path)) {
                deeperOffsetPaths.add(path);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> path = p.second;
            if (isDeeperOffsetPath(path)) {
                deeperOffsetPaths.add(path);
            }
        }

        stripCoveredOffsetByPaths(slot, targetAccessPaths, deeperOffsetPaths);
    }

    private static boolean isDeeperOffsetPath(List<String> path) {
        return path.size() > 2 && AccessPathInfo.ACCESS_OFFSET.equals(path.get(path.size() - 1));
    }

    private static void stripCoveredOffsetByPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            List<List<String>> coveringPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty() || coveringPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> pathsToRemove = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> pathsToAdd = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : new ArrayList<>(targetPaths)) {
            OffsetPathRewrite rewrite = analyzeOffsetPathRewrite(
                    slot.getDataType(), p.second, coveringPaths);
            if (!rewrite.shouldRemoveOffsetPath()) {
                continue;
            }
            pathsToRemove.add(p);
            for (List<String> supplementalPath : rewrite.getSupplementalPaths()) {
                pathsToAdd.add(Pair.of(p.first, supplementalPath));
            }
        }
        targetPaths.removeAll(pathsToRemove);
        targetPaths.addAll(pathsToAdd);
    }

    /**
     * Remove array NULL-only paths from {@code targetAccessPaths} when another path already reads
     * the same array container or data under it. This mirrors OFFSET coverage because an array
     * element/data read must not be combined with an array NULL_MAP_ONLY read for the same prefix.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code [map.VALUES.NULL, map.VALUES.*.field]} becomes
     *       {@code [map.VALUES.*.field]}.</li>
     *   <li>{@code [map.*.NULL, map.VALUES.*.field]} becomes
     *       {@code [map.KEYS, map.VALUES.*.field]} so map lookup keys are still available.</li>
     * </ul>
     */
    private static void stripCoveredArrayNullSuffixPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }

        List<List<String>> nonNullPaths = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : coveringAccessPaths.get(slotId)) {
            List<String> path = p.second;
            if (path.isEmpty() || !AccessPathInfo.ACCESS_NULL.equals(path.get(path.size() - 1))) {
                nonNullPaths.add(path);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> path = p.second;
            if (path.isEmpty() || !AccessPathInfo.ACCESS_NULL.equals(path.get(path.size() - 1))) {
                nonNullPaths.add(path);
            }
        }

        List<Pair<ColumnAccessPathType, List<String>>> pathsToRemove = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> pathsToAdd = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : new ArrayList<>(targetPaths)) {
            List<String> path = p.second;
            if (path.isEmpty() || !AccessPathInfo.ACCESS_NULL.equals(path.get(path.size() - 1))) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            Optional<DataType> prefixType = dataTypeAtPath(slot.getDataType(), prefix);
            if (!prefixType.isPresent() || !prefixType.get().isArrayType()) {
                continue;
            }
            OffsetPathRewrite rewrite = analyzePrefixCoverage(slot.getDataType(), prefix, nonNullPaths);
            if (!rewrite.shouldRemoveOffsetPath()) {
                continue;
            }
            pathsToRemove.add(p);
            for (List<String> supplementalPath : rewrite.getSupplementalPaths()) {
                pathsToAdd.add(Pair.of(p.first, supplementalPath));
            }
        }
        targetPaths.removeAll(pathsToRemove);
        targetPaths.addAll(pathsToAdd);
    }

    /**
     * Remove exact metadata-only NULL/OFFSET paths when the same field is read in full.
     * This rule is type-agnostic: once {@code s} itself is accessed, {@code s.NULL} and
     * {@code s.OFFSET} are redundant and unsafe to keep with the full data path.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code [str_col, str_col.NULL]} becomes {@code [str_col]}.</li>
     *   <li>{@code [arr, arr.OFFSET]} becomes {@code [arr]}.</li>
     *   <li>{@code [map.*, map.*.OFFSET]} becomes {@code [map.*]}.</li>
     * </ul>
     */
    private static void stripExactCoveredDataSkippingSuffixPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }

        List<List<String>> fullAccessPaths = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : coveringAccessPaths.get(slotId)) {
            if (!isMetaPath(p.second)) {
                fullAccessPaths.add(p.second);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            if (!isMetaPath(p.second)) {
                fullAccessPaths.add(p.second);
            }
        }

        List<Pair<ColumnAccessPathType, List<String>>> pathsToRemove = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> path = p.second;
            if (!isMetaPath(path)) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            for (List<String> fullAccessPath : fullAccessPaths) {
                if (pathCoversPrefix(fullAccessPath, prefix)) {
                    pathsToRemove.add(p);
                    break;
                }
            }
        }
        targetPaths.removeAll(pathsToRemove);
    }

    private static Optional<DataType> dataTypeAtPath(DataType slotType, List<String> path) {
        if (path.isEmpty()) {
            return Optional.empty();
        }
        DataType currentType = slotType;
        for (int i = 1; i < path.size(); i++) {
            String component = path.get(i);
            if (currentType.isStructType()) {
                StructField field = ((StructType) currentType).getField(component);
                if (field == null) {
                    return Optional.empty();
                }
                currentType = field.getDataType();
            } else if (currentType.isArrayType()) {
                if (!AccessPathInfo.ACCESS_ALL.equals(component)) {
                    return Optional.empty();
                }
                currentType = ((ArrayType) currentType).getItemType();
            } else if (currentType.isMapType()) {
                currentType = descendMapType((MapType) currentType, component);
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(currentType);
    }

    private static OffsetPathRewrite compareOffsetPrefixCoverage(
            DataType slotType, List<String> prefix, List<String> nonOffset) {
        if (nonOffset.isEmpty()) {
            return OffsetPathRewrite.remove();
        }
        int minLen = Math.min(prefix.size(), nonOffset.size());
        List<List<String>> supplementalPaths = new ArrayList<>();
        DataType currentType = slotType;
        for (int i = 0; i < minLen; i++) {
            String prefixComponent = prefix.get(i);
            String nonOffsetComponent = nonOffset.get(i);
            if (i == 0) {
                if (!prefixComponent.equals(nonOffsetComponent)) {
                    return OffsetPathRewrite.keep();
                }
                continue;
            }
            if (currentType.isStructType()) {
                if (!prefixComponent.equals(nonOffsetComponent)) {
                    return OffsetPathRewrite.keep();
                }
                StructField field = ((StructType) currentType).getField(prefixComponent);
                if (field == null) {
                    return OffsetPathRewrite.keep();
                }
                currentType = field.getDataType();
                continue;
            }
            if (currentType.isArrayType()) {
                if (!prefixComponent.equals(nonOffsetComponent)
                        || !AccessPathInfo.ACCESS_ALL.equals(prefixComponent)) {
                    return OffsetPathRewrite.keep();
                }
                currentType = ((ArrayType) currentType).getItemType();
                continue;
            }
            if (currentType.isMapType()) {
                MapType mapType = (MapType) currentType;
                if (prefixComponent.equals(nonOffsetComponent)) {
                    currentType = descendMapType(mapType, prefixComponent);
                    continue;
                }
                if (AccessPathInfo.ACCESS_ALL.equals(prefixComponent)
                        && AccessPathInfo.ACCESS_MAP_VALUES.equals(nonOffsetComponent)) {
                    supplementalPaths.add(buildMapKeysOnlyPath(prefix, i));
                    currentType = mapType.getValueType();
                    continue;
                }
                if (AccessPathInfo.ACCESS_MAP_VALUES.equals(prefixComponent)
                        && AccessPathInfo.ACCESS_ALL.equals(nonOffsetComponent)) {
                    currentType = mapType.getValueType();
                    continue;
                }
                if (AccessPathInfo.ACCESS_MAP_KEYS.equals(prefixComponent)
                        && AccessPathInfo.ACCESS_ALL.equals(nonOffsetComponent)) {
                    currentType = mapType.getKeyType();
                    continue;
                }
                return OffsetPathRewrite.keep();
            }
            if (!prefixComponent.equals(nonOffsetComponent)) {
                return OffsetPathRewrite.keep();
            }
        }
        if (supplementalPaths.isEmpty()) {
            return OffsetPathRewrite.remove();
        }
        return OffsetPathRewrite.rewriteWithSupplementalPaths(supplementalPaths);
    }

    private static DataType descendMapType(MapType mapType, String component) {
        if (AccessPathInfo.ACCESS_MAP_KEYS.equals(component)) {
            return mapType.getKeyType();
        }
        return mapType.getValueType();
    }

    private static List<String> buildMapKeysOnlyPath(List<String> prefix, int mapTokenIndex) {
        List<String> keyPath = new ArrayList<>(prefix.subList(0, mapTokenIndex));
        keyPath.add(AccessPathInfo.ACCESS_MAP_KEYS);
        return keyPath;
    }

    private static final class OffsetPathRewrite {
        private static final OffsetPathRewrite KEEP = new OffsetPathRewrite(false, ImmutableList.of());
        private static final OffsetPathRewrite REMOVE = new OffsetPathRewrite(true, ImmutableList.of());

        private final boolean removeOffsetPath;
        private final List<List<String>> supplementalPaths;

        private OffsetPathRewrite(boolean removeOffsetPath, List<List<String>> supplementalPaths) {
            this.removeOffsetPath = removeOffsetPath;
            this.supplementalPaths = supplementalPaths;
        }

        private static OffsetPathRewrite keep() {
            return KEEP;
        }

        private static OffsetPathRewrite remove() {
            return REMOVE;
        }

        private static OffsetPathRewrite rewriteWithSupplementalPaths(List<List<String>> supplementalPaths) {
            return new OffsetPathRewrite(true, ImmutableList.copyOf(supplementalPaths));
        }

        private boolean shouldRemoveOffsetPath() {
            return removeOffsetPath;
        }

        private List<List<String>> getSupplementalPaths() {
            return supplementalPaths;
        }
    }

    /**
     * Strip NULL-suffix paths that are redundant because a non-NULL path reads child
     * data below the same prefix or reads an OFFSET path over the same prefix.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code [struct_col.NULL, struct_col.city]} becomes {@code [struct_col.city]}.</li>
     *   <li>{@code [str_col.NULL, str_col.OFFSET]} becomes {@code [str_col.OFFSET]} because
     *       the offset read can provide nullness for variable-length columns.</li>
     * </ul>
     *
     * <p>A parent NULL path must also be removed when any child path is required under the
     * same prefix, e.g. [struct_col, NULL] with [struct_col, city]. This looks like the
     * parent null map may still be useful for predicates, but it cannot be kept in
     * allAccessPaths with the current BE iterator contract: Struct/Array/Map iterators
     * treat a leading NULL sub-path as NULL_MAP_ONLY and skip all children. If FE kept
     * [struct_col.NULL, struct_col.city] in allAccessPaths, BE would read only the
     * struct null map and default-fill city instead of routing the city child iterator.
     * When the NULL path is removed from allAccessPaths, it must also be removed from
     * predicateAccessPaths so the BE can rely on predicate paths being a subset of all
     * paths. The normal nullable container read materializes the parent null map
     * together with required children.
     */
    private static void stripNullSuffixPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> allAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> slotPaths = allAccessPaths.get(slotId);

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : slotPaths) {
            List<String> path = p.second;
            if (path.isEmpty() || !AccessPathInfo.ACCESS_NULL.equals(path.get(path.size() - 1))) {
                continue;
            }
            // Prefix is the column/subcolumn path without the trailing NULL suffix.
            // A non-NULL path that equals this prefix means the same column/subcolumn
            // is read in full, making the NULL-only path redundant.
            // An OFFSET-suffix path over the same prefix is also enough for the BE to
            // derive null-ness for variable-length columns, so [col.NULL] is redundant
            // when [col.OFFSET] already exists.
            List<String> prefix = path.subList(0, path.size() - 1);
            boolean covered = false;
            for (Pair<ColumnAccessPathType, List<String>> q : slotPaths) {
                List<String> other = q.second;
                if (other.isEmpty()) {
                    continue;
                }
                if (other == path) {
                    continue;
                }
                if (AccessPathInfo.ACCESS_NULL.equals(other.get(other.size() - 1))) {
                    // A deeper NULL path (e.g. [a, *, NULL]) covers a shallower one
                    // (e.g. [a, NULL]) because reading the deeper path requires
                    // traversing the container, which materializes the container's
                    // null information.
                    if (hasStrictPrefix(other, prefix)) {
                        covered = true;
                        break;
                    }
                    continue;
                }
                if (other.equals(prefix)) {
                    covered = true;
                    break;
                }
                if (hasStrictPrefix(other, prefix)) {
                    covered = true;
                    break;
                }
                if (other.size() == prefix.size() + 1
                        && AccessPathInfo.ACCESS_OFFSET.equals(other.get(other.size() - 1))
                        && other.subList(0, prefix.size()).equals(prefix)) {
                    covered = true;
                    break;
                }
            }
            if (covered) {
                toRemove.add(p);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> r : toRemove) {
            allAccessPaths.remove(slotId, r);
        }
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

    private static boolean hasStrictPrefix(List<String> path, List<String> prefix) {
        return path.size() > prefix.size() && path.subList(0, prefix.size()).equals(prefix);
    }

    private static boolean pathCoversPrefix(List<String> path, List<String> prefix) {
        return prefix.size() >= path.size() && prefix.subList(0, path.size()).equals(path);
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
            Slot slot, DataType prunedDataType, List<ColumnAccessPath> allPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> predicateAccessPaths) {
        if (!prunedDataType.equals(slot.getDataType())) {
            return false;
        }
        if (slot.getDataType() instanceof NestedColumnPrunable || slot.getDataType().isVariantType()) {
            return false;
        }
        if (!predicateAccessPaths.get(slot.getExprId().asInt()).isEmpty()) {
            return false;
        }
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
         * Collect MAP nodes where keys are fully accessed for element lookup while values
         * only need metadata (OFFSET or NULL). Expected access paths for each returned prefix:
         * [prefix, KEYS] and [prefix, VALUES, metaSuffix].
         *
         * @param metaSuffix {@link AccessPathInfo#ACCESS_OFFSET} or
         *                   {@link AccessPathInfo#ACCESS_NULL}
         */
        public List<List<String>> collectMapValueMetaOnlyAccessPaths(String metaSuffix) {
            List<List<String>> mapPrefixes = new ArrayList<>();
            if (!isRoot) {
                collectMapValueMetaOnlyAccessPaths(new ArrayList<>(), mapPrefixes, metaSuffix);
                return mapPrefixes;
            }
            for (Entry<String, DataTypeAccessTree> child : children.entrySet()) {
                List<String> path = new ArrayList<>();
                path.add(child.getKey());
                child.getValue().collectMapValueMetaOnlyAccessPaths(path, mapPrefixes, metaSuffix);
            }
            return mapPrefixes;
        }

        private void collectMapValueMetaOnlyAccessPaths(
                List<String> currentPath, List<List<String>> mapPrefixes, String metaSuffix) {
            if (isMapValueMetaOnlyAccess(metaSuffix)) {
                mapPrefixes.add(new ArrayList<>(currentPath));
            }
            for (Entry<String, DataTypeAccessTree> child : children.entrySet()) {
                List<String> childPath = new ArrayList<>(currentPath);
                childPath.add(child.getKey());
                child.getValue().collectMapValueMetaOnlyAccessPaths(childPath, mapPrefixes, metaSuffix);
            }
        }

        private boolean isMapValueMetaOnlyAccess(String metaSuffix) {
            if (!type.isMapType() || accessAll) {
                return false;
            }
            DataTypeAccessTree keysChild = children.get(AccessPathInfo.ACCESS_MAP_KEYS);
            DataTypeAccessTree valsChild = children.get(AccessPathInfo.ACCESS_MAP_VALUES);
            // Keys must be fully accessed (element-at lookup).
            if (!keysChild.accessAll) {
                return false;
            }
            boolean hasMeta = AccessPathInfo.ACCESS_OFFSET.equals(metaSuffix)
                    ? valsChild.hasOffsetPath
                    : valsChild.hasNullPath;
            if (!hasMeta || valsChild.accessAll) {
                return false;
            }
            if (valsChild.type.isStringLikeType()) {
                return true;
            }
            if (valsChild.type.isArrayType()) {
                // Array value: verify no element was read directly
                // (e.g. map_col['k'][0] would set allChild.accessAll=true).
                DataTypeAccessTree allChild = valsChild.children.get(AccessPathInfo.ACCESS_ALL);
                return !allChild.accessAll && !allChild.accessPartialChild;
            }
            return true;
        }

        /** True when the column is accessed ONLY via the offset array (e.g. length(str_col),
         *  length(arr_col), length(map_col)), meaning the type must not change but an access
         *  path still needs to be sent to BE so it can skip the char/element data. */
        public boolean hasOffsetOnlyAccess() {
            DataTypeAccessTree selfOrRootChild = this;
            if (isRoot) {
                selfOrRootChild = children.values().iterator().next();
            }
            if (!selfOrRootChild.hasOffsetPath || selfOrRootChild.accessAll) {
                return false;
            }
            if (selfOrRootChild.type.isStringLikeType()) {
                return true;
            }
            if (selfOrRootChild.type.isArrayType()) {
                // True only if no element was accessed (element_at / explode etc.)
                DataTypeAccessTree allChild = selfOrRootChild.children.get(AccessPathInfo.ACCESS_ALL);
                return !allChild.accessAll && !allChild.accessPartialChild;
            }
            if (selfOrRootChild.type.isMapType()) {
                // True only if neither keys nor values were accessed directly
                DataTypeAccessTree keysChild = selfOrRootChild.children.get(AccessPathInfo.ACCESS_MAP_KEYS);
                DataTypeAccessTree valsChild = selfOrRootChild.children.get(AccessPathInfo.ACCESS_MAP_VALUES);
                return !keysChild.accessAll && !keysChild.accessPartialChild
                        && !valsChild.accessAll && !valsChild.accessPartialChild;
            }
            return false;
        }

        /** True when the column is accessed ONLY via IS NULL / IS NOT NULL,
         *  meaning the BE only needs to read the null flag, not the actual data. */
        public boolean hasNullOnlyAccess() {
            if (isRoot) {
                DataTypeAccessTree child = children.values().iterator().next();
                return child.hasNullPath && !child.accessAll
                        && !child.hasOffsetPath && !child.accessPartialChild;
            }
            return hasNullPath && !accessAll && !hasOffsetPath && !accessPartialChild;
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
}
