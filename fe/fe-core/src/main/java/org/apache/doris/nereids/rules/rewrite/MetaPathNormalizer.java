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

import org.apache.doris.analysis.ColumnAccessPathType;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Normalizes and strips redundant metadata-only (NULL/OFFSET) access paths
 * so that BE readers receive a consistent, conflict-free set of paths.
 *
 * <p>Single entry point: {@link #normalizeAndStrip(Slot, DataTypeAccessTree, Multimap, Multimap)}.
 */
public final class MetaPathNormalizer {

    private MetaPathNormalizer() {}

    /**
     * Normalize map value meta-only access paths, then strip redundant NULL/OFFSET
     * paths. These two steps must always run together: normalize rewrites
     * {@code [m, *, META]} into {@code [m, KEYS] + [m, VALUES, META]}, and strip
     * removes paths that are covered by deeper or higher-priority paths.
     */
    public static void normalizeAndStrip(
            Slot slot, DataTypeAccessTree accessTree,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        normalizeMapValueMetaOnlyAccessPaths(slot, accessTree, targetAccessPaths);
        stripCoveredMetaPaths(slot, targetAccessPaths, coveringAccessPaths);
    }

    // ========================================================================
    // isMetaPath
    // ========================================================================

    private static boolean isMetaPath(List<String> path) {
        if (path.isEmpty()) {
            return false;
        }
        String lastComponent = path.get(path.size() - 1);
        return AccessPathInfo.ACCESS_NULL.equals(lastComponent)
                || AccessPathInfo.ACCESS_OFFSET.equals(lastComponent);
    }

    private static List<List<String>> collectPaths(
            Collection<Pair<ColumnAccessPathType, List<String>>> a,
            Collection<Pair<ColumnAccessPathType, List<String>>> b, boolean meta) {
        List<List<String>> result = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : a) {
            if (!p.second.isEmpty() && isMetaPath(p.second) == meta) {
                result.add(p.second);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> p : b) {
            if (!p.second.isEmpty() && isMetaPath(p.second) == meta) {
                result.add(p.second);
            }
        }
        return result;
    }

    // ========================================================================
    // Normalize: map value meta-only access paths
    // ========================================================================

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

    // ========================================================================
    // Strip: remove redundant meta paths
    // ========================================================================

    /**
     * Strip redundant metadata-only NULL/OFFSET paths, keeping enough real paths for BE
     * readers to avoid OFFSET_ONLY / NULL_MAP_ONLY modes that skip required child data.
     *
     * <p>Stripping is organised in two levels:
     *
     * <p><b>Level 1 — Same-prefix priority:</b> when two paths target the same
     * column/subcolumn and differ only in the suffix, the higher-priority suffix
     * eliminates the lower.
     * <pre>{@code
     *   [prefix]  >  [prefix, OFFSET]  >  [prefix, NULL]
     * }</pre>
     * <ul>
     *   <li>{@code [prefix]} strips {@code [prefix, OFFSET]} and
     *       {@code [prefix, NULL]} — full access covers all metadata.</li>
     *   <li>{@code [prefix, OFFSET]} strips {@code [prefix, NULL]} — offset read
     *       provides null information for variable-length columns.</li>
     * </ul>
     *
     * <p><b>Level 2 — Deeper-prefix coverage:</b> when a covering path has a prefix
     * that strictly contains the target path's prefix, traversing the deeper container
     * already materialises the container, making the shorter-prefix meta path redundant.
     * <ul>
     *   <li>Target suffix {@code OFFSET}, covered by deeper prefix:
     *     <ul>
     *       <li>{@code Data}:   {@code [a, *, field]}  strips {@code [a, OFFSET]}.</li>
     *       <li>{@code OFFSET}: {@code [a, *, OFFSET]} strips {@code [a, OFFSET]}.</li>
     *       <li>{@code NULL}:   {@code [a, *, NULL]}   strips {@code [a, OFFSET]}.</li>
     *     </ul>
     *   </li>
     *   <li>Target suffix {@code NULL}, covered by deeper prefix:
     *     <ul>
     *       <li>{@code Data}:   {@code [a, b, c]}      strips {@code [a, b, NULL]}.</li>
     *       <li>{@code OFFSET}: {@code [a, *, OFFSET]} strips {@code [a, NULL]}.</li>
     *       <li>{@code NULL}:   {@code [a, *, NULL]}   strips {@code [a, NULL]}.</li>
     *     </ul>
     *   </li>
     * </ul>
     *
     * <p>Map OFFSET stripping may need supplemental key paths (e.g.
     * {@code [m, *, OFFSET]} + {@code [m, VALUES]} becomes {@code [m, KEYS]} +
     * {@code [m, VALUES]}), handled via
     * {@link #compareMetaPathPrefixCoverage} and {@link #buildMapKeysOnlyPath}.
     */
    private static void stripCoveredMetaPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        //stripCoveredMetaPaths
        //    Level 1: same-prefix priority — [prefix] > [prefix, OFFSET] > [prefix, NULL]
        //      ├── stripExactPrefixCoveredMetaPaths     full access covers both
        //      └── stripNullBySamePrefixOffset          OFFSET covers NULL
        //
        //    Level 2: deeper-prefix coverage
        //      ├── stripMetaPathsByDeeperPrefix(OFFSET) deeper data/meta → OFFSET
        //      └── stripMetaPathsByDeeperPrefix(NULL)   deeper data/meta → NULL
        stripExactPrefixCoveredMetaPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripNullBySamePrefixOffset(slot, targetAccessPaths);

        stripMetaPathsByDeeperPrefix(slot, AccessPathInfo.ACCESS_OFFSET,
                targetAccessPaths, coveringAccessPaths);
        stripMetaPathsByDeeperPrefix(slot, AccessPathInfo.ACCESS_NULL,
                targetAccessPaths, coveringAccessPaths);
    }

    /**
     * Level 1 — same-prefix OFFSET strips NULL: {@code [prefix, OFFSET]} strips
     * {@code [prefix, NULL]}. Uses type-aware comparison so that map-level
     * {@code *}/VALUES equivalence is recognized
     * (e.g. {@code [m, *, *, OFFSET]} strips {@code [m, VALUES, *, NULL]}).
     */
    private static void stripNullBySamePrefixOffset(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> allAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> slotPaths = allAccessPaths.get(slotId);
        if (slotPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> pathsToAdd = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : slotPaths) {
            List<String> path = p.second;
            if (path.isEmpty() || !AccessPathInfo.ACCESS_NULL.equals(path.get(path.size() - 1))) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            for (Pair<ColumnAccessPathType, List<String>> q : slotPaths) {
                List<String> other = q.second;
                if (other == path || other.isEmpty()) {
                    continue;
                }
                if (other.size() != path.size()
                        || !AccessPathInfo.ACCESS_OFFSET.equals(other.get(other.size() - 1))) {
                    continue;
                }
                List<String> otherPrefix = other.subList(0, other.size() - 1);
                OffsetPathRewrite rewrite = compareMetaPathPrefixCoverage(
                        slot.getDataType(), prefix, otherPrefix);
                if (rewrite.shouldRemoveOffsetPath()) {
                    toRemove.add(p);
                    for (List<String> supplementalPath : rewrite.getSupplementalPaths()) {
                        pathsToAdd.add(Pair.of(p.first, supplementalPath));
                    }
                    break;
                }
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> r : toRemove) {
            allAccessPaths.remove(slotId, r);
        }
        allAccessPaths.putAll(slotId, pathsToAdd);
    }

    private static OffsetPathRewrite analyzePrefixCoverage(
            DataType slotType, List<String> prefix, List<List<String>> dataPaths) {
        List<List<String>> supplementalPaths = new ArrayList<>();
        for (List<String> dataPath : dataPaths) {
            OffsetPathRewrite candidate = compareMetaPathPrefixCoverage(slotType, prefix, dataPath);
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
     * Level 2 — strip target paths ending with {@code metaSuffix}
     * (OFFSET or NULL) when a deeper-prefix path covers them.
     *
     * <p>Data covering paths: delegated to
     * {@link #stripMetaByDeeperDataPaths}.
     *
     * <p>Meta covering paths: collected from both sources and delegated
     * to {@link #stripMetaByDeeperMetaPaths}.
     */
    private static void stripMetaPathsByDeeperPrefix(
            Slot slot, String metaSuffix,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }
        Collection<Pair<ColumnAccessPathType, List<String>>> coveringPaths =
                coveringAccessPaths.get(slotId);

        List<List<String>> dataPaths = collectPaths(coveringPaths, targetPaths, false);
        stripMetaByDeeperDataPaths(slot, targetAccessPaths, dataPaths, metaSuffix);

        List<List<String>> metaPaths = collectPaths(coveringPaths, targetPaths, true);
        stripMetaByDeeperMetaPaths(slot.getDataType(), slotId, metaSuffix,
                metaPaths, targetAccessPaths);
    }

    /**
     * Level 2 — for each target path ending with {@code metaSuffix}, remove it
     * when a covering meta path has a deeper prefix after both paths have their
     * meta suffixes stripped. Same-prefix cross-type is handled by
     * {@link #stripNullBySamePrefixOffset} instead.
     */
    private static void stripMetaByDeeperMetaPaths(
            DataType slotType, int slotId, String metaSuffix,
            List<List<String>> coveringPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths) {
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths =
                targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty() || coveringPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> pathsToAdd = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> targetPath = p.second;
            if (targetPath.isEmpty() || !metaSuffix.equals(targetPath.get(targetPath.size() - 1))) {
                continue;
            }
            List<String> targetPrefix = targetPath.subList(0, targetPath.size() - 1);
            for (List<String> coveringPath : coveringPaths) {
                if (coveringPath == targetPath || coveringPath.isEmpty()) {
                    continue;
                }
                // Strip meta suffix from both and compare prefix depth: only
                // strictly deeper covering paths subsume the target. Same-prefix
                // cross-type is handled by Level-1 priority (OFFSET > NULL).
                if (coveringPath.size() - 1 <= targetPath.size() - 1) {
                    continue;
                }
                OffsetPathRewrite rewrite = compareMetaPathPrefixCoverage(
                        slotType, targetPrefix, coveringPath);
                if (rewrite.shouldRemoveOffsetPath()) {
                    toRemove.add(p);
                    for (List<String> supplementalPath : rewrite.getSupplementalPaths()) {
                        pathsToAdd.add(Pair.of(p.first, supplementalPath));
                    }
                    break;
                }
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> r : toRemove) {
            targetAccessPaths.remove(slotId, r);
        }
        targetAccessPaths.putAll(slotId, pathsToAdd);
    }

    /**
     * Strip target paths ending with {@code metaSuffix} (OFFSET or NULL) when a
     * data path deeper than or equal to the target prefix already covers the
     * same container. Handles map key supplementation when the covering path
     * accesses only VALUES while the target prefix uses {@code *} (≡ VALUES).
     */
    private static void stripMetaByDeeperDataPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            List<List<String>> dataPaths, String metaSuffix) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty() || dataPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> pathsToRemove = new ArrayList<>();
        List<Pair<ColumnAccessPathType, List<String>>> pathsToAdd = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : new ArrayList<>(targetPaths)) {
            List<String> path = p.second;
            if (path.isEmpty() || !metaSuffix.equals(path.get(path.size() - 1))) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            OffsetPathRewrite rewrite = analyzePrefixCoverage(
                    slot.getDataType(), prefix, dataPaths);
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
     */
    private static void stripExactPrefixCoveredMetaPaths(
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

    private static boolean pathCoversPrefix(List<String> path, List<String> prefix) {
        return prefix.size() >= path.size() && prefix.subList(0, path.size()).equals(path);
    }

    /**
     * Walk {@code prefix} and {@code coveringPath} component-by-component, type-aware.
     * Returns {@code remove()} when {@code coveringPath} already reads the container
     * targeted by the OFFSET/NULL path whose prefix is {@code prefix}, making the
     * meta path redundant.
     *
     * <p>Type-specific handling:
     * <ul>
     *   <li><b>Struct</b> — components must match exactly.</li>
     *   <li><b>Array</b> — both components must be {@code *} (ACCESS_ALL).</li>
     *   <li><b>Map</b> — handles {@code *}/VALUES/KEYS equivalence with optional key
     *     path supplementation.</li>
     * </ul>
     */
    private static OffsetPathRewrite compareMetaPathPrefixCoverage(
            DataType slotType, List<String> prefix, List<String> coveringPath) {
        if (coveringPath.isEmpty()) {
            return OffsetPathRewrite.remove();
        }
        int minLen = Math.min(prefix.size(), coveringPath.size());
        List<List<String>> supplementalPaths = new ArrayList<>();
        DataType currentType = slotType;
        for (int i = 0; i < minLen; i++) {
            String prefixComponent = prefix.get(i);
            String coveringPathComponent = coveringPath.get(i);
            if (i == 0) {
                if (!prefixComponent.equals(coveringPathComponent)) {
                    return OffsetPathRewrite.keep();
                }
                continue;
            }
            if (currentType.isStructType()) {
                if (!prefixComponent.equals(coveringPathComponent)) {
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
                if (!prefixComponent.equals(coveringPathComponent)
                        || !AccessPathInfo.ACCESS_ALL.equals(prefixComponent)) {
                    return OffsetPathRewrite.keep();
                }
                currentType = ((ArrayType) currentType).getItemType();
                continue;
            }
            if (currentType.isMapType()) {
                MapType mapType = (MapType) currentType;
                if (prefixComponent.equals(coveringPathComponent)) {
                    currentType = descendMapType(mapType, prefixComponent);
                    continue;
                }
                if (AccessPathInfo.ACCESS_ALL.equals(prefixComponent)
                        && AccessPathInfo.ACCESS_MAP_VALUES.equals(coveringPathComponent)) {
                    supplementalPaths.add(buildMapKeysOnlyPath(prefix, i));
                    currentType = mapType.getValueType();
                    continue;
                }
                if (AccessPathInfo.ACCESS_MAP_VALUES.equals(prefixComponent)
                        && AccessPathInfo.ACCESS_ALL.equals(coveringPathComponent)) {
                    currentType = mapType.getValueType();
                    continue;
                }
                if (AccessPathInfo.ACCESS_MAP_KEYS.equals(prefixComponent)
                        && AccessPathInfo.ACCESS_ALL.equals(coveringPathComponent)) {
                    currentType = mapType.getKeyType();
                    continue;
                }
                return OffsetPathRewrite.keep();
            }
            if (!prefixComponent.equals(coveringPathComponent)) {
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

    // ========================================================================
    // OffsetPathRewrite
    // ========================================================================

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
}
