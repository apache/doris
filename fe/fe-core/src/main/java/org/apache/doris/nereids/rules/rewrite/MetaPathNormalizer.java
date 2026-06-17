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
import java.util.Optional;

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
     * <p><b>Level 1 — Same-depth priority:</b> when two paths share the same prefix and
     * differ only in the final meta suffix, the higher-priority one eliminates the lower.
     * <pre>{@code
     *   Data  >  OFFSET  >  NULL
     * }</pre>
     * <ul>
     *   <li>{@code Data} strips {@code OFFSET}: {@code [a]} strips {@code [a, OFFSET]}.</li>
     *   <li>{@code Data} strips {@code NULL}:  {@code [a]} strips {@code [a, NULL]}.</li>
     *   <li>{@code OFFSET} strips {@code NULL}: {@code [a, OFFSET]} strips {@code [a, NULL]}.</li>
     * </ul>
     *
     * <p><b>Level 2 — Deeper path covers shallower meta:</b> when a covering path goes
     * deeper into the type tree, its data reader already materialises the container,
     * making a shallower meta-only path redundant.
     * <ul>
     *   <li>Target suffix {@code OFFSET}, covered by deeper:
     *     <ul>
     *       <li>{@code Data}:   {@code [a, *, field]}  strips {@code [a, OFFSET]}.</li>
     *       <li>{@code OFFSET}: {@code [a, *, OFFSET]} strips {@code [a, OFFSET]}.</li>
     *       <li>{@code NULL}:   {@code [a, *, NULL]}   strips {@code [a, OFFSET]}.</li>
     *     </ul>
     *   </li>
     *   <li>Target suffix {@code NULL}, covered by deeper:
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
        // Level 1: same-depth priority — Data > OFFSET > NULL.
        stripExactPrefixCoveredMetaPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripNullBySameDepthOffset(slot, targetAccessPaths);

        // Level 2: deeper path covers shallower meta path.
        stripShallowerOffsetPaths(slot, targetAccessPaths, coveringAccessPaths);
        stripShallowerNullPaths(slot, targetAccessPaths);

        // Special: array NULL + map key supplementation.
        stripCoveredArrayNullMetaPaths(slot, targetAccessPaths, coveringAccessPaths);
    }

    /**
     * Level 1 — same-depth OFFSET strips NULL: {@code [a, OFFSET]} strips
     * {@code [a, NULL]}.
     */
    private static void stripNullBySameDepthOffset(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> allAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> slotPaths = allAccessPaths.get(slotId);
        if (slotPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
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
                if (other.size() == path.size()
                        && AccessPathInfo.ACCESS_OFFSET.equals(other.get(other.size() - 1))
                        && other.subList(0, prefix.size()).equals(prefix)) {
                    toRemove.add(p);
                    break;
                }
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> r : toRemove) {
            allAccessPaths.remove(slotId, r);
        }
    }

    /**
     * Decide whether an OFFSET-suffix path can be removed because another path already covers
     * the same container.
     */
    private static OffsetPathRewrite analyzeOffsetPathRewrite(
            DataType slotType, List<String> path, List<List<String>> coveringPaths) {
        if (path.isEmpty()
                || !AccessPathInfo.ACCESS_OFFSET.equals(path.get(path.size() - 1))) {
            return OffsetPathRewrite.keep();
        }
        List<String> prefix = path.subList(0, path.size() - 1);
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
            OffsetPathRewrite candidate = compareMetaPathPrefixCoverage(slotType, prefix, coveringPath);
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
     * Level 2 — deeper paths cover shallower OFFSET paths:
     * <ul>
     *   <li>Deeper {@code Data}:   delegates to {@link #stripCoveredOffsetByPaths}
     *       for map key supplementation.</li>
     *   <li>Deeper {@code OFFSET} / {@code NULL}: delegates to
     *       {@link #stripCoveredMetaByPrefix}.</li>
     * </ul>
     */
    private static void stripShallowerOffsetPaths(
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

        List<List<String>> deeperMetaPaths = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : coveringAccessPaths.get(slotId)) {
            if (!p.second.isEmpty() && isMetaPath(p.second)) {
                deeperMetaPaths.add(p.second);
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            if (!p.second.isEmpty() && isMetaPath(p.second)) {
                deeperMetaPaths.add(p.second);
            }
        }
        stripCoveredMetaByPrefix(slot.getDataType(), slotId, AccessPathInfo.ACCESS_OFFSET,
                deeperMetaPaths, targetAccessPaths);
    }

    /**
     * Level 2 — deeper paths cover shallower NULL paths:
     * <ul>
     *   <li>Deeper {@code Data}: same-depth exact prefix match strips NULL
     *       (e.g. {@code [a]} strips {@code [a, NULL]}), and deeper paths use
     *       {@link #compareMetaPathPrefixCoverage} for type-aware comparison
     *       (e.g. {@code [m, *, v]} strips {@code [m, VALUES, NULL]}).</li>
     *   <li>Deeper {@code OFFSET} / {@code NULL}: delegates to
     *       {@link #stripCoveredMetaByPrefix}.</li>
     * </ul>
     */
    private static void stripShallowerNullPaths(
            Slot slot, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> allAccessPaths) {
        int slotId = slot.getExprId().asInt();
        Collection<Pair<ColumnAccessPathType, List<String>>> slotPaths = allAccessPaths.get(slotId);
        if (slotPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : slotPaths) {
            List<String> path = p.second;
            if (path.isEmpty() || !AccessPathInfo.ACCESS_NULL.equals(path.get(path.size() - 1))) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            for (Pair<ColumnAccessPathType, List<String>> q : slotPaths) {
                List<String> other = q.second;
                if (other == path || other.isEmpty() || isMetaPath(other)) {
                    continue;
                }
                if (other.equals(prefix)) {
                    toRemove.add(p);
                    break;
                }
                if (other.size() > prefix.size()
                        && compareMetaPathPrefixCoverage(
                                slot.getDataType(), prefix, other)
                                .shouldRemoveOffsetPath()) {
                    toRemove.add(p);
                    break;
                }
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> r : toRemove) {
            allAccessPaths.remove(slotId, r);
        }

        List<List<String>> metaPaths = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : slotPaths) {
            if (!p.second.isEmpty() && isMetaPath(p.second)) {
                metaPaths.add(p.second);
            }
        }
        stripCoveredMetaByPrefix(slot.getDataType(), slotId, AccessPathInfo.ACCESS_NULL,
                metaPaths, allAccessPaths);
    }

    /**
     * Level 2 — for each target path ending with {@code targetSuffix}, remove it
     * when a strictly deeper meta path has the target prefix as a strict prefix.
     *
     * <p>Both target and covering paths have their meta suffix stripped before
     * comparison, so only genuinely deeper paths match. Same-depth cross-type
     * is handled by {@link #stripNullBySameDepthOffset} instead.
     */
    private static void stripCoveredMetaByPrefix(
            DataType slotType, int slotId, String targetSuffix,
            List<List<String>> coveringMetaPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths) {
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths =
                targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty() || coveringMetaPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> path = p.second;
            if (path.isEmpty() || !targetSuffix.equals(path.get(path.size() - 1))) {
                continue;
            }
            List<String> targetPrefix = path.subList(0, path.size() - 1);
            for (List<String> other : coveringMetaPaths) {
                if (other == path || other.isEmpty()) {
                    continue;
                }
                OffsetPathRewrite rewrite = compareMetaPathPrefixCoverage(
                        slotType, targetPrefix, other);
                if (rewrite.shouldRemoveOffsetPath()) {
                    toRemove.add(p);
                    break;
                }
            }
        }
        for (Pair<ColumnAccessPathType, List<String>> r : toRemove) {
            targetAccessPaths.remove(slotId, r);
        }
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
     * the same array container or data under it.
     */
    private static void stripCoveredArrayNullMetaPaths(
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

    /**
     * Walk {@code prefix} and {@code nonOffset} component-by-component, type-aware.
     * Returns {@code remove()} when {@code nonOffset} already reads the container
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
