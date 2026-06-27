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

import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Strips redundant metadata-only (NULL/OFFSET) access paths using pure
 * string-prefix comparison.  Map-level wildcards must already have been
 * expanded into {@code KEYS} + {@code VALUES} by the caller.
 *
 * <p>Single entry point: {@link #strip(int, Multimap, Multimap)}.
 */
public final class MetaPathStriper {

    private MetaPathStriper() {}

    /**
     * Strip redundant metadata-only NULL/OFFSET paths using pure string-prefix
     * comparison, keeping enough real paths for BE readers to avoid
     * OFFSET_ONLY / NULL_MAP_ONLY modes that skip required child data.
     *
     * <p>Stripping is organised in two levels:
     *
     * <p><b>Level 1 — Same-prefix priority:</b> when two paths share the same
     * prefix and differ only in the final meta suffix, the higher-priority one
     * eliminates the lower.
     * <pre>{@code
     *   Data  >  OFFSET  >  NULL
     * }</pre>
     * <ul>
     *   <li>{@code Data} strips {@code OFFSET}: {@code [a]} strips {@code [a, OFFSET]}.</li>
     *   <li>{@code Data} strips {@code NULL}:  {@code [a]} strips {@code [a, NULL]}.</li>
     *   <li>{@code OFFSET} strips {@code NULL}: {@code [a, OFFSET]} strips
     *       {@code [a, NULL]}.</li>
     * </ul>
     *
     * <p><b>Level 2 — Deeper-prefix coverage:</b> when a covering path goes
     * deeper into the type tree, its data reader already materialises the
     * container, making a shallower meta-only path redundant.
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
     * <p>Pre-condition: map-level {@code *} wildcards must already have been
     * expanded into {@code KEYS} + {@code VALUES} by the caller.  This class
     * uses pure string-prefix comparison and is type-unaware.
     */
    public static void strip(
            int slotId,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        stripExactPrefixCoveredMetaPaths(slotId, targetAccessPaths, coveringAccessPaths);
        stripNullBySamePrefixOffset(slotId, targetAccessPaths);

        stripMetaPathsByDeeperPrefix(slotId, AccessPathInfo.ACCESS_OFFSET,
                targetAccessPaths, coveringAccessPaths);
        stripMetaPathsByDeeperPrefix(slotId, AccessPathInfo.ACCESS_NULL,
                targetAccessPaths, coveringAccessPaths);
    }

    // ========================================================================
    // Path helpers
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

    private static boolean isPrefixCovered(List<String> prefix, List<String> coveringPath) {
        if (coveringPath.isEmpty()) {
            return true;
        }
        int minLen = Math.min(prefix.size(), coveringPath.size());
        for (int i = 0; i < minLen; i++) {
            if (!prefix.get(i).equals(coveringPath.get(i))) {
                return false;
            }
        }
        return true;
    }

    // ========================================================================
    // Level 1 — same-prefix priority
    // ========================================================================

    /**
     * {@code [prefix]} strips {@code [prefix, OFFSET]} and {@code [prefix, NULL]}.
     */
    private static void stripExactPrefixCoveredMetaPaths(
            int slotId,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }

        List<List<String>> fullAccessPaths = collectPaths(
                coveringAccessPaths.get(slotId), targetPaths, false);

        List<Pair<ColumnAccessPathType, List<String>>> pathsToRemove = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : targetPaths) {
            List<String> path = p.second;
            if (!isMetaPath(path)) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            for (List<String> fullAccessPath : fullAccessPaths) {
                if (isPrefixCovered(prefix, fullAccessPath)
                        && prefix.size() >= fullAccessPath.size()) {
                    pathsToRemove.add(p);
                    break;
                }
            }
        }
        targetPaths.removeAll(pathsToRemove);
    }

    /**
     * {@code [prefix, OFFSET]} strips {@code [prefix, NULL]}.
     */
    private static void stripNullBySamePrefixOffset(
            int slotId, Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> allAccessPaths) {
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
                if (other.size() != path.size()
                        || !AccessPathInfo.ACCESS_OFFSET.equals(other.get(other.size() - 1))) {
                    continue;
                }
                List<String> otherPrefix = other.subList(0, other.size() - 1);
                if (isPrefixCovered(prefix, otherPrefix)) {
                    toRemove.add(p);
                    break;
                }
            }
        }
        slotPaths.removeAll(toRemove);
    }

    // ========================================================================
    // Level 2 — deeper-prefix coverage
    // ========================================================================

    private static void stripMetaPathsByDeeperPrefix(
            int slotId, String metaSuffix,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> coveringAccessPaths) {
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths = targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty()) {
            return;
        }
        Collection<Pair<ColumnAccessPathType, List<String>>> coveringPaths =
                coveringAccessPaths.get(slotId);

        List<List<String>> dataPaths = collectPaths(coveringPaths, targetPaths, false);
        stripMetaByDeeperDataPaths(slotId, targetAccessPaths, dataPaths, metaSuffix);

        List<List<String>> metaPaths = collectPaths(coveringPaths, targetPaths, true);
        stripMetaByDeeperMetaPaths(slotId, metaSuffix, metaPaths, targetAccessPaths);
    }

    /**
     * Strip target meta paths covered by a data path.
     */
    private static void stripMetaByDeeperDataPaths(
            int slotId,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths,
            List<List<String>> dataPaths, String metaSuffix) {
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths =
                targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty() || dataPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
        for (Pair<ColumnAccessPathType, List<String>> p : new ArrayList<>(targetPaths)) {
            List<String> path = p.second;
            if (path.isEmpty() || !metaSuffix.equals(path.get(path.size() - 1))) {
                continue;
            }
            List<String> prefix = path.subList(0, path.size() - 1);
            for (List<String> dataPath : dataPaths) {
                if (isPrefixCovered(prefix, dataPath)) {
                    toRemove.add(p);
                    break;
                }
            }
        }
        targetPaths.removeAll(toRemove);
    }

    /**
     * Strip target meta paths covered by a strictly deeper meta path.
     */
    private static void stripMetaByDeeperMetaPaths(
            int slotId, String metaSuffix,
            List<List<String>> coveringPaths,
            Multimap<Integer, Pair<ColumnAccessPathType, List<String>>> targetAccessPaths) {
        Collection<Pair<ColumnAccessPathType, List<String>>> targetPaths =
                targetAccessPaths.get(slotId);
        if (targetPaths.isEmpty() || coveringPaths.isEmpty()) {
            return;
        }

        List<Pair<ColumnAccessPathType, List<String>>> toRemove = new ArrayList<>();
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
                if (coveringPath.size() - 1 <= targetPath.size() - 1) {
                    continue;
                }
                if (isPrefixCovered(targetPrefix, coveringPath)) {
                    toRemove.add(p);
                    break;
                }
            }
        }
        targetPaths.removeAll(toRemove);
    }
}
