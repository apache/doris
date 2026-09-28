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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.lance.job.LanceIndexNameNormalizer;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The section 3.4 family comparison between the logical index vocabulary (concrete algorithms
 * such as IVF_PQ or BTREE reported by {@code describeIndices}) and the physical one (the
 * umbrella manifest types VECTOR/SCALAR alongside concrete algorithm names from
 * {@code getIndexes}), plus the section 4.1 case-only name-collision handling admission needs
 * for its IF preflight.
 *
 * <p>There is deliberately no mapping between any Doris internal index family and a Lance
 * family: names outside the Lance vocabulary (for example the SQL category ANN) never match.
 */
final class LanceIndexFamilies {
    private static final String PHYSICAL_VECTOR = "vector";
    private static final String PHYSICAL_SCALAR = "scalar";
    /** Supported vector algorithms in normalized form (design section 2.4). */
    private static final Set<String> SUPPORTED_VECTOR_ALGORITHMS = ImmutableSet.of("ivfpq");
    /** Supported scalar algorithms in normalized form (design section 2.4). */
    private static final Set<String> SUPPORTED_SCALAR_ALGORITHMS = ImmutableSet.of("btree", "bitmap");

    private LanceIndexFamilies() {
    }

    /** Family normalization: case-fold under the root locale and ignore underscores. */
    static String normalize(String logicalOrPhysicalType) {
        if (logicalOrPhysicalType == null) {
            throw new IllegalArgumentException("Lance index type must not be null");
        }
        return logicalOrPhysicalType.toLowerCase(Locale.ROOT).replace("_", "");
    }

    /**
     * True when the physical entry can back the logical algorithm: identical normalized names
     * always match (including provider spelling variants such as BTREE versus BTree); the
     * VECTOR umbrella accepts only the supported vector algorithms; the SCALAR umbrella
     * accepts only the supported scalar algorithms; anything else never matches.
     */
    static boolean isCompatible(String logicalType, String physicalIndexTypeName) {
        String logical = normalize(logicalType);
        String physical = normalize(physicalIndexTypeName);
        if (physical.equals(logical)) {
            return true;
        }
        if (PHYSICAL_VECTOR.equals(physical)) {
            return SUPPORTED_VECTOR_ALGORITHMS.contains(logical);
        }
        if (PHYSICAL_SCALAR.equals(physical)) {
            return SUPPORTED_SCALAR_ALGORITHMS.contains(logical);
        }
        return false;
    }

    /**
     * True when two or more distinct display names normalize (name normalization v1, not the
     * family rule above) to {@code normalizedTarget} — an external case-only collision that
     * admission must refuse rather than guess (design section 4.1).
     */
    static boolean isAmbiguousCaseCollision(Collection<String> displayNames, String normalizedTarget) {
        return matchingDisplayNames(displayNames, normalizedTarget).size() > 1;
    }

    /**
     * Returns the single stored display name matching {@code normalizedTarget}, or null when
     * there is no match or the match is ambiguous.
     */
    @Nullable
    static String uniqueMatch(Collection<String> displayNames, String normalizedTarget) {
        Set<String> matches = matchingDisplayNames(displayNames, normalizedTarget);
        return matches.size() == 1 ? matches.iterator().next() : null;
    }

    private static Set<String> matchingDisplayNames(Collection<String> displayNames,
            String normalizedTarget) {
        if (displayNames == null || normalizedTarget == null) {
            throw new IllegalArgumentException(
                    "display names and normalized target must not be null");
        }
        Set<String> matches = new LinkedHashSet<>();
        for (String displayName : displayNames) {
            if (displayName == null) {
                continue;
            }
            if (LanceIndexNameNormalizer.normalize(displayName).equals(normalizedTarget)) {
                matches.add(displayName);
            }
        }
        return matches;
    }
}
