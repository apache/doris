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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One-to-one mapping set between query relations and view relations.
 */
public class RelationMapping extends Mapping {
    private static final Logger LOG = LogManager.getLogger(RelationMapping.class);
    private final ImmutableBiMap<MappedRelation, MappedRelation> mappedRelationMap;

    public RelationMapping(ImmutableBiMap<MappedRelation, MappedRelation> mappedRelationMap) {
        this.mappedRelationMap = mappedRelationMap;
    }

    public BiMap<MappedRelation, MappedRelation> getMappedRelationMap() {
        return mappedRelationMap;
    }

    public static RelationMapping of(ImmutableBiMap<MappedRelation, MappedRelation> mappedRelationMap) {
        return new RelationMapping(mappedRelationMap);
    }

    /**
     * Generate all possible RelationMappings by source and target relations.
     *
     * Every source relation must be mapped. The target side may have more same-table relations
     * for query-partial rewrite.
     */
    public static List<RelationMapping> generate(List<CatalogRelation> sources, List<CatalogRelation> targets,
            int maxMappingCount) {
        HashMultimap<TableIdentifier, MappedRelation> sourceBuckets = HashMultimap.create();
        for (CatalogRelation relation : sources) {
            sourceBuckets.put(new TableIdentifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        HashMultimap<TableIdentifier, MappedRelation> targetBuckets = HashMultimap.create();
        for (CatalogRelation relation : targets) {
            targetBuckets.put(new TableIdentifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        List<List<BiMap<MappedRelation, MappedRelation>>> mappedRelations = new ArrayList<>();

        for (TableIdentifier relationIdentity : sourceBuckets.keySet()) {
            Set<MappedRelation> sourceMappedRelations = sourceBuckets.get(relationIdentity);
            Set<MappedRelation> targetMappedRelations = targetBuckets.get(relationIdentity);
            if (sourceMappedRelations.size() > targetMappedRelations.size()) {
                return ImmutableList.of();
            }
            if (targetMappedRelations.size() == 1 && sourceMappedRelations.size() == 1) {
                MappedRelation sourceRelation = sourceMappedRelations.iterator().next();
                MappedRelation targetRelation = targetMappedRelations.iterator().next();
                ImmutableBiMap.Builder<MappedRelation, MappedRelation> biMapBuilder = ImmutableBiMap.builder();
                mappedRelations.add(ImmutableList.of(
                        biMapBuilder.put(sourceRelation, targetRelation).build()));
                continue;
            }
            List<BiMap<MappedRelation, MappedRelation>> relationMappingPowerList = new ArrayList<>();
            List<Pair<MappedRelation[], MappedRelation[]>> combinations = getUniquePermutation(
                    sourceMappedRelations.toArray(new MappedRelation[0]),
                    targetMappedRelations.toArray(new MappedRelation[0]), maxMappingCount);
            for (Pair<MappedRelation[], MappedRelation[]> combination : combinations) {
                BiMap<MappedRelation, MappedRelation> combinationBiMap = HashBiMap.create();
                MappedRelation[] key = combination.key();
                MappedRelation[] value = combination.value();
                for (int i = 0; i < key.length; i++) {
                    combinationBiMap.put(key[i], value[i]);
                }
                relationMappingPowerList.add(combinationBiMap);
            }
            mappedRelations.add(relationMappingPowerList);
        }
        List<RelationMapping> relationMappings = new ArrayList<>();
        buildRelationMappings(mappedRelations, 0, new ArrayList<>(), relationMappings, maxMappingCount);
        return ImmutableList.copyOf(relationMappings);
    }

    public static RelationMapping merge(List<BiMap<MappedRelation, MappedRelation>> relationMappings) {
        Builder<MappedRelation, MappedRelation> mappingBuilder = ImmutableBiMap.builder();
        for (BiMap<MappedRelation, MappedRelation> relationMapping : relationMappings) {
            relationMapping.forEach(mappingBuilder::put);
        }
        return RelationMapping.of(mappingBuilder.build());
    }

    /**
     * Build full RelationMappings by taking the cartesian product of per-bucket local mappings.
     *
     * Example:
     * mappedRelations =
     * [
     *   [{q1 -> v1, q2 -> v2}, {q1 -> v2, q2 -> v1}],
     *   [{q3 -> v3}, {q3 -> v4}]
     * ]
     *
     * The backtracking process does:
     * 1. Pick one local mapping from bucket 0.
     * 2. Pick one local mapping from bucket 1.
     * 3. After all buckets are chosen, merge the current path into one complete RelationMapping.
     *
     * The example above produces four final mappings:
     * - {q1 -> v1, q2 -> v2, q3 -> v3}
     * - {q1 -> v1, q2 -> v2, q3 -> v4}
     * - {q1 -> v2, q2 -> v1, q3 -> v3}
     * - {q1 -> v2, q2 -> v1, q3 -> v4}
     */
    private static void buildRelationMappings(List<List<BiMap<MappedRelation, MappedRelation>>> mappedRelations,
            int offset, List<BiMap<MappedRelation, MappedRelation>> currentMappings,
            List<RelationMapping> relationMappings, int maxMappingCount) {
        if (relationMappings.size() >= maxMappingCount) {
            return;
        }
        if (offset >= mappedRelations.size()) {
            // The current path already contains one local mapping from each bucket.
            relationMappings.add(merge(currentMappings));
            return;
        }
        for (BiMap<MappedRelation, MappedRelation> mappedRelation : mappedRelations.get(offset)) {
            // Choose one local mapping from the current bucket and continue with the next bucket.
            currentMappings.add(mappedRelation);
            buildRelationMappings(mappedRelations, offset + 1, currentMappings, relationMappings,
                    maxMappingCount);
            // Backtrack and try the next local mapping in the same bucket.
            currentMappings.remove(currentMappings.size() - 1);
            if (relationMappings.size() >= maxMappingCount) {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "RelationMapping { mappedRelationMap=" + mappedRelationMap + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelationMapping that = (RelationMapping) o;
        return Objects.equals(mappedRelationMap, that.mappedRelationMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappedRelationMap);
    }

    /**
     * Permutation and remove duplicated element
     * For example:
     * Given [1, 4, 5] and [191, 194, 195]
     * This would return
     * [
     * [(1, 191) (4, 194) (5, 195)],
     * [(1, 191) (4, 195) (5, 194)],
     * [(1, 194) (4, 191) (5, 195)],
     * [(1, 194) (4, 195) (5, 191)],
     * [(1, 195) (4, 191) (5, 194)],
     * [(1, 195) (4, 194) (5, 191)]
     * ]
     * */
    public static List<Pair<MappedRelation[], MappedRelation[]>> getUniquePermutation(
            MappedRelation[] left, MappedRelation[] right, int maxMappingCount) {
        boolean needSwap = left.length > right.length;
        if (needSwap) {
            MappedRelation[] temp = left;
            left = right;
            right = temp;
        }

        boolean[] used = new boolean[right.length];
        MappedRelation[] current = new MappedRelation[left.length];
        List<Pair<MappedRelation[], MappedRelation[]>> results = new ArrayList<>();
        backtrack(left, right, 0, used, current, results, maxMappingCount);
        if (needSwap) {
            List<Pair<MappedRelation[], MappedRelation[]>> tmpResults = results;
            results = new ArrayList<>();
            for (Pair<MappedRelation[], MappedRelation[]> relation : tmpResults) {
                results.add(Pair.of(relation.value(), relation.key()));
            }
        }
        return results;
    }

    /** Pure permutation backtracking; compatibility is filtered by the caller. */
    private static void backtrack(MappedRelation[] left, MappedRelation[] right, int index,
            boolean[] used, MappedRelation[] current, List<Pair<MappedRelation[], MappedRelation[]>> results,
            int maxMappingCount) {
        if (results.size() >= maxMappingCount) {
            LOG.warn("queryToViewTableMappings is over limit and be intercepted, "
                            + "results size is {}, MappedRelation left is {}, MappedRelation right is {}",
                    results.size(), Arrays.toString(left), Arrays.toString(right));
            return;
        }
        if (index == left.length) {
            results.add(Pair.of(left, Arrays.copyOf(current, current.length)));
            return;
        }

        for (int i = 0; i < right.length; i++) {
            if (!used[i]) {
                used[i] = true;
                current[index] = right[i];
                backtrack(left, right, index + 1, used, current, results, maxMappingCount);
                used[i] = false;
            }
        }
    }
}
