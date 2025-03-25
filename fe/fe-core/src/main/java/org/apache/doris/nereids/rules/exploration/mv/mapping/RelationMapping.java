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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Relation mapping
 * such as query pattern is a1 left join a2 left join b
 * view pattern is a1 left join a2 left join b. the mapping will be
 * [{a1:a1, a2:a2, b:b}, {a1:a2, a2:a1, b:b}]
 */
public class RelationMapping extends Mapping {

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
     * Generate mapping according to source and target relation
     */
    public static List<RelationMapping> generate(List<CatalogRelation> sources, List<CatalogRelation> targets) {
        // Construct tmp map, key is the table qualifier, value is the corresponding catalog relations
        HashMultimap<TableIdentifier, MappedRelation> sourceTableRelationIdMap = HashMultimap.create();
        for (CatalogRelation relation : sources) {
            sourceTableRelationIdMap.put(getTableIdentifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        HashMultimap<TableIdentifier, MappedRelation> targetTableRelationIdMap = HashMultimap.create();
        for (CatalogRelation relation : targets) {
            targetTableRelationIdMap.put(getTableIdentifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        Set<TableIdentifier> sourceTableKeySet = sourceTableRelationIdMap.keySet();
        List<List<BiMap<MappedRelation, MappedRelation>>> mappedRelations = new ArrayList<>();

        for (TableIdentifier tableIdentifier : sourceTableKeySet) {
            Set<MappedRelation> sourceMappedRelations = sourceTableRelationIdMap.get(tableIdentifier);
            Set<MappedRelation> targetMappedRelations = targetTableRelationIdMap.get(tableIdentifier);
            if (targetMappedRelations.isEmpty()) {
                continue;
            }
            // if source and target relation appear once, just map them
            if (targetMappedRelations.size() == 1 && sourceMappedRelations.size() == 1) {
                ImmutableBiMap.Builder<MappedRelation, MappedRelation> biMapBuilder = ImmutableBiMap.builder();
                mappedRelations.add(ImmutableList.of(
                        biMapBuilder.put(sourceMappedRelations.iterator().next(),
                                targetMappedRelations.iterator().next()).build()));
                continue;
            }
            // relation appear more than once, should cartesian them and power set to correct combination
            // if query is select * from tableA0, tableA1, materialized view is select * from tableA2, tableA3,
            // the relationMappingPowerList in relationMappingPowerList should be bi-direction
            // [
            //    {tableA0 -> tableA2, tableA1 -> tableA3}
            //    {tableA0 -> tableA3, tableA1 -> tableA2}
            // ]
            // query is select * from tableA0, tableA1, tableA4
            List<BiMap<MappedRelation, MappedRelation>> relationMappingPowerList = new ArrayList<>();
            List<Pair<MappedRelation[], MappedRelation[]>> combinations = getUniquePermutation(
                    sourceMappedRelations.toArray(sourceMappedRelations.toArray(new MappedRelation[0])),
                    targetMappedRelations.toArray(new MappedRelation[0]));
            for (Pair<MappedRelation[], MappedRelation[]> combination : combinations) {
                BiMap<MappedRelation, MappedRelation> combinationBiMap = HashBiMap.create();
                MappedRelation[] key = combination.key();
                MappedRelation[] value = combination.value();
                int length = Math.min(key.length, value.length);
                for (int i = 0; i < length; i++) {
                    combinationBiMap.put(key[i], value[i]);
                }
                relationMappingPowerList.add(combinationBiMap);
            }
            mappedRelations.add(relationMappingPowerList);
        }
        // mappedRelations product and merge into each relationMapping
        return Lists.cartesianProduct(mappedRelations).stream()
                .map(RelationMapping::merge)
                .collect(ImmutableList.toImmutableList());
    }

    public static RelationMapping merge(List<BiMap<MappedRelation, MappedRelation>> relationMappings) {
        Builder<MappedRelation, MappedRelation> mappingBuilder = ImmutableBiMap.builder();
        for (BiMap<MappedRelation, MappedRelation> relationMapping : relationMappings) {
            relationMapping.forEach(mappingBuilder::put);
        }
        return RelationMapping.of(mappingBuilder.build());
    }

    private static TableIdentifier getTableIdentifier(TableIf tableIf) {
        return new TableIdentifier(tableIf);
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
    private static List<Pair<MappedRelation[], MappedRelation[]>> getUniquePermutation(
            MappedRelation[] left, MappedRelation[] right) {
        boolean needSwap = left.length > right.length;
        if (needSwap) {
            MappedRelation[] temp = left;
            left = right;
            right = temp;
        }

        boolean[] used = new boolean[right.length];
        MappedRelation[] current = new MappedRelation[left.length];
        List<Pair<MappedRelation[], MappedRelation[]>> results = new ArrayList<>();
        backtrack(left, right, 0, used, current, results);
        if (needSwap) {
            List<Pair<MappedRelation[], MappedRelation[]>> tmpResults = results;
            results = new ArrayList<>();
            for (Pair<MappedRelation[], MappedRelation[]> relation : tmpResults) {
                results.add(Pair.of(relation.value(), relation.key()));
            }
        }
        return results;
    }

    private static void backtrack(MappedRelation[] left, MappedRelation[] right, int index,
            boolean[] used, MappedRelation[] current, List<Pair<MappedRelation[], MappedRelation[]>> results) {
        if (index == left.length) {
            results.add(Pair.of(Arrays.copyOf(left, left.length), Arrays.copyOf(current, current.length)));
            return;
        }

        for (int i = 0; i < right.length; i++) {
            if (!used[i]) {
                used[i] = true;
                current[index] = right[i];
                backtrack(left, right, index + 1, used, current, results);
                used[i] = false;
            }
        }
    }
}
