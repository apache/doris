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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
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
        HashMultimap<Long, MappedRelation> sourceTableRelationIdMap = HashMultimap.create();
        for (CatalogRelation relation : sources) {
            sourceTableRelationIdMap.put(getTableQualifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        HashMultimap<Long, MappedRelation> targetTableRelationIdMap = HashMultimap.create();
        for (CatalogRelation relation : targets) {
            targetTableRelationIdMap.put(getTableQualifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        Set<Long> sourceTableKeySet = sourceTableRelationIdMap.keySet();
        List<List<RelationMapping>> mappedRelations = new ArrayList<>();

        for (Long sourceTableQualifier : sourceTableKeySet) {
            Set<MappedRelation> sourceMappedRelations = sourceTableRelationIdMap.get(sourceTableQualifier);
            Set<MappedRelation> targetMappedRelations = targetTableRelationIdMap.get(sourceTableQualifier);
            if (targetMappedRelations.isEmpty()) {
                continue;
            }
            // if source and target relation appear once, just map them
            if (targetMappedRelations.size() == 1 && sourceMappedRelations.size() == 1) {
                ImmutableBiMap.Builder<MappedRelation, MappedRelation> biMapBuilder = ImmutableBiMap.builder();
                mappedRelations.add(ImmutableList.of(
                        RelationMapping.of(biMapBuilder.put(sourceMappedRelations.iterator().next(),
                                targetMappedRelations.iterator().next()).build())));
                continue;
            }
            // relation appear more than once, should cartesian them and power set to correct combination
            // if query is a0, a1, view is b0, b1
            // relationMapping will be
            // a0 b0
            // a0 b1
            // a1 b0
            // a1 b1
            ImmutableList<Pair<MappedRelation, MappedRelation>> relationMapping = Sets.cartesianProduct(
                            sourceMappedRelations, targetMappedRelations)
                    .stream()
                    .map(listPair -> Pair.of(listPair.get(0), listPair.get(1)))
                    .collect(ImmutableList.toImmutableList());

            // the mapping in relationMappingPowerList should be bi-direction
            // [
            //    {a0-b0, a1-b1}
            //    {a1-b0, a0-b1}
            // ]
            List<RelationMapping> relationMappingPowerList = new ArrayList<>();
            int relationMappingSize = relationMapping.size();
            int relationMappingMinSize = Math.min(sourceMappedRelations.size(), targetMappedRelations.size());
            for (int i = 0; i < relationMappingSize; i++) {
                HashBiMap<MappedRelation, MappedRelation> relationBiMap = HashBiMap.create();
                relationBiMap.put(relationMapping.get(i).key(), relationMapping.get(i).value());
                for (int j = i + 1; j < relationMappingSize; j++) {
                    if (!relationBiMap.containsKey(relationMapping.get(j).key())
                            && !relationBiMap.containsValue(relationMapping.get(j).value())) {
                        relationBiMap.put(relationMapping.get(j).key(), relationMapping.get(j).value());
                    }
                }
                // mapping should contain min num of relation in source or target at least
                if (relationBiMap.size() >= relationMappingMinSize) {
                    relationMappingPowerList.add(RelationMapping.of(ImmutableBiMap.copyOf(relationBiMap)));
                }
            }
            mappedRelations.add(relationMappingPowerList);
        }
        // mappedRelations product and merge into each relationMapping
        return Lists.cartesianProduct(mappedRelations).stream()
                .map(RelationMapping::merge)
                .collect(ImmutableList.toImmutableList());
    }

    public static RelationMapping merge(List<RelationMapping> relationMappings) {
        Builder<MappedRelation, MappedRelation> mappingBuilder = ImmutableBiMap.builder();
        for (RelationMapping relationMapping : relationMappings) {
            relationMapping.getMappedRelationMap().forEach(mappingBuilder::put);
        }
        return RelationMapping.of(mappingBuilder.build());
    }

    private static Long getTableQualifier(TableIf tableIf) {
        return tableIf.getId();
    }
}
