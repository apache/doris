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
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;

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
        LinkedListMultimap<Long, MappedRelation> sourceTableRelationIdMap = LinkedListMultimap.create();
        for (CatalogRelation relation : sources) {
            sourceTableRelationIdMap.put(getTableQualifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        LinkedListMultimap<Long, MappedRelation> targetTableRelationIdMap = LinkedListMultimap.create();
        for (CatalogRelation relation : targets) {
            targetTableRelationIdMap.put(getTableQualifier(relation.getTable()),
                    MappedRelation.of(relation.getRelationId(), relation));
        }
        Set<Long> sourceTableKeySet = sourceTableRelationIdMap.keySet();
        List<List<Pair<MappedRelation, MappedRelation>>> mappedRelations = new ArrayList<>();

        for (Long sourceTableQualifier : sourceTableKeySet) {
            List<MappedRelation> sourceMappedRelations = sourceTableRelationIdMap.get(sourceTableQualifier);
            List<MappedRelation> targetMappedRelations = targetTableRelationIdMap.get(sourceTableQualifier);
            if (targetMappedRelations.isEmpty()) {
                continue;
            }
            // if source and target relation appear once, just map them
            if (targetMappedRelations.size() == 1 && sourceMappedRelations.size() == 1) {
                mappedRelations.add(ImmutableList.of(Pair.of(sourceMappedRelations.get(0),
                        targetMappedRelations.get(0))));
                continue;
            }
            // relation appear more than once, should cartesian them
            ImmutableList<Pair<MappedRelation, MappedRelation>> relationMapping = Lists.cartesianProduct(
                            sourceTableRelationIdMap.get(sourceTableQualifier), targetMappedRelations)
                    .stream()
                    .map(listPair -> Pair.of(listPair.get(0), listPair.get(1)))
                    .collect(ImmutableList.toImmutableList());
            mappedRelations.add(relationMapping);
        }

        int mappedRelationCount = mappedRelations.size();

        return Lists.cartesianProduct(mappedRelations).stream()
                .map(mappedRelationList -> {
                    Builder<MappedRelation, MappedRelation> mapBuilder = ImmutableBiMap.builder();
                    for (int relationIndex = 0; relationIndex < mappedRelationCount; relationIndex++) {
                        mapBuilder.put(mappedRelationList.get(relationIndex).key(),
                                mappedRelationList.get(relationIndex).value());
                    }
                    return RelationMapping.of(mapBuilder.build());
                })
                .collect(ImmutableList.toImmutableList());
    }

    private static Long getTableQualifier(TableIf tableIf) {
        return tableIf.getId();
    }
}
