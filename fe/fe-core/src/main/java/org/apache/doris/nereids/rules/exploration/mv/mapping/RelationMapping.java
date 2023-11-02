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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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

    private final BiMap<MappedRelation, MappedRelation> mappedRelationMap;

    public RelationMapping(BiMap<MappedRelation, MappedRelation> mappedRelationMap) {
        this.mappedRelationMap = mappedRelationMap;
    }

    public BiMap<MappedRelation, MappedRelation> getMappedRelationMap() {
        return mappedRelationMap;
    }

    public static RelationMapping of(BiMap<MappedRelation, MappedRelation> mappedRelationMap) {
        return new RelationMapping(mappedRelationMap);
    }

    /**
     * Generate mapping according to source and target relation
     */
    public static List<RelationMapping> generate(List<CatalogRelation> sources, List<CatalogRelation> targets) {
        // Construct tmp map, key is the table qualifier, value is the corresponding catalog relations
        LinkedListMultimap<String, MappedRelation> sourceTableRelationIdMap = LinkedListMultimap.create();
        for (CatalogRelation relation : sources) {
            String tableQualifier = getTableQualifier(relation.getTable());
            if (tableQualifier == null) {
                return null;
            }
            sourceTableRelationIdMap.put(tableQualifier, MappedRelation.of(relation.getRelationId(), relation));
        }
        LinkedListMultimap<String, MappedRelation> targetTableRelationIdMap = LinkedListMultimap.create();
        for (CatalogRelation relation : targets) {
            String tableQualifier = getTableQualifier(relation.getTable());
            if (tableQualifier == null) {
                return null;
            }
            targetTableRelationIdMap.put(tableQualifier, MappedRelation.of(relation.getRelationId(), relation));
        }

        Set<String> sourceTableKeySet = sourceTableRelationIdMap.keySet();
        List<List<Pair<MappedRelation, MappedRelation>>> mappedRelations = new ArrayList<>();

        for (String sourceTableQualifier : sourceTableKeySet) {
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
                    BiMap<MappedRelation, MappedRelation> relationMappedRelationBiMap =
                            HashBiMap.create(mappedRelationCount);
                    for (int relationIndex = 0; relationIndex < mappedRelationCount; relationIndex++) {
                        relationMappedRelationBiMap.put(mappedRelationList.get(relationIndex).key(),
                                mappedRelationList.get(relationIndex).value());
                    }
                    return RelationMapping.of(relationMappedRelationBiMap);
                })
                .collect(ImmutableList.toImmutableList());
    }

    private static String getTableQualifier(TableIf tableIf) {
        String tableName = tableIf.getName();
        DatabaseIf database = tableIf.getDatabase();
        if (database == null) {
            return null;
        }
        return database.getFullName() + ":" + tableName;
    }
}
