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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.List;

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

    /**
     * Generate mapping according to source and target relation
     */
    public static List<RelationMapping> generate(List<CatalogRelation> source, List<CatalogRelation> target) {
        Multimap<TableIf, CatalogRelation> queryTableRelationIdMap = ArrayListMultimap.create();
        for (CatalogRelation relation : source) {
            queryTableRelationIdMap.put(relation.getTable(), relation);
        }
        Multimap<TableIf, CatalogRelation> viewTableRelationIdMap = ArrayListMultimap.create();
        for (CatalogRelation relation : target) {
            viewTableRelationIdMap.put(relation.getTable(), relation);
        }
        // todo generate relation map
        return ImmutableList.of();
    }
}
