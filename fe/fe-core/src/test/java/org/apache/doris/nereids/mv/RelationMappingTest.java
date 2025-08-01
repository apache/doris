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

package org.apache.doris.nereids.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.exploration.mv.mapping.Mapping.MappedRelation;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RelationMappingTest {

    @Mocked
    public CatalogRelation catalogRelation;

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
    @Test
    public void testGetPermutation() {

        MappedRelation mr1 = MappedRelation.of(new RelationId(1), catalogRelation);
        MappedRelation mr4 = MappedRelation.of(new RelationId(4), catalogRelation);
        MappedRelation mr5 = MappedRelation.of(new RelationId(5), catalogRelation);

        MappedRelation mr191 = MappedRelation.of(new RelationId(191), catalogRelation);
        MappedRelation mr194 = MappedRelation.of(new RelationId(194), catalogRelation);
        MappedRelation mr195 = MappedRelation.of(new RelationId(195), catalogRelation);

        MappedRelation[] left = new MappedRelation[] {mr1, mr4, mr5};
        MappedRelation[] right = new MappedRelation[] {mr191, mr194, mr195};

        List<Pair<MappedRelation[], MappedRelation[]>> uniquePermutation = RelationMapping.getUniquePermutation(left,
                right, 10);
        Set<BiMap<Integer, Integer>> generatedMappedSet = generateDirectPermutations(uniquePermutation);

        Set<BiMap<Integer, Integer>> exceptedSet = new HashSet<>();
        BiMap<Integer, Integer> eachMap = HashBiMap.create();
        eachMap.put(1, 195);
        eachMap.put(4, 194);
        eachMap.put(5, 191);
        exceptedSet.add(eachMap);
        eachMap = HashBiMap.create();
        eachMap.put(1, 194);
        eachMap.put(4, 191);
        eachMap.put(5, 195);
        exceptedSet.add(eachMap);
        eachMap = HashBiMap.create();
        eachMap.put(1, 194);
        eachMap.put(4, 195);
        eachMap.put(5, 191);
        exceptedSet.add(eachMap);
        eachMap = HashBiMap.create();
        eachMap.put(1, 195);
        eachMap.put(4, 191);
        eachMap.put(5, 194);
        exceptedSet.add(eachMap);
        eachMap = HashBiMap.create();
        eachMap.put(1, 191);
        eachMap.put(4, 194);
        eachMap.put(5, 195);
        exceptedSet.add(eachMap);
        eachMap = HashBiMap.create();
        eachMap.put(1, 191);
        eachMap.put(4, 195);
        eachMap.put(5, 194);
        exceptedSet.add(eachMap);

        Assertions.assertEquals(exceptedSet, generatedMappedSet);
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
    @Test
    public void testGetPermutationWithLimit() {

        MappedRelation mr1 = MappedRelation.of(new RelationId(1), catalogRelation);
        MappedRelation mr4 = MappedRelation.of(new RelationId(4), catalogRelation);
        MappedRelation mr5 = MappedRelation.of(new RelationId(5), catalogRelation);

        MappedRelation mr191 = MappedRelation.of(new RelationId(191), catalogRelation);
        MappedRelation mr194 = MappedRelation.of(new RelationId(194), catalogRelation);
        MappedRelation mr195 = MappedRelation.of(new RelationId(195), catalogRelation);

        MappedRelation[] left = new MappedRelation[] {mr1, mr4, mr5};
        MappedRelation[] right = new MappedRelation[] {mr191, mr194, mr195};

        List<Pair<MappedRelation[], MappedRelation[]>> uniquePermutation = RelationMapping.getUniquePermutation(left,
                right, 5);

        Assertions.assertEquals(5, uniquePermutation.size());
    }

    private Set<BiMap<Integer, Integer>> generateDirectPermutations(
            List<Pair<MappedRelation[], MappedRelation[]>> uniquePermutation) {
        Set<BiMap<Integer, Integer>> relationMappingPowerSet = new HashSet<>();
        for (Pair<MappedRelation[], MappedRelation[]> combination : uniquePermutation) {
            BiMap<Integer, Integer> combinationBiMap = HashBiMap.create();
            MappedRelation[] key = combination.key();
            MappedRelation[] value = combination.value();
            int length = Math.min(key.length, value.length);
            for (int i = 0; i < length; i++) {
                combinationBiMap.put(key[i].getRelationId().asInt(), value[i].getRelationId().asInt());
            }
            relationMappingPowerSet.add(combinationBiMap);
        }
        return relationMappingPowerSet;
    }

}
