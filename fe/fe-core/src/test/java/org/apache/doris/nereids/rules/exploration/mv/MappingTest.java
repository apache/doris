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

import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * MappingTest
 */
public class MappingTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("mapping_test");
        useDatabase("mapping_test");

        createTable("CREATE TABLE IF NOT EXISTS lineitem (\n"
                + "  L_ORDERKEY    INTEGER NOT NULL,\n"
                + "  L_PARTKEY     INTEGER NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(L_ORDERKEY, L_PARTKEY)\n"
                + "DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        createTable("CREATE TABLE IF NOT EXISTS orders  (\n"
                + "  O_ORDERKEY       INTEGER NOT NULL,\n"
                + "  O_CUSTKEY        INTEGER NOT NULL,\n"
                + "  O_ORDERSTATUS    CHAR(1) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)\n"
                + "DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
        createTable("CREATE TABLE IF NOT EXISTS customer (\n"
                + "  C_CUSTKEY     INTEGER NOT NULL,\n"
                + "  C_NAME        VARCHAR(25) NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(C_CUSTKEY, C_NAME)\n"
                + "DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")");
    }

    // test the num of source and target table is same
    @Test
    public void testGenerateMapping1() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem,\n"
                        + "  customer\n"
                        + "WHERE\n"
                        + "  c_custkey = o_custkey\n"
                        + "  AND l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  customer,\n"
                        + "  orders,\n"
                        + "  lineitem\n"
                        + "WHERE\n"
                        + "  c_custkey = o_custkey\n"
                        + "  AND l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(1, generateRelationMapping.size());

        // expected slot mapping
        BiMap<ExprId, ExprId> expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(3), new ExprId(5));
        expectedSlotMapping.put(new ExprId(4), new ExprId(6));
        expectedSlotMapping.put(new ExprId(5), new ExprId(0));
        expectedSlotMapping.put(new ExprId(6), new ExprId(1));
        // expected relation mapping
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        assertRelationMapping(generateRelationMapping.get(0), expectedRelationMapping, expectedSlotMapping);
    }

    // test the num of source table is less than target table
    @Test
    public void testGenerateMapping2() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  customer\n"
                        + "WHERE\n"
                        + "  c_custkey = o_custkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  customer,\n"
                        + "  orders,\n"
                        + "  lineitem\n"
                        + "WHERE\n"
                        + "  c_custkey = o_custkey\n"
                        + "  AND l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(1, generateRelationMapping.size());

        // expected slot mapping
        BiMap<ExprId, ExprId> expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(3), new ExprId(0));
        expectedSlotMapping.put(new ExprId(4), new ExprId(1));
        // expected relation mapping
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        assertRelationMapping(generateRelationMapping.get(0), expectedRelationMapping, expectedSlotMapping);
    }

    // test the num of source table is more than target table
    @Test
    public void testGenerateMapping3() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem,\n"
                        + "  customer\n"
                        + "WHERE\n"
                        + "  c_custkey = o_custkey\n"
                        + "  AND l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  customer,\n"
                        + "  orders\n"
                        + "WHERE\n"
                        + "  c_custkey = o_custkey\n")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(1, generateRelationMapping.size());

        // expected slot mapping
        BiMap<ExprId, ExprId> expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(5), new ExprId(0));
        expectedSlotMapping.put(new ExprId(6), new ExprId(1));
        // expected relation mapping
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        assertRelationMapping(generateRelationMapping.get(0), expectedRelationMapping, expectedSlotMapping);
    }

    // test table of source query is repeated
    @Test
    public void testGenerateMapping4() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.* "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem l1,\n"
                        + "  lineitem l2\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey\n"
                        + "  AND l1.l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT * "
                        + "FROM\n"
                        + "  lineitem,\n"
                        + "  orders\n"
                        + "WHERE\n"
                        + " l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(2, generateRelationMapping.size());

        // expected slot mapping
        BiMap<ExprId, ExprId> expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(3), new ExprId(0));
        expectedSlotMapping.put(new ExprId(4), new ExprId(1));
        // expected relation mapping
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        assertRelationMapping(generateRelationMapping.get(0), expectedRelationMapping, expectedSlotMapping);

        // expected slot mapping
        expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(5), new ExprId(0));
        expectedSlotMapping.put(new ExprId(6), new ExprId(1));
        // expected relation mapping
        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        assertRelationMapping(generateRelationMapping.get(1), expectedRelationMapping, expectedSlotMapping);
    }

    @Test
    public void testGenerateMapping5() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.* "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem l1,\n"
                        + "  lineitem l2\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey\n"
                        + "  AND l1.l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.* "
                        + "FROM\n"
                        + "  lineitem l1,\n"
                        + "  orders,\n"
                        + "  lineitem l2\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey\n"
                        + " AND l2.l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(2, generateRelationMapping.size());

        // expected slot mapping
        BiMap<ExprId, ExprId> expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(3), new ExprId(0));
        expectedSlotMapping.put(new ExprId(4), new ExprId(1));
        expectedSlotMapping.put(new ExprId(5), new ExprId(5));
        expectedSlotMapping.put(new ExprId(6), new ExprId(6));
        // expected relation mapping
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        assertRelationMapping(generateRelationMapping.get(1), expectedRelationMapping, expectedSlotMapping);

        // expected slot mapping
        expectedSlotMapping = HashBiMap.create();
        expectedSlotMapping.put(new ExprId(0), new ExprId(2));
        expectedSlotMapping.put(new ExprId(1), new ExprId(3));
        expectedSlotMapping.put(new ExprId(2), new ExprId(4));
        expectedSlotMapping.put(new ExprId(3), new ExprId(5));
        expectedSlotMapping.put(new ExprId(4), new ExprId(6));
        expectedSlotMapping.put(new ExprId(5), new ExprId(0));
        expectedSlotMapping.put(new ExprId(6), new ExprId(1));
        // expected relation mapping
        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        assertRelationMapping(generateRelationMapping.get(0), expectedRelationMapping, expectedSlotMapping);
    }

    // Test more than two tables, and the same table num in source plan is equals to target plan
    @Test
    public void testGenerateMapping6() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.l_orderkey, l2.l_orderkey, l3.l_orderkey "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem l1,\n"
                        + "  lineitem l2,\n"
                        + "  lineitem l3\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey and l1.l_orderkey = l3.l_orderkey\n"
                        + "  AND l1.l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.l_orderkey, l2.l_orderkey, l3.l_orderkey "
                        + "FROM\n"
                        + "  lineitem l1,\n"
                        + "  orders,\n"
                        + "  lineitem l2,\n"
                        + "  lineitem l3\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey and l1.l_orderkey = l3.l_orderkey\n"
                        + " AND l2.l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(6, generateRelationMapping.size());

        // expected table relation mapping is as following
        // (1, 3), (2, 2), (3, 0), (0, 1)
        // (1, 0), (2, 3), (3, 2), (0, 1)
        // (1, 2), (2, 3), (3, 0), (0, 1)
        // (1, 0), (2, 2), (3, 3), (0, 1)
        // (1, 2), (2, 0), (3, 3), (0, 1)
        // (1, 3), (2, 0), (3, 2), (0, 1)
        Set<BiMap<RelationId, RelationId>> expectedRelationMappingSet = new HashSet<>();
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(3));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        expectedRelationMapping.put(new RelationId(3), new RelationId(0));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(3));
        expectedRelationMapping.put(new RelationId(3), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(2), new RelationId(3));
        expectedRelationMapping.put(new RelationId(3), new RelationId(0));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        expectedRelationMapping.put(new RelationId(3), new RelationId(3));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        expectedRelationMapping.put(new RelationId(3), new RelationId(3));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(3));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        expectedRelationMapping.put(new RelationId(3), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        assertRelationMapping(new HashSet<>(generateRelationMapping), expectedRelationMappingSet);
    }

    // Test more than two tables, and the same table num in source plan is less then the num of target plan
    @Test
    public void testGenerateMapping7() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.l_orderkey, l3.l_orderkey "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem l1,\n"
                        + "  lineitem l3\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l3.l_orderkey\n"
                        + "  AND l1.l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.l_orderkey, l2.l_orderkey, l3.l_orderkey "
                        + "FROM\n"
                        + "  lineitem l1,\n"
                        + "  orders,\n"
                        + "  lineitem l2,\n"
                        + "  lineitem l3\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey and l1.l_orderkey = l3.l_orderkey\n"
                        + " AND l2.l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(6, generateRelationMapping.size());

        // expected table relation mapping is as following
        // (1, 2), (2, 0), (0, 1)
        // (1, 2), (2, 3), (0, 1)
        // (1, 0), (2, 2), (0, 1)
        // (1, 0), (2, 3), (0, 1)
        // (1, 3), (2, 0), (0, 1)
        // (1, 3), (2, 2), (0, 1)
        Set<BiMap<RelationId, RelationId>> expectedRelationMappingSet = new HashSet<>();
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(2), new RelationId(3));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(3));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(3));
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(3));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        assertRelationMapping(new HashSet<>(generateRelationMapping), expectedRelationMappingSet);
    }

    // Test more than two tables, and the same table num in source plan is more then the num of target plan
    @Test
    public void testGenerateMapping8() {
        Plan sourcePlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.l_orderkey, l2.l_orderkey, l3.l_orderkey "
                        + "FROM\n"
                        + "  orders,\n"
                        + "  lineitem l1,\n"
                        + "  lineitem l2,\n"
                        + "  lineitem l3\n"
                        + "WHERE\n"
                        + "  l1.l_orderkey = l2.l_orderkey and l1.l_orderkey = l3.l_orderkey\n"
                        + "  AND l1.l_orderkey = o_orderkey")
                .getPlan();

        Plan targetPlan = PlanChecker.from(connectContext)
                .analyze("SELECT orders.*, l1.l_orderkey, l3.l_orderkey "
                        + "FROM\n"
                        + "  lineitem l1,\n"
                        + "  orders,\n"
                        + "  lineitem l3\n"
                        + "WHERE\n"
                        + " l1.l_orderkey = l3.l_orderkey\n"
                        + " AND l3.l_orderkey = o_orderkey")
                .getPlan();
        List<CatalogRelation> sourceRelations = new ArrayList<>();
        sourcePlan.accept(RelationCollector.INSTANCE, sourceRelations);

        List<CatalogRelation> targetRelations = new ArrayList<>();
        targetPlan.accept(RelationCollector.INSTANCE, targetRelations);

        List<RelationMapping> generateRelationMapping = RelationMapping.generate(sourceRelations, targetRelations);
        Assertions.assertNotNull(generateRelationMapping);
        Assertions.assertEquals(6, generateRelationMapping.size());

        // expected table relation mapping is as following
        // (1, 0), (2, 2), (0, 1)
        // (1, 0), (3, 2), (0, 1)
        // (2, 0), (1, 2), (0, 1)
        // (3, 0), (2, 2), (0, 1)
        // (2, 0), (3, 2), (0, 1)
        // (3, 0), (1, 2), (0, 1)
        Set<BiMap<RelationId, RelationId>> expectedRelationMappingSet = new HashSet<>();
        BiMap<RelationId, RelationId> expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(1), new RelationId(0));
        expectedRelationMapping.put(new RelationId(3), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(3), new RelationId(0));
        expectedRelationMapping.put(new RelationId(2), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(2), new RelationId(0));
        expectedRelationMapping.put(new RelationId(3), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        expectedRelationMapping = HashBiMap.create();
        expectedRelationMapping.put(new RelationId(3), new RelationId(0));
        expectedRelationMapping.put(new RelationId(1), new RelationId(2));
        expectedRelationMapping.put(new RelationId(0), new RelationId(1));
        expectedRelationMappingSet.add(expectedRelationMapping);

        assertRelationMapping(new HashSet<>(generateRelationMapping), expectedRelationMappingSet);
    }

    private void assertRelationMapping(RelationMapping relationMapping,
            BiMap<RelationId, RelationId> expectRelationMapping,
            BiMap<ExprId, ExprId> expectSlotMapping) {
        // check relation mapping
        BiMap<RelationId, RelationId> generatedRelationMapping = HashBiMap.create();
        relationMapping.getMappedRelationMap().forEach((key, value) ->
                generatedRelationMapping.put(key.getRelationId(), value.getRelationId()));
        Assertions.assertEquals(generatedRelationMapping, expectRelationMapping);

        // Generate slot mapping from relationMapping and check
        SlotMapping slotMapping = SlotMapping.generate(relationMapping);
        Assertions.assertNotNull(slotMapping);
        BiMap<ExprId, ExprId> generatedSlotMapping = HashBiMap.create();
        slotMapping.getRelationSlotMap().forEach((key, value) ->
                generatedSlotMapping.put(key.getExprId(), value.getExprId())
        );
        Assertions.assertEquals(generatedSlotMapping, expectSlotMapping);
    }

    private void assertRelationMapping(Set<RelationMapping> relationMapping,
            Set<BiMap<RelationId, RelationId>> expectRelationMapping) {
        // check relation mapping if equals or not
        Set<BiMap<RelationId, RelationId>> relationMappingSet = new HashSet<>();
        relationMapping.forEach(mapping -> {
                    BiMap<RelationId, RelationId> generatedRelationMapping = HashBiMap.create();
                    mapping.getMappedRelationMap().forEach((key, value) ->
                            generatedRelationMapping.put(key.getRelationId(), value.getRelationId()));
                    relationMappingSet.add(generatedRelationMapping);
                }
        );
        Assertions.assertEquals(relationMappingSet, expectRelationMapping);
    }

    protected static class RelationCollector extends DefaultPlanVisitor<Void, List<CatalogRelation>> {

        public static final RelationCollector INSTANCE = new RelationCollector();

        @Override
        public Void visit(Plan plan, List<CatalogRelation> catalogRelations) {
            if (plan instanceof CatalogRelation) {
                catalogRelations.add((CatalogRelation) plan);
            }
            return super.visit(plan, catalogRelations);
        }
    }
}
