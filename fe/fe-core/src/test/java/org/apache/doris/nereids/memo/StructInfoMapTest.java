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

package org.apache.doris.nereids.memo;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class StructInfoMapTest extends SqlTestBase {
    @Test
    void testCollectStructInfosByMvBaseTableId() throws Exception {
        try {
            CascadesContext withoutNestRewriteContext = prepareJoinRewriteContext(false);
            Set<Set<String>> withoutNestRewriteRelationNames = collectStructInfos(
                    withoutNestRewriteContext, "mv1", "T3").stream()
                    .map(StructInfoMapTest::relationNames)
                    .collect(Collectors.toSet());
            Assertions.assertFalse(withoutNestRewriteRelationNames.contains(ImmutableSet.of("mv1", "T3")));
            Assertions.assertFalse(withoutNestRewriteRelationNames.stream()
                    .anyMatch(relationNames -> relationNames.contains("mv1")));

            CascadesContext withNestRewriteContext = prepareJoinRewriteContext(true);
            Set<Set<String>> baseRelationNames = collectStructInfos(withNestRewriteContext, "T1", "T2", "T3").stream()
                    .map(StructInfoMapTest::relationNames)
                    .collect(Collectors.toSet());
            Assertions.assertTrue(baseRelationNames
                    .contains(ImmutableSet.of("T1", "T2", "T3")));
            Set<Set<String>> nestedMvRelationNames = collectStructInfos(withNestRewriteContext, "mv1", "T3").stream()
                    .map(StructInfoMapTest::relationNames)
                    .collect(Collectors.toSet());
            Assertions.assertTrue(nestedMvRelationNames
                    .contains(ImmutableSet.of("mv1", "T3")));
        } finally {
            dropMvByNereids("drop materialized view if exists mv1");
        }
    }

    @Test
    void testGetStructInfoByExactRelationIdSet() throws Exception {
        try {
            CascadesContext cascadesContext = prepareJoinRewriteContext(true);
            StructInfo nestedMvCandidate = collectStructInfos(cascadesContext, "mv1", "T3").stream()
                    .filter(structInfo -> relationNames(structInfo).equals(ImmutableSet.of("mv1", "T3")))
                    .findFirst()
                    .orElse(null);
            Assertions.assertNotNull(nestedMvCandidate);

            Group root = cascadesContext.getMemo().getRoot();
            Plan rootPlan = root.getLogicalExpressions().get(0).getPlan();
            StructInfo exactStructInfo = root.getStructInfoMap().getStructInfoByRelationIdSet(
                    cascadesContext, nestedMvCandidate.getRelationBitSet(), rootPlan);
            Assertions.assertNotNull(exactStructInfo);
            Assertions.assertEquals(ImmutableSet.of("mv1", "T3"), relationNames(exactStructInfo));

            BitSet unknownRelationIdSet = new BitSet();
            unknownRelationIdSet.set(100000);
            Assertions.assertNull(root.getStructInfoMap().getStructInfoByRelationIdSet(
                    cascadesContext, unknownRelationIdSet, rootPlan));
        } finally {
            dropMvByNereids("drop materialized view if exists mv1");
        }
    }

    private CascadesContext prepareJoinRewriteContext(boolean enableNestRewrite) throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        installValidRelationManager();
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = enableNestRewrite;
        connectContext.getSessionVariable().materializedViewRewriteDurationThresholdMs = 1000000;
        dropMvByNereids("drop materialized view if exists mv1");
        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id from T1 inner join T2 on T1.id = T2.id;");
        mockCandidateMtmv("mv1");
        CascadesContext cascadesContext = createCascadesContext(
                "select T1.id from T1 inner join T2 on T1.id = T2.id inner join T3 on T1.id = T3.id",
                connectContext
        );
        PlanChecker.from(cascadesContext)
                .setIsQuery()
                .analyze()
                .rewrite()
                .optimize();
        return cascadesContext;
    }

    private List<StructInfo> collectStructInfos(CascadesContext cascadesContext, String... tableNames) {
        Group root = cascadesContext.getMemo().getRoot();
        Plan rootPlan = root.getLogicalExpressions().get(0).getPlan();
        return root.getStructInfoMap().collectStructInfosByMvBaseTableId(
                cascadesContext, buildTableIdSet(cascadesContext, tableNames), rootPlan);
    }

    private BitSet buildTableIdSet(CascadesContext cascadesContext, String... tableNames) {
        Database db = (Database) Env.getCurrentEnv().getInternalCatalog().getDbNullable(connectContext.getDatabase());
        BitSet tableIdSet = new BitSet();
        for (String tableName : tableNames) {
            Table table = db.getTableNullable(tableName);
            tableIdSet.set(cascadesContext.getStatementContext().getTableId(table).asInt());
        }
        return tableIdSet;
    }

    private static Set<String> relationNames(StructInfo structInfo) {
        return structInfo.getRelations().stream()
                .map(CatalogRelation::getTable)
                .map(table -> table.getName())
                .collect(Collectors.toSet());
    }
}
