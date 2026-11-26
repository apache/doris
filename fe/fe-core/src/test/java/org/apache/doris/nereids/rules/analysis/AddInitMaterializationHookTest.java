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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.exploration.mv.InitConsistentMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

/** Tests materialized view hook registration for DML plans. */
public class AddInitMaterializationHookTest extends TestWithFeService {

    private static final String DATABASE = "add_init_materialization_hook_test";
    private static final String SOURCE_TABLE = "source_table";
    private static final String TARGET_TABLE = "target_table";

    private MTMVRelationManager originalRelationManager;
    private MTMVService mtmvService;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        useDatabase(DATABASE);
        createTable("CREATE TABLE " + SOURCE_TABLE + " (k1 INT) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE " + TARGET_TABLE + " (k1 INT) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        mtmvService = Env.getCurrentEnv().getMtmvService();
        originalRelationManager = mtmvService.getRelationManager();
    }

    @AfterEach
    void restoreRelationManager() {
        Deencapsulation.setField(mtmvService, "relationManager", originalRelationManager);
    }

    @Test
    void dmlRewriteDisabledDoesNotCollectMtmvCandidates() {
        assertCandidateCollection(false);
    }

    @Test
    void dmlRewriteEnabledCollectsMtmvCandidates() {
        assertCandidateCollection(true);
    }

    private void assertCandidateCollection(boolean enableDmlMaterializedViewRewrite) {
        connectContext.getSessionVariable().enableDmlMaterializedViewRewrite = enableDmlMaterializedViewRewrite;

        MTMVRelationManager relationManager = Mockito.spy(originalRelationManager);
        MTMV candidate = Mockito.mock(MTMV.class);
        MTMVRelation relation = Mockito.mock(MTMVRelation.class);
        Mockito.when(candidate.getFullQualifiers()).thenReturn(
                ImmutableList.of("internal", DATABASE, "candidate_mv"));
        Mockito.when(candidate.getRelation()).thenReturn(relation);
        Mockito.when(relation.getBaseTables()).thenReturn(ImmutableSet.of());
        Mockito.doReturn(ImmutableSet.of(candidate)).when(relationManager).getCandidateMTMVs(Mockito.any());
        Deencapsulation.setField(mtmvService, "relationManager", relationManager);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext,
                ((InsertIntoTableCommand) new NereidsParser().parseSingle(
                        "INSERT INTO " + TARGET_TABLE + " SELECT * FROM " + SOURCE_TABLE)).getLogicalQuery());
        cascadesContext.newTableCollector(true).collect();

        Set<PlannerHook> plannerHooks = cascadesContext.getStatementContext().getPlannerHooks();
        boolean hasDmlHook = plannerHooks.contains(InitConsistentMaterializationContextHook.INSTANCE);
        Assertions.assertEquals(enableDmlMaterializedViewRewrite, hasDmlHook);
        Mockito.verify(relationManager, enableDmlMaterializedViewRewrite ? Mockito.times(1) : Mockito.never())
                .getCandidateMTMVs(Mockito.any());
    }
}
