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

package org.apache.doris.nereids;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Test the USE_MV hint resolution between the sync materialization and the async materialization.
 */
public class UseMvHintTest extends TestWithFeService {

    private static final List<String> ASYNC_M_IDENTIFIER = ImmutableList.of("internal", "test", "m");
    private static final List<String> SYNC_M_IDENTIFIER = ImmutableList.of("internal", "test", "t", "m");

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
    }

    private UseMvHint createUseMvHint(List<List<String>> parameters) {
        return new UseMvHint("USE_MV", parameters, true, false, ImmutableList.of());
    }

    private MaterializationContext mockMaterializationContext(List<String> identifier) {
        MaterializationContext context = Mockito.mock(MaterializationContext.class);
        Mockito.when(context.generateMaterializationIdentifier()).thenReturn(identifier);
        return context;
    }

    @Test
    public void testFindUseMvParameter() {
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(ASYNC_M_IDENTIFIER));
        // the async materialization is referenced by its exact identifier
        Assertions.assertEquals(Optional.of(ASYNC_M_IDENTIFIER),
                useMvHint.findUseMvParameter(ASYNC_M_IDENTIFIER));
        // the sync materialization with the same name is referenced by the bare name parameter
        Assertions.assertEquals(Optional.of(ASYNC_M_IDENTIFIER),
                useMvHint.findUseMvParameter(SYNC_M_IDENTIFIER));
        // other materializations are not referenced
        Assertions.assertFalse(useMvHint.findUseMvParameter(ImmutableList.of("internal", "test", "t", "other"))
                .isPresent());

        UseMvHint fullQualifiedHint = createUseMvHint(
                ImmutableList.of(ImmutableList.of("internal", "test", "t", "m")));
        Assertions.assertEquals(Optional.of(SYNC_M_IDENTIFIER),
                fullQualifiedHint.findUseMvParameter(SYNC_M_IDENTIFIER));
        Assertions.assertFalse(fullQualifiedHint.findUseMvParameter(ASYNC_M_IDENTIFIER).isPresent());
    }

    /**
     * A sync index and an async materialized view can have the same name, because the sync index name
     * is table local. The bare name parameter must be kept as the exact identifier of the async
     * materialized view, otherwise the async materialized view is dropped from the hint.
     */
    @Test
    public void testSyncBareNameKeepsAsyncIdentifier() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(ASYNC_M_IDENTIFIER));
        statementContext.addHint(useMvHint);
        connectContext.setStatementContext(statementContext);

        MaterializationContext syncContext = mockMaterializationContext(SYNC_M_IDENTIFIER);
        MaterializationContext asyncContext = mockMaterializationContext(ASYNC_M_IDENTIFIER);
        List<MaterializationContext> hintContexts = InitMaterializationContextHook.INSTANCE
                .getMaterializationContextByHint(ImmutableList.of(syncContext, asyncContext));

        Assertions.assertTrue(hintContexts.contains(syncContext));
        Assertions.assertTrue(hintContexts.contains(asyncContext));
        Map<List<String>, Boolean> useMvTableColumnMap = useMvHint.getUseMvTableColumnMap();
        Assertions.assertTrue(useMvTableColumnMap.containsKey(ASYNC_M_IDENTIFIER));
        Assertions.assertTrue(useMvTableColumnMap.containsKey(SYNC_M_IDENTIFIER));
    }

    private PhysicalOlapScan mockOlapScan(long baseIndexId, long selectedIndexId, String indexName) {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("test");
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getBaseIndexId()).thenReturn(baseIndexId);
        Mockito.when(table.getIndexNameById(Mockito.anyLong())).thenReturn(indexName);
        RelationId relationId = StatementScopeIdGenerator.newRelationId();
        return new PhysicalOlapScan(relationId, table, ImmutableList.of("test", "t"),
                selectedIndexId, new ArrayList<>(), new ArrayList<>(),
                new DistributionSpecHash(new ArrayList<>(), ShuffleType.NATURAL),
                PreAggStatus.on(), ImmutableList.of(), Optional.empty(),
                Mockito.mock(LogicalProperties.class), Optional.empty(), ImmutableList.of(),
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty());
    }

    /**
     * The sync materialization can be selected by the INDEX clause or by the USE_MV hint when the
     * relation is bound, so the hint must be reported as used when the final plan scans it, even if
     * the materialization context did not go through a successful cost based rewrite.
     */
    @Test
    public void testChosenSyncIndexIsReportedAsUsed() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(ImmutableList.of("t", "m")));
        statementContext.addHint(useMvHint);

        NereidsPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 2L, "m"));

        Assertions.assertEquals(Hint.HintStatus.SUCCESS, useMvHint.getStatus());
        Assertions.assertTrue(useMvHint.getUseMvTableColumnMap().get(SYNC_M_IDENTIFIER));
    }

    @Test
    public void testNotChosenSyncIndexIsReportedAsUnUsed() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(ImmutableList.of("t", "m")));
        statementContext.addHint(useMvHint);

        NereidsPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 1L, "t"));

        Assertions.assertEquals(Hint.HintStatus.UNUSED, useMvHint.getStatus());
        Assertions.assertFalse(useMvHint.getUseMvTableColumnMap().get(SYNC_M_IDENTIFIER));
    }
}
