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
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

        UseMvHintPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 2L, "m"));

        Assertions.assertEquals(Hint.HintStatus.SUCCESS, useMvHint.getStatus());
        Assertions.assertTrue(useMvHint.getUseMvTableColumnMap().get(SYNC_M_IDENTIFIER));
    }

    @Test
    public void testNotChosenSyncIndexIsReportedAsUnUsed() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(ImmutableList.of("t", "m")));
        statementContext.addHint(useMvHint);

        UseMvHintPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 1L, "t"));

        Assertions.assertEquals(Hint.HintStatus.UNUSED, useMvHint.getStatus());
        Assertions.assertFalse(useMvHint.getUseMvTableColumnMap().get(SYNC_M_IDENTIFIER));
    }

    /**
     * Every parameter of the hint is registered, the hinted materializations are alternatives of each other
     * and the second one can be the chosen one.
     */
    @Test
    public void testAllHintedMvsAreRegistered() {
        List<String> firstMv = ImmutableList.of("internal", "test", "m1");
        List<String> secondMv = ImmutableList.of("internal", "test", "m2");
        Map<List<String>, Boolean> useMvTableColumnMap = createUseMvHint(
                ImmutableList.of(firstMv, secondMv)).getUseMvTableColumnMap();

        Assertions.assertEquals(2, useMvTableColumnMap.size());
        Assertions.assertTrue(useMvTableColumnMap.containsKey(firstMv));
        Assertions.assertTrue(useMvTableColumnMap.containsKey(secondMv));
    }

    /**
     * Several hinted materializations are alternatives of each other, so the hint is satisfied when only one
     * of them is used, and the other ones are reported as not chosen.
     */
    @Test
    public void testOneOfTheHintedMvsIsEnough() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));
        List<String> chosenMv = ImmutableList.of("internal", "test", "t", "m1");
        List<String> alternativeMv = ImmutableList.of("internal", "test", "t", "m2");
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(chosenMv, alternativeMv));
        statementContext.addHint(useMvHint);

        UseMvHintPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 2L, "m1"));

        Assertions.assertEquals(Hint.HintStatus.SUCCESS, useMvHint.getStatus());
        Assertions.assertTrue(useMvHint.getUseMvTableColumnMap().get(chosenMv));
        Assertions.assertFalse(useMvHint.getUseMvTableColumnMap().get(alternativeMv));
    }

    /**
     * A hint which already reports a syntax error keeps its status, the finalization only decides whether the
     * materializations of a well formed hint are used.
     */
    @Test
    public void testSyntaxErrorHintIsNotOverridden() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));
        UseMvHint useMvHint = createUseMvHint(ImmutableList.of(ASYNC_M_IDENTIFIER));
        statementContext.addHint(useMvHint);
        useMvHint.setStatus(Hint.HintStatus.SYNTAX_ERROR);

        UseMvHintPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 1L, "t"));

        Assertions.assertEquals(Hint.HintStatus.SYNTAX_ERROR, useMvHint.getStatus());
    }

    /**
     * A query without the USE_MV hint is not affected by the finalization.
     */
    @Test
    public void testNoUseMvHintIsIgnored() {
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement("select k from t", 0));

        UseMvHintPlanner.finalizeUseMvHintStatus(statementContext, mockOlapScan(1L, 1L, "t"));
    }

    /**
     * The layout of a materialization is not the layout of the relation it replaces, even though the hash
     * distribution of the two compares equal because the shuffle type and the shuffle columns are the same.
     */
    @Test
    public void testLayoutIdentityIsCompared() {
        ExprId key = StatementScopeIdGenerator.newExprId();
        DistributionSpecHash relationLayout = new DistributionSpecHash(ImmutableList.of(key),
                ShuffleType.NATURAL, 1L, 1L, ImmutableSet.of(10L));
        DistributionSpecHash otherTableLayout = new DistributionSpecHash(ImmutableList.of(key),
                ShuffleType.NATURAL, 2L, 2L, ImmutableSet.of(10L));

        Assertions.assertTrue(UseMvHintPlanner.providesProperties(
                new PhysicalProperties(relationLayout), new PhysicalProperties(relationLayout)));
        Assertions.assertFalse(UseMvHintPlanner.providesProperties(
                new PhysicalProperties(otherTableLayout), new PhysicalProperties(relationLayout)));
    }

    /**
     * An input with another storage layout still delivers the shuffle columns of the requirement, so it can
     * stay in a partitioned join after the other input was replaced by a materialization. Other shuffle
     * columns are not delivered.
     */
    @Test
    public void testShuffleColumnsOfAnotherLayout() {
        ExprId key = StatementScopeIdGenerator.newExprId();
        ExprId otherKey = StatementScopeIdGenerator.newExprId();
        DistributionSpec required = new DistributionSpecHash(ImmutableList.of(key),
                ShuffleType.NATURAL, 1L, 1L, ImmutableSet.of(10L));
        DistributionSpec otherLayout = new DistributionSpecHash(ImmutableList.of(key),
                ShuffleType.NATURAL, 2L, 2L, ImmutableSet.of(10L));
        DistributionSpec otherColumns = new DistributionSpecHash(ImmutableList.of(otherKey),
                ShuffleType.NATURAL, 2L, 2L, ImmutableSet.of(10L));

        Assertions.assertTrue(UseMvHintPlanner.providesShuffleColumns(otherLayout, required));
        Assertions.assertFalse(UseMvHintPlanner.providesShuffleColumns(otherColumns, required));
    }

    /**
     * Only the distributions which are delivered by an exchange take part in the partitioned pairing of a
     * join, the ones which keep the storage layout of a relation are co-located or bucket shuffled.
     */
    @Test
    public void testGlobalHashExchangeDetection() {
        ExprId key = StatementScopeIdGenerator.newExprId();
        Assertions.assertFalse(UseMvHintPlanner.isGlobalHashExchange(
                new DistributionSpecHash(ImmutableList.of(key), ShuffleType.NATURAL)));
        Assertions.assertFalse(UseMvHintPlanner.isGlobalHashExchange(
                new DistributionSpecHash(ImmutableList.of(key), ShuffleType.STORAGE_BUCKETED)));
        Assertions.assertTrue(UseMvHintPlanner.isGlobalHashExchange(
                new DistributionSpecHash(ImmutableList.of(key), ShuffleType.EXECUTION_BUCKETED)));
        Assertions.assertTrue(UseMvHintPlanner.isGlobalHashExchange(
                new DistributionSpecHash(ImmutableList.of(key), ShuffleType.REQUIRE)));
        Assertions.assertFalse(UseMvHintPlanner.isGlobalHashExchange(DistributionSpecGather.INSTANCE));
    }

    /**
     * The synthesized exchange delivers the shuffle columns of the requirement as a hash shuffle, the same way
     * the enforcers of the cost based selection do it. A gather distribution is delivered as it is.
     */
    @Test
    public void testEnforcedExchangeDistribution() {
        ExprId key = StatementScopeIdGenerator.newExprId();
        DistributionSpec exchange = UseMvHintPlanner.toExchangeDistribution(
                new DistributionSpecHash(ImmutableList.of(key), ShuffleType.REQUIRE));

        Assertions.assertEquals(ImmutableList.of(key),
                ((DistributionSpecHash) exchange).getOrderedShuffledColumns());
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED,
                ((DistributionSpecHash) exchange).getShuffleType());
        Assertions.assertSame(DistributionSpecGather.INSTANCE,
                UseMvHintPlanner.toExchangeDistribution(DistributionSpecGather.INSTANCE));
    }
}
