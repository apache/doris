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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.DistributionMapping;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class LogicalOlapScanToPhysicalOlapScanTest {
    @Test
    void buildDistributionMappingsUsesBaseColumnProvenance() {
        ExprId aliasExprId = new ExprId(1);
        SlotReference aliasSlot = Mockito.mock(SlotReference.class);
        Column aliasColumn = Mockito.mock(Column.class);
        Mockito.when(aliasSlot.getExprId()).thenReturn(aliasExprId);
        Mockito.when(aliasSlot.getOriginalColumn()).thenReturn(Optional.of(aliasColumn));
        Mockito.when(aliasColumn.getName()).thenReturn("alias_d1");
        Mockito.when(aliasColumn.tryGetBaseColumnName()).thenReturn("d1");

        Column distributionColumn = Mockito.mock(Column.class);
        Mockito.when(distributionColumn.getName()).thenReturn("k1");
        HashDistributionInfo distributionInfo = Mockito.mock(HashDistributionInfo.class);
        Mockito.when(distributionInfo.getDistributionColumns())
                .thenReturn(ImmutableList.of(distributionColumn));

        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", ImmutableList.of("d1"), ImmutableList.of("k1"));
        List<DistributionMapping> mappings =
                LogicalOlapScanToPhysicalOlapScan.buildDistributionMappings(
                        distributionInfo, ImmutableList.<Slot>of(aliasSlot), ImmutableList.of(mapping));

        Assertions.assertEquals(1, mappings.size());
        Assertions.assertEquals(ImmutableList.of(aliasExprId), mappings.get(0).getDeterminantExprIds());
        Assertions.assertEquals(ImmutableList.of(0), mappings.get(0).getTargetDistributionIndices());
    }

    @Test
    void buildDistributionMappingsRejectsMissingDeterminantOrTarget() {
        SlotReference slot = Mockito.mock(SlotReference.class);
        Column visibleColumn = Mockito.mock(Column.class);
        Mockito.when(slot.getExprId()).thenReturn(new ExprId(1));
        Mockito.when(slot.getOriginalColumn()).thenReturn(Optional.of(visibleColumn));
        Mockito.when(visibleColumn.tryGetBaseColumnName()).thenReturn("extra_col");

        Column distributionColumn = Mockito.mock(Column.class);
        Mockito.when(distributionColumn.getName()).thenReturn("k1");
        HashDistributionInfo distributionInfo = Mockito.mock(HashDistributionInfo.class);
        Mockito.when(distributionInfo.getDistributionColumns())
                .thenReturn(ImmutableList.of(distributionColumn));

        DistributionMappingConstraint missingDeterminant = new DistributionMappingConstraint(
                "missing_determinant", "mapping_id", ImmutableList.of("d1"), ImmutableList.of("k1"));
        DistributionMappingConstraint missingTarget = new DistributionMappingConstraint(
                "missing_target", "mapping_id", ImmutableList.of("extra_col"), ImmutableList.of("k2"));

        Assertions.assertTrue(LogicalOlapScanToPhysicalOlapScan.buildDistributionMappings(
                distributionInfo,
                ImmutableList.<Slot>of(slot),
                ImmutableList.of(missingDeterminant, missingTarget)).isEmpty());
    }

    @Test
    void mappingScanDisablesSqlResultCache() {
        ExprId determinantExprId = new ExprId(1);
        SlotReference determinantSlot = Mockito.mock(SlotReference.class);
        Column determinantColumn = Mockito.mock(Column.class);
        Mockito.when(determinantSlot.getExprId()).thenReturn(determinantExprId);
        Mockito.when(determinantSlot.getOriginalColumn()).thenReturn(Optional.of(determinantColumn));
        Mockito.when(determinantColumn.tryGetBaseColumnName()).thenReturn("d1");

        Column distributionColumn = Mockito.mock(Column.class);
        Mockito.when(distributionColumn.getName()).thenReturn("k1");
        HashDistributionInfo distributionInfo = Mockito.mock(HashDistributionInfo.class);
        Mockito.when(distributionInfo.getDistributionColumns())
                .thenReturn(ImmutableList.of(distributionColumn));

        OlapTable table = Mockito.mock(OlapTable.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Mockito.when(constraintManager.getDistributionMappingConstraintsForPlanning(table))
                .thenReturn(ImmutableList.of(new DistributionMappingConstraint(
                        "mapping", "mapping_id", ImmutableList.of("d1"), ImmutableList.of("k1"))));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);

        SqlCacheContext sqlCacheContext = Mockito.mock(SqlCacheContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getSqlCacheContext()).thenReturn(Optional.of(sqlCacheContext));
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(sessionVariable.isEnableColocateMappingConstraint()).thenReturn(true);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);

        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedContext.when(ConnectContext::get).thenReturn(connectContext);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            List<DistributionMapping> mappings =
                    LogicalOlapScanToPhysicalOlapScan.buildDistributionMappings(
                            table, distributionInfo, ImmutableList.<Slot>of(determinantSlot));

            Assertions.assertEquals(1, mappings.size());
            Mockito.verify(sqlCacheContext).setHasUnsupportedTables(true);
        }
    }

    @Test
    void randomDistributionWithUnusableMappingFallsBackToRegularPlanning() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new RandomDistributionInfo(4));
        LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of());

        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        Mockito.when(constraintManager.getDistributionMappingConstraintsForPlanning(table))
                .thenReturn(ImmutableList.of());
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(sessionVariable.isEnableColocateMappingConstraint()).thenReturn(true);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);

        try (MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<Utils> mockedUtils = Mockito.mockStatic(Utils.class)) {
            mockedContext.when(ConnectContext::get).thenReturn(connectContext);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Assertions.assertSame(DistributionSpecStorageAny.INSTANCE,
                    LogicalOlapScanToPhysicalOlapScan.convertDistribution(scan));
            Mockito.verify(constraintManager).getDistributionMappingConstraintsForPlanning(table);
        }
    }
}
