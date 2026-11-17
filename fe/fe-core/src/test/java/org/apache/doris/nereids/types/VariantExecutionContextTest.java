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

package org.apache.doris.nereids.types;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.Config;
import org.apache.doris.common.VariantExecutionContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContextUtil;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.thrift.TAlterTabletReqV2;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class VariantExecutionContextTest {
    @Test
    public void recursiveCteComparesEffectiveExecutionVersions() {
        boolean previous = Config.enable_variant_v2;
        ConnectContext previousContext = ConnectContext.get();
        Config.enable_variant_v2 = false;
        ConnectContext.remove();
        try {
            VariantType legacy = new VariantType(3);
            VariantType v2 = legacy.withComputeV2(true);
            int originalHash = legacy.hashCode();
            Assertions.assertFalse(legacy.equalsForRecursiveCte(v2));
            Assertions.assertFalse(v2.equalsForRecursiveCte(legacy));
            try (VariantExecutionContext.Scope ignored = VariantExecutionContext.forceV2()) {
                Assertions.assertTrue(legacy.equalsForRecursiveCte(v2));
                Assertions.assertTrue(v2.equalsForRecursiveCte(legacy));
                Assertions.assertEquals(originalHash, legacy.hashCode());
                Assertions.assertNotEquals(legacy, v2);
            }
            Assertions.assertFalse(legacy.equalsForRecursiveCte(v2));
            Config.enable_variant_v2 = true;
            Assertions.assertTrue(legacy.equalsForRecursiveCte(v2));
            Config.enable_variant_v2 = false;
            ConnectContext ctx = new ConnectContext();
            ctx.setStatementContext(new StatementContext(ctx, null));
            ctx.getStatementContext().requireVariantV2Execution();
            ctx.setThreadLocalInfo();
            Assertions.assertTrue(legacy.equalsForRecursiveCte(v2));
        } finally {
            Config.enable_variant_v2 = previous;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void statementAndTaskPoliciesDoNotLeakAcrossStatementsOrThreads() throws Exception {
        boolean previous = Config.enable_variant_v2;
        ConnectContext previousContext = ConnectContext.get();
        Config.enable_variant_v2 = false;
        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
            ctx.setStatementContext(new StatementContext(ctx, null));
            Assertions.assertFalse(VariantType.INSTANCE.isExecutionV2());
            ctx.getStatementContext().requireVariantV2Execution();
            Assertions.assertTrue(VariantType.INSTANCE.isExecutionV2());
            Assertions.assertTrue(((org.apache.doris.catalog.VariantType) VariantType.INSTANCE
                    .toCatalogDataType()).toThrift().getTypes().get(0).scalar_type.isVariantIsV2());
            Assertions.assertFalse(Config.enable_variant_v2);

            AtomicBoolean otherThreadUsesV2 = new AtomicBoolean(true);
            Thread thread = new Thread(() -> otherThreadUsesV2.set(VariantExecutionContext.useV2()));
            thread.start();
            thread.join();
            Assertions.assertFalse(otherThreadUsesV2.get());

            ctx.setStatementContext(new StatementContext(ctx, null));
            Assertions.assertFalse(VariantType.INSTANCE.isExecutionV2());
            ctx.setForceVariantV2Execution(true);
            Assertions.assertTrue(VariantExecutionContext.useV2());
            ConnectContext.remove();
            Assertions.assertFalse(VariantExecutionContext.useV2());
        } finally {
            Config.enable_variant_v2 = previous;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void descriptorSerializationScopeSurvivesContextSwitchAndRestoresOnFailure() {
        boolean previous = Config.enable_variant_v2;
        ConnectContext previousContext = ConnectContext.get();
        Config.enable_variant_v2 = false;
        ConnectContext.remove();
        try {
            Assertions.assertThrows(IllegalStateException.class, () -> {
                try (VariantExecutionContext.Scope ignored = VariantExecutionContext.forceV2()) {
                    ConnectContext ctx = new ConnectContext();
                    ctx.setThreadLocalInfo();
                    Assertions.assertTrue(VariantExecutionContext.useV2());
                    try (VariantExecutionContext.Scope nested = VariantExecutionContext.forceV2()) {
                        Assertions.assertTrue(VariantExecutionContext.useV2());
                    }
                    Assertions.assertTrue(VariantExecutionContext.useV2());
                    throw new IllegalStateException("task failure");
                }
            });
            Assertions.assertFalse(VariantExecutionContext.useV2());
            Assertions.assertFalse(VariantType.INSTANCE.isComputeV2());
        } finally {
            Config.enable_variant_v2 = previous;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void alterTaskDescriptorsAndExpressionsAreV2WithoutAConnectContext() {
        boolean previous = Config.enable_variant_v2;
        ConnectContext previousContext = ConnectContext.get();
        Config.enable_variant_v2 = false;
        ConnectContext.remove();
        try {
            DescriptorTable descriptors = new DescriptorTable();
            TupleDescriptor tuple = descriptors.createTupleDescriptor();
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple);
            slot.setType(new org.apache.doris.catalog.VariantType());
            slot.setColumn(new Column("v", slot.getType()));
            slot.setIsNullable(true);
            for (AlterJobV2.JobType kind : new AlterJobV2.JobType[] {
                    AlterJobV2.JobType.ROLLUP,
                    AlterJobV2.JobType.SCHEMA_CHANGE}) {
                AlterReplicaTask task = new AlterReplicaTask(
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, kind,
                        Collections.singletonMap("v", new SlotRef(slot)),
                        descriptors, Collections.singletonList(slot.getColumn()),
                        new HashMap<>(), null, 0, "");
                TAlterTabletReqV2 request = task.toThrift();
                Assertions.assertTrue(request.getDescTbl().getSlotDescriptors().get(0)
                        .getSlotType().getTypes().get(0).scalar_type.isVariantIsV2());
                Assertions.assertTrue(request.getMaterializedViewParams().get(0).getMvExpr()
                        .getNodes().get(0).getType().getTypes().get(0).scalar_type.isVariantIsV2());
                Assertions.assertFalse(VariantExecutionContext.useV2());
            }
            Assertions.assertFalse(slot.getType().toThrift().getTypes().get(0).scalar_type.isVariantIsV2());
        } finally {
            Config.enable_variant_v2 = previous;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void v1QueriesCannotUseV2OrUnknownVariantMaterializations() {
        Plan variantPlan =
                new LogicalOneRowRelation(
                        new RelationId(1), Collections.singletonList(
                                new Alias(
                                        new Cast(
                                                new StringLiteral("{}"),
                                                VariantType.INSTANCE), "v")));
        Map<String, String> v2 = Collections.singletonMap(
                ConnectContextUtil.MV_VARIANT_EXECUTION_VERSION, "2");
        Assertions.assertFalse(org.apache.doris.mtmv.MTMVPlanUtil.isVariantExecutionCompatible(
                variantPlan, v2, false));
        Assertions.assertTrue(org.apache.doris.mtmv.MTMVPlanUtil.isVariantExecutionCompatible(
                variantPlan, v2, true));
        Assertions.assertFalse(org.apache.doris.mtmv.MTMVPlanUtil.isVariantExecutionCompatible(
                variantPlan, Collections.emptyMap(), true));
        Plan scalarPlan =
                new LogicalOneRowRelation(
                        new RelationId(2), Collections.singletonList(
                                new Alias(
                                        new IntegerLiteral(1), "i")));
        Assertions.assertTrue(org.apache.doris.mtmv.MTMVPlanUtil.isVariantExecutionCompatible(
                scalarPlan, Collections.emptyMap(), false));
    }

}
