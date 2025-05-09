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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.processor.post.materialize.MaterializeProbeVisitor.ProbeContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

/**
 * visitor to probe the slots which can perform lazy materialization
 */
public class MaterializeProbeVisitor extends DefaultPlanVisitor<Optional<MaterializeSource>, ProbeContext> {

    private static Set<Class> SUPPORT_RELATION_TYPES = ImmutableSet.of(
            OlapTable.class,
            HiveTable.class,
            IcebergExternalTable.class,
            HMSExternalTable.class
    );

    /**
     * context
     */
    public static class ProbeContext {
        public SlotReference slot;

        /**
         * constructor
         */
        public ProbeContext(SlotReference slot) {
            this.slot = slot;
        }
    }

    @Override
    public Optional<MaterializeSource> visit(Plan plan, ProbeContext context) {
        if (plan.getInputSlots().contains(context.slot)) {
            return Optional.empty();
        }

        Plan next = null;
        for (Plan child : plan.children()) {
            if (child.getOutput().contains(context.slot)) {
                next = child;
                break;
            }
        }
        if (next == null) {
            return Optional.empty();
        } else {
            return next.accept(this, context);
        }
    }

    boolean checkRelationTableSupportedType(PhysicalCatalogRelation relation) {
        if (!SUPPORT_RELATION_TYPES.contains(relation.getTable().getClass())) {
            return false;
        }

        if (relation.getTable() instanceof HMSExternalTable) {
            HMSExternalTable hmsExternalTable = (HMSExternalTable) relation.getTable();
            return (hmsExternalTable.getDlaType() == DLAType.HIVE && hmsExternalTable.supportedHiveTopNLazyTable())
                    || hmsExternalTable.getDlaType() == DLAType.ICEBERG;
        }
        return true;
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalCatalogRelation(
            PhysicalCatalogRelation relation, ProbeContext context) {
        if (checkRelationTableSupportedType(relation)
                    && relation.getOutput().contains(context.slot)
                    && !relation.getOperativeSlots().contains(context.slot)) {
            // lazy materialize slot must be a passive slot
            return Optional.of(new MaterializeSource(relation, context.slot));
        }
        return Optional.empty();
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalLazyMaterialize(
            PhysicalLazyMaterialize<? extends Plan> materialize, ProbeContext context) {
        return materialize.child().accept(this, context);
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalSetOperation(
            PhysicalSetOperation setOperation, ProbeContext context) {
        /*
          union_all could support lazy materialization, but there are efficiency issues in BE.
          And hence, all set operation are not support lazy materialization.
         */
        return Optional.empty();
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalProject(
            PhysicalProject<? extends Plan> project, ProbeContext context) {
        int idx = project.getOutput().indexOf(context.slot);
        if (idx < 0) {
            return Optional.empty();
        }
        NamedExpression projectExpr = project.getProjects().get(idx);
        if (projectExpr instanceof SlotReference) {
            return project.child().accept(this, context);
        } else {
            // projectExpr is alias
            Alias alias = (Alias) projectExpr;
            if (alias.child() instanceof SlotReference) {
                ProbeContext childContext = new ProbeContext((SlotReference) alias.child());
                return project.child().accept(this, childContext);
            } else {
                return Optional.empty();
            }
        }
    }

}
