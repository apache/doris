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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.processor.post.materialize.MaterializeProbeVisitor.ProbeContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * visitor to probe the slots which can perform lazy materialization
 */
public class MaterializeProbeVisitor extends DefaultPlanVisitor<Optional<MaterializeSource>, ProbeContext> {
    protected static final Logger LOG = LogManager.getLogger(MaterializeProbeVisitor.class);

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
        public Set<Slot> requiredMaterializedSlots;

        /**
         * constructor
         */
        public ProbeContext(SlotReference slot) {
            this(slot, new HashSet<>());
        }

        public ProbeContext(SlotReference slot, Set<Slot> requiredMaterializedSlots) {
            this.slot = slot;
            this.requiredMaterializedSlots = requiredMaterializedSlots;
        }

    }

    @Override
    public Optional<MaterializeSource> visitPhysicalFilter(PhysicalFilter<? extends Plan> filter,
                                                           ProbeContext context) {
        if (SessionVariable.getTopNLazyMaterializationUsingIndex() && filter.child() instanceof PhysicalOlapScan) {
            // agg table do not support lazy materialize
            OlapTable table = ((PhysicalOlapScan) filter.child()).getTable();
            if (KeysType.AGG_KEYS.equals(table.getKeysType())) {
                return Optional.empty();
            }
            if (filter.getInputSlots().contains(context.slot)) {
                Relation relation = (Relation) filter.child();
                return Optional.of(new MaterializeSource(
                        relation, findRelationOutputSlot(relation, context.slot).orElse(context.slot)));
            } else {
                return filter.child().accept(this, context);
            }
        }
        return this.visit(filter, context);
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

    boolean checkTVFRelationTableSupportedType(PhysicalTVFRelation tvfRelation) {
        Map<String, String> properties = tvfRelation.getFunction().getTVFProperties().getMap();
        String functionName = tvfRelation.getFunction().getName();

        if (functionName.equals("local") || functionName.equals("s3") || functionName.equals("hdfs")) {
            if (properties.containsKey("format")
                    && (properties.get("format").equalsIgnoreCase("parquet")
                    || properties.get("format").equalsIgnoreCase("orc"))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalOlapScan(PhysicalOlapScan scan, ProbeContext context) {
        if (scan.getSelectedIndexId() != scan.getTable().getBaseIndexId()) {
            return Optional.empty();
        }
        // agg table do not support lazy materialize
        OlapTable table = scan.getTable();
        if (KeysType.AGG_KEYS.equals(table.getKeysType())) {
            return Optional.empty();
        }
        if (context.requiredMaterializedSlots.contains(context.slot)) {
            return Optional.empty();
        }
        return Optional.of(
                new MaterializeSource(scan, findRelationOutputSlot(scan, context.slot).orElse(context.slot)));
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalCatalogRelation(
            PhysicalCatalogRelation relation, ProbeContext context) {
        if (checkRelationTableSupportedType(relation)
                    && relation.getOutput().contains(context.slot)
                    && !relation.getOperativeSlots().contains(context.slot)
                    && !context.requiredMaterializedSlots.contains(context.slot)) {
            // lazy materialize slot must be backed by a base column.
            if (context.slot.getOriginalColumn().isPresent()) {
                return Optional.of(new MaterializeSource(
                        relation, findRelationOutputSlot(relation, context.slot).orElse(context.slot)));
            } else {
                context.requiredMaterializedSlots.addAll(relation.getOutputSet());
                LOG.info("lazy materialize {} failed, because its column is empty", context.slot);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<MaterializeSource> visitPhysicalTVFRelation(
            PhysicalTVFRelation tvfRelation, ProbeContext context) {
        if (checkTVFRelationTableSupportedType(tvfRelation) && tvfRelation.getOutput().contains(context.slot)
                && !tvfRelation.getOperativeSlots().contains(context.slot)
                && !context.requiredMaterializedSlots.contains(context.slot)) {
            // lazy materialize slot must be backed by a base column.
            if (context.slot.getOriginalColumn().isPresent()) {
                return Optional.of(new MaterializeSource(
                        tvfRelation, findRelationOutputSlot(tvfRelation, context.slot).orElse(context.slot)));
            } else {
                LOG.info("lazy materialize {} failed, because its column is empty", context.slot);
            }
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
            if (alias.child() instanceof SlotReference && !SessionVariable.getTopNLazyMaterializationUsingIndex()) {
                SlotReference childSlot = (SlotReference) alias.child();
                ProbeContext childContext = new ProbeContext(childSlot, context.requiredMaterializedSlots);
                Optional<MaterializeSource> source = project.child().accept(this, childContext);
                if (!source.isPresent() && !childSlot.getOriginalColumn().isPresent()) {
                    context.requiredMaterializedSlots.addAll(project.getInputSlots());
                }
                return source;
            } else {
                for (Slot inputSlot : projectExpr.getInputSlots()) {
                    context.requiredMaterializedSlots.add(inputSlot);
                    if (inputSlot instanceof SlotReference) {
                        ProbeContext childContext = new ProbeContext((SlotReference) inputSlot,
                                context.requiredMaterializedSlots);
                        project.child().accept(this, childContext);
                    }
                }
                return Optional.empty();
            }
        }
    }

    private Optional<SlotReference> findRelationOutputSlot(Relation relation, SlotReference contextSlot) {
        return relation.getOutput().stream()
                .filter(slot -> slot instanceof SlotReference && slot.equals(contextSlot))
                .map(slot -> (SlotReference) slot)
                .findFirst();
    }

}
