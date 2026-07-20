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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.processor.post.materialize.MaterializationAnalysisResult.NotApplicableReason;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBucketedHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Stateless bottom-up resolver from an output slot to its physical source-column lineage. */
public final class SlotLineageAnalyzer {
    private static final ImmutableSet<Class<?>> SUPPORTED_RELATION_TYPES = ImmutableSet.of(
            OlapTable.class, HiveTable.class, IcebergExternalTable.class, HMSExternalTable.class);

    public MaterializationAnalysisResult<OutputLineage> analyze(Plan plan, SlotReference slot) {
        return analyze(plan, slot, new IdentityHashMap<>());
    }

    /** Resolve all requested outputs with one shared lineage cache for this candidate TopN. */
    public Map<Slot, MaterializationAnalysisResult<OutputLineage>> analyze(
            Plan plan, List<? extends Slot> slots) {
        Map<Plan, Map<SlotReference, MaterializationAnalysisResult<OutputLineage>>> cache =
                new IdentityHashMap<>();
        Map<Plan, Map<SlotReference, Boolean>> containsOutputCache = new IdentityHashMap<>();
        Map<Slot, MaterializationAnalysisResult<OutputLineage>> results = new LinkedHashMap<>();
        for (Slot slot : slots) {
            if (slot instanceof SlotReference) {
                results.put(slot, analyzeFromProducer(
                        plan, (SlotReference) slot, cache, containsOutputCache));
            }
        }
        return results;
    }

    private MaterializationAnalysisResult<OutputLineage> analyze(Plan plan, SlotReference slot,
            Map<Plan, Map<SlotReference, MaterializationAnalysisResult<OutputLineage>>> cache) {
        Map<SlotReference, MaterializationAnalysisResult<OutputLineage>> planCache =
                cache.computeIfAbsent(plan, ignored -> new LinkedHashMap<>());
        MaterializationAnalysisResult<OutputLineage> cached = planCache.get(slot);
        if (cached != null) {
            return cached;
        }
        MaterializationAnalysisResult<OutputLineage> result = analyzeUncached(plan, slot, cache);
        planCache.put(slot, result);
        return result;
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeFromProducer(Plan plan, SlotReference slot,
            Map<Plan, Map<SlotReference, MaterializationAnalysisResult<OutputLineage>>> cache,
            Map<Plan, Map<SlotReference, Boolean>> containsOutputCache) {
        if (plan.getOutput().contains(slot)) {
            return analyze(plan, slot, cache);
        }
        List<Plan> matchingChildren = new ArrayList<>();
        for (Plan child : plan.children()) {
            if (containsOutput(child, slot, containsOutputCache)) {
                matchingChildren.add(child);
            }
        }
        if (matchingChildren.size() != 1) {
            return notApplicable(NotApplicableReason.AMBIGUOUS_LINEAGE,
                    "slot " + slot + " is produced by " + matchingChildren.size()
                            + " subtrees of " + plan.getClass().getSimpleName());
        }
        return analyzeFromProducer(matchingChildren.get(0), slot, cache, containsOutputCache);
    }

    private boolean containsOutput(Plan plan, SlotReference slot,
            Map<Plan, Map<SlotReference, Boolean>> cache) {
        Map<SlotReference, Boolean> planCache = cache.computeIfAbsent(plan, ignored -> new LinkedHashMap<>());
        Boolean cached = planCache.get(slot);
        if (cached != null) {
            return cached;
        }
        boolean contains = plan.getOutput().contains(slot);
        if (!contains) {
            for (Plan child : plan.children()) {
                if (containsOutput(child, slot, cache)) {
                    contains = true;
                    break;
                }
            }
        }
        planCache.put(slot, contains);
        return contains;
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeUncached(Plan plan, SlotReference slot,
            Map<Plan, Map<SlotReference, MaterializationAnalysisResult<OutputLineage>>> cache) {
        if (isBarrier(plan)) {
            return notApplicable(NotApplicableReason.UNSUPPORTED_OPERATOR, plan.getClass().getSimpleName());
        }
        if (plan instanceof PhysicalLazyMaterialize) {
            return analyze(plan.child(0), slot, cache);
        }
        if (plan instanceof PhysicalProject) {
            return analyzeProject((PhysicalProject<?>) plan, slot, cache);
        }
        if (plan instanceof PhysicalFilter) {
            MaterializationAnalysisResult<OutputLineage> filterResult = analyzeIndexFilter(
                    (PhysicalFilter<?>) plan, slot, cache);
            if (filterResult != null) {
                return filterResult;
            }
        }
        if (plan instanceof PhysicalOlapScan) {
            return analyzeOlapScan((PhysicalOlapScan) plan, slot);
        }
        if (plan instanceof PhysicalCatalogRelation) {
            return analyzeCatalogRelation((PhysicalCatalogRelation) plan, slot);
        }
        if (plan instanceof PhysicalTVFRelation) {
            return analyzeTvfRelation((PhysicalTVFRelation) plan, slot);
        }
        List<Plan> matchingChildren = new ArrayList<>();
        for (Plan child : plan.children()) {
            if (child.getOutput().contains(slot)) {
                matchingChildren.add(child);
            }
        }
        if (matchingChildren.size() != 1) {
            return notApplicable(NotApplicableReason.AMBIGUOUS_LINEAGE,
                    "slot " + slot + " matches " + matchingChildren.size() + " children of "
                            + plan.getClass().getSimpleName());
        }
        return analyze(matchingChildren.get(0), slot, cache);
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeProject(
            PhysicalProject<?> project, SlotReference outputSlot,
            Map<Plan, Map<SlotReference, MaterializationAnalysisResult<OutputLineage>>> cache) {
        int index = project.getOutput().indexOf(outputSlot);
        if (index < 0) {
            return notApplicable(NotApplicableReason.AMBIGUOUS_LINEAGE,
                    "project does not produce slot " + outputSlot);
        }
        NamedExpression expression = project.getProjects().get(index);
        if (expression instanceof SlotReference) {
            return analyze(project.child(), (SlotReference) expression, cache);
        }
        if (expression instanceof Alias && ((Alias) expression).child() instanceof SlotReference) {
            SlotReference inputSlot = (SlotReference) ((Alias) expression).child();
            MaterializationAnalysisResult<OutputLineage> childResult = analyze(project.child(), inputSlot, cache);
            if (!childResult.isApplicable() || !childResult.getValue().getSource().isPresent()) {
                return childResult;
            }
            return MaterializationAnalysisResult.applicable(OutputLineage.forwarded(
                    inputSlot, childResult.getValue().getSource().get()));
        }
        return MaterializationAnalysisResult.applicable(OutputLineage.derived(
                ImmutableList.copyOf(expression.getInputSlots())));
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeIndexFilter(
            PhysicalFilter<?> filter, SlotReference slot,
            Map<Plan, Map<SlotReference, MaterializationAnalysisResult<OutputLineage>>> cache) {
        if (!SessionVariable.getTopNLazyMaterializationUsingIndex()
                || !(filter.child() instanceof PhysicalOlapScan)) {
            return null;
        }
        PhysicalOlapScan scan = (PhysicalOlapScan) filter.child();
        if (KeysType.AGG_KEYS.equals(scan.getTable().getKeysType())) {
            return notApplicable(NotApplicableReason.UNSUPPORTED_SOURCE, "aggregate-key table");
        }
        if (filter.getInputSlots().contains(slot)) {
            SlotReference sourceSlot = findRelationOutputSlot(scan, slot);
            return sourceSlot == null
                    ? notApplicable(NotApplicableReason.MISSING_SOURCE_METADATA, slot.toString())
                    : direct(scan, sourceSlot);
        }
        return analyze(scan, slot, cache);
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeOlapScan(
            PhysicalOlapScan scan, SlotReference slot) {
        if (scan.getSelectedIndexId() != scan.getTable().getBaseIndexId()) {
            return notApplicable(NotApplicableReason.UNSUPPORTED_INDEX, scan.getTable().getName());
        }
        if (KeysType.AGG_KEYS.equals(scan.getTable().getKeysType())) {
            return notApplicable(NotApplicableReason.UNSUPPORTED_SOURCE, "aggregate-key table");
        }
        SlotReference sourceSlot = findRelationOutputSlot(scan, slot);
        return sourceSlot == null
                ? notApplicable(NotApplicableReason.MISSING_SOURCE_METADATA, slot.toString())
                : direct(scan, sourceSlot);
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeCatalogRelation(
            PhysicalCatalogRelation relation, SlotReference slot) {
        if (!isSupportedCatalogRelation(relation) || !relation.getOutput().contains(slot)
                || relation.getOperativeSlots().contains(slot)) {
            return notApplicable(NotApplicableReason.UNSUPPORTED_SOURCE, relation.toString());
        }
        SlotReference sourceSlot = findRelationOutputSlot(relation, slot);
        return sourceSlot == null || !sourceSlot.getOriginalColumn().isPresent()
                ? notApplicable(NotApplicableReason.MISSING_SOURCE_METADATA, slot.toString())
                : direct(relation, sourceSlot);
    }

    private MaterializationAnalysisResult<OutputLineage> analyzeTvfRelation(
            PhysicalTVFRelation relation, SlotReference slot) {
        if (!isSupportedTvfRelation(relation) || !relation.getOutput().contains(slot)
                || relation.getOperativeSlots().contains(slot)) {
            return notApplicable(NotApplicableReason.UNSUPPORTED_SOURCE, relation.toString());
        }
        SlotReference sourceSlot = findRelationOutputSlot(relation, slot);
        return sourceSlot == null || !sourceSlot.getOriginalColumn().isPresent()
                ? notApplicable(NotApplicableReason.MISSING_SOURCE_METADATA, slot.toString())
                : direct(relation, sourceSlot);
    }

    private MaterializationAnalysisResult<OutputLineage> direct(Relation relation, SlotReference sourceSlot) {
        if (relation instanceof PhysicalOlapScan
                && !((PhysicalOlapScan) relation).getTable().getEnableLightSchemaChange()) {
            // BE cannot add GLOBAL_ROWID_COL to a tablet schema without column unique IDs.
            return notApplicable(NotApplicableReason.UNSUPPORTED_SOURCE, "light schema change disabled");
        }
        return sourceSlot.getOriginalColumn().isPresent()
                ? MaterializationAnalysisResult.applicable(
                        OutputLineage.direct(new MaterializeSource(relation, sourceSlot)))
                : notApplicable(NotApplicableReason.MISSING_SOURCE_METADATA, sourceSlot.toString());
    }

    private SlotReference findRelationOutputSlot(Relation relation, SlotReference requestedSlot) {
        for (Slot output : relation.getOutput()) {
            if (output instanceof SlotReference && output.equals(requestedSlot)) {
                return (SlotReference) output;
            }
        }
        return null;
    }

    private boolean isSupportedCatalogRelation(PhysicalCatalogRelation relation) {
        if (!SUPPORTED_RELATION_TYPES.contains(relation.getTable().getClass())) {
            return false;
        }
        if (relation.getTable() instanceof HMSExternalTable) {
            HMSExternalTable table = (HMSExternalTable) relation.getTable();
            return table.getDlaType() == HMSExternalTable.DLAType.ICEBERG
                    || table.getDlaType() == HMSExternalTable.DLAType.HIVE && table.supportedHiveTopNLazyTable();
        }
        return true;
    }

    private boolean isSupportedTvfRelation(PhysicalTVFRelation relation) {
        Map<String, String> properties = relation.getFunction().getTVFProperties().getMap();
        String functionName = relation.getFunction().getName();
        if (!functionName.equals("local") && !functionName.equals("s3") && !functionName.equals("hdfs")) {
            return false;
        }
        String format = properties.get("format");
        return format != null && (format.equalsIgnoreCase("parquet") || format.equalsIgnoreCase("orc"));
    }

    private boolean isBarrier(Plan plan) {
        return plan instanceof PhysicalHashAggregate
                || plan instanceof PhysicalBucketedHashAggregate
                || plan instanceof PhysicalCTEConsumer
                || plan instanceof PhysicalCTEProducer
                || plan instanceof PhysicalRepeat
                || plan instanceof PhysicalSetOperation
                || plan instanceof PhysicalOneRowRelation;
    }

    private MaterializationAnalysisResult<OutputLineage> notApplicable(
            NotApplicableReason reason, String detail) {
        return MaterializationAnalysisResult.notApplicable(reason, detail);
    }
}
