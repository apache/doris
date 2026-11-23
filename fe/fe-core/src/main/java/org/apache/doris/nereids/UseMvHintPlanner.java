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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.ChildOutputPropertyDeriver;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.OrderSpec;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Choose the physical plan which satisfies the USE_MV hint as a hard constraint, and report the
 * materializations which the final plan actually uses.
 * <p>
 * A hinted materialization is required only once: several hinted materializations are usually
 * alternatives of each other, and a materialization which several children of a join can reach does not
 * have to be produced by every one of them. The selection picks the cheapest plan which contains at least
 * one of the reachable materializations and synthesizes the enforcers which the replaced relations need,
 * the rest of the plan selection stays the cost based one of {@link NereidsPlanner}.
 */
public final class UseMvHintPlanner {
    private UseMvHintPlanner() {
    }

    /**
     * Choose the best plan which contains at least one of the USE_MV hinted materializations.
     *
     * @return the chosen plan together with its estimated cost, or null when there is no materialization
     *         to enforce, the caller then keeps its regular plan selection
     */
    public static PlanWithCost chooseBestPlanConsideringRequiredUseMv(Group rootGroup,
            PhysicalProperties physicalProperties, CascadesContext cascadesContext)
            throws AnalysisException {
        StatementContext stmtCtx = cascadesContext.getStatementContext();
        Set<List<String>> requiredUseMvIdentifiers = computeRequiredUseMvIdentifiers(stmtCtx);
        if (requiredUseMvIdentifiers.isEmpty()) {
            return null;
        }
        Map<Group, Set<List<String>>> mvReachableCache = new HashMap<>();
        computeReachableMvIdentifiers(rootGroup, mvReachableCache, requiredUseMvIdentifiers);
        Set<List<String>> rootReachable = mvReachableCache.get(rootGroup);
        // Drop the required MVs which the rewrite can not produce, and mark them as UnUsed,
        // USE_MV is best-effort and should not fail the query.
        requiredUseMvIdentifiers = dropUnreachableUseMvIdentifiers(stmtCtx, requiredUseMvIdentifiers,
                rootReachable == null ? ImmutableSet.of() : rootReachable);
        if (requiredUseMvIdentifiers.isEmpty()) {
            return null;
        }
        PlanWithCost chosenPlan = chooseBestPlanWithRequiredMaterializations(rootGroup,
                physicalProperties, requiredUseMvIdentifiers, mvReachableCache, cascadesContext);
        if (chosenPlan == null) {
            // No materialization can be produced together with the required properties, fall back to
            // the cost based plan instead of failing the query.
            markUseMvNotChosen(stmtCtx, requiredUseMvIdentifiers);
            return chooseBestPlanWithCost(rootGroup, physicalProperties, cascadesContext);
        }
        return enforceOutputProperties(chosenPlan, physicalProperties, rootGroup, cascadesContext);
    }

    /**
     * Choose the best physical plan considering USE_MV hint as a hard constraint.
     * If USE_MV hint is present with rewrite-success MVs, the returned plan must
     * contain those MVs. Otherwise, falls back to standard cost-based selection.
     *
     * @param rootGroup the root memo group
     * @param physicalProperties required physical properties
     * @param cascadesContext the cascades context
     * @return physical plan respecting USE_MV hard constraint
     */
    public static PhysicalPlan chooseBestPlanConsideringUseMv(Group rootGroup,
            PhysicalProperties physicalProperties, CascadesContext cascadesContext)
            throws AnalysisException {
        PlanWithCost chosenPlan = chooseBestPlanConsideringRequiredUseMv(rootGroup, physicalProperties,
                cascadesContext);
        if (chosenPlan == null) {
            return NereidsPlanner.chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        }
        return chosenPlan.plan;
    }

    /**
     * Compute the set of USE_MV hint qualifiers that successfully rewrote.
     * Marks USE_MV hint parameters that did NOT rewrite-success as UnUsed.
     */
    private static Set<List<String>> computeRequiredUseMvIdentifiers(StatementContext statementContext) {
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (!useMvHint.isPresent()) {
            return ImmutableSet.of();
        }
        Set<List<String>> useMvQualifiers = useMvHint.get().getUseMvTableColumnMap().keySet();
        Set<List<String>> rewrittenSuccessSet = statementContext.getMaterializationRewrittenSuccessSet();
        Set<List<String>> required = new HashSet<>(useMvQualifiers);
        required.retainAll(rewrittenSuccessSet);
        // Mark USE_MV qualifiers that did not rewrite-success as UnUsed
        Set<List<String>> notRewritten = new HashSet<>(useMvQualifiers);
        notRewritten.removeAll(rewrittenSuccessSet);
        for (List<String> qualifier : notRewritten) {
            // Update hint map to mark as not-chosen (false) with failure reason
            useMvHint.get().getUseMvTableColumnMap().put(qualifier, false);
        }
        if (!notRewritten.isEmpty()) {
            // If some USE_MV hints failed to rewrite, set status to UNUSED with reason
            useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
            useMvHint.get().setErrorMessage("MV(s) not rewrite-success after CBO: " + notRewritten);
        }
        return required;
    }

    /**
     * Extract the MV qualifier(s) from a scan plan if it matches a required USE_MV identifier.
     * Returns the set of MV qualifiers this plan represents.
     */
    private static Set<List<String>> getScanMvIdentifiers(Plan plan,
            Set<List<String>> requiredUseMvIdentifiers) {
        Set<List<String>> result = new HashSet<>();
        if (plan instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) plan;
            OlapTable table = scan.getTable();
            if (table.getBaseIndexId() != scan.getSelectedIndexId()) {
                List<String> qualifier = new ArrayList<>();
                qualifier.add(table.getDatabase().getCatalog().getName());
                qualifier.add(table.getDatabase().getFullName());
                qualifier.add(table.getName());
                qualifier.add(table.getIndexNameById(scan.getSelectedIndexId()));
                if (requiredUseMvIdentifiers.contains(qualifier)) {
                    result.add(qualifier);
                }
            }
        }
        if (plan instanceof PhysicalCatalogRelation) {
            PhysicalCatalogRelation rel = (PhysicalCatalogRelation) plan;
            if (rel.getTable() instanceof MTMV) {
                List<String> qualifier = rel.getTable().getFullQualifiers();
                if (requiredUseMvIdentifiers.contains(qualifier)) {
                    result.add(qualifier);
                }
            }
            // Also check for sync MV via OlapScan (PhysicalOlapScan is a PhysicalCatalogRelation)
            if (rel instanceof PhysicalOlapScan) {
                PhysicalOlapScan scan = (PhysicalOlapScan) rel;
                OlapTable table = scan.getTable();
                if (table.getBaseIndexId() != scan.getSelectedIndexId()) {
                    List<String> qualifier = new ArrayList<>();
                    qualifier.add(table.getDatabase().getCatalog().getName());
                    qualifier.add(table.getDatabase().getFullName());
                    qualifier.add(table.getName());
                    qualifier.add(table.getIndexNameById(scan.getSelectedIndexId()));
                    if (requiredUseMvIdentifiers.contains(qualifier)) {
                        result.add(qualifier);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Memoized bottom-up DFS: compute all MV identifiers reachable from this group's subtree
     * through physical GroupExpressions (including enforcers).
     */
    private static void computeReachableMvIdentifiers(Group group,
            Map<Group, Set<List<String>>> cache, Set<List<String>> requiredUseMvIdentifiers) {
        if (cache.containsKey(group)) {
            return;
        }
        Set<List<String>> reachable = new HashSet<>();
        // Put in cache BEFORE recursing to break cycles
        cache.put(group, reachable);

        for (GroupExpression ge : group.getPhysicalExpressions()) {
            Set<List<String>> geReachable = getScanMvIdentifiers(ge.getPlan(), requiredUseMvIdentifiers);
            for (Group child : ge.children()) {
                computeReachableMvIdentifiers(child, cache, requiredUseMvIdentifiers);
                geReachable.addAll(cache.get(child));
            }
            reachable.addAll(geReachable);
        }
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            Set<List<String>> enforcerReachable = new HashSet<>();
            for (Group child : enforcer.children()) {
                computeReachableMvIdentifiers(child, cache, requiredUseMvIdentifiers);
                enforcerReachable.addAll(cache.get(child));
            }
            reachable.addAll(enforcerReachable);
        }
    }

    /**
     * Drop the required USE_MV identifiers which are not reachable in the memo, and mark them
     * as not chosen in the hint. USE_MV is a best-effort hint: when the materialized view can not
     * be produced by the rewrite, for example the query aggregations can not be answered by the
     * materialized view, the query is planned without this hint instead of failing.
     *
     * @return the required USE_MV identifiers which are reachable, or an empty set if all are dropped
     */
    private static Set<List<String>> dropUnreachableUseMvIdentifiers(StatementContext statementContext,
            Set<List<String>> requiredUseMvIdentifiers, Set<List<String>> reachableUseMvIdentifiers) {
        Set<List<String>> reachableRequired = new HashSet<>(requiredUseMvIdentifiers);
        reachableRequired.retainAll(reachableUseMvIdentifiers);
        if (reachableRequired.size() == requiredUseMvIdentifiers.size()) {
            return requiredUseMvIdentifiers;
        }
        Set<List<String>> unreachable = new HashSet<>(requiredUseMvIdentifiers);
        unreachable.removeAll(reachableUseMvIdentifiers);
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (useMvHint.isPresent()) {
            useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
            useMvHint.get().setErrorMessage("MV(s) not found in memo after rewrite: " + unreachable);
            for (List<String> qualifier : unreachable) {
                useMvHint.get().getUseMvTableColumnMap().put(qualifier, false);
            }
        }
        return reachableRequired;
    }

    /**
     * Mark the USE_MV qualifiers which are not chosen in the final plan, so that the hint is reported
     * as UnUsed together with the reason.
     */
    private static void markUseMvNotChosen(StatementContext statementContext,
            Set<List<String>> notChosenQualifiers) {
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (!useMvHint.isPresent()) {
            return;
        }
        useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
        useMvHint.get().setErrorMessage("MV(s) can not be chosen with the required properties: "
                + notChosenQualifiers);
        for (List<String> qualifier : notChosenQualifiers) {
            useMvHint.get().getUseMvTableColumnMap().put(qualifier, false);
        }
    }

    /**
     * Enforce the required output properties on the selected plan, the selected materialization can
     * provide other properties than the costed plan which it replaced.
     */
    private static PlanWithCost enforceOutputProperties(PlanWithCost selectedPlan,
            PhysicalProperties requiredProperties, Group group, CascadesContext cascadesContext) {
        if (selectedPlan.plan instanceof PhysicalSink
                || providesProperties(selectedPlan.plan.getPhysicalProperties(), requiredProperties)) {
            // A sink is the root of the plan and its child is already required to provide the
            // properties, an enforcer above it would be meaningless.
            return selectedPlan;
        }
        return enforceProperties(selectedPlan, requiredProperties, false, group, cascadesContext);
    }

    /**
     * Choose the cost based plan of the group together with its estimated cost.
     */
    private static PlanWithCost chooseBestPlanWithCost(Group rootGroup, PhysicalProperties physicalProperties,
            CascadesContext cascadesContext) throws AnalysisException {
        PhysicalPlan physicalPlan = NereidsPlanner.chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        double planCost = rootGroup.getLowestCostPlan(physicalProperties)
                .map(costAndPlan -> costAndPlan.first.getValue()).orElse(0.0);
        return new PlanWithCost(physicalPlan, planCost);
    }

    /**
     * Choose the cheapest physical plan which contains at least one of the required materializations.
     * <p>
     * At each group, picks the cheapest physical GroupExpression which can produce one of the remaining
     * required materializations, then assigns the materializations which its children still have to produce
     * to a single child each and recurses. A materialization is not required from every child which could
     * reach it, see {@link #assignRequiredMaterializations}.
     *
     * @return the chosen plan together with its estimated cost, or null if no materialization can be
     *         produced together with the required properties
     */
    private static PlanWithCost chooseBestPlanWithRequiredMaterializations(
            Group group, PhysicalProperties properties,
            Set<List<String>> remaining, Map<Group, Set<List<String>>> reachableCache,
            CascadesContext cascadesContext) {
        Set<List<String>> requiredMvs = new HashSet<>(remaining);
        if (requiredMvs.isEmpty()) {
            return chooseBestPlanWithCost(group, properties, cascadesContext);
        }

        PhysicalProperties selectedProperties = properties;
        GroupExpression chosenGE = chooseGroupExpression(group, properties, requiredMvs, reachableCache);
        if (chosenGE == null) {
            // The hinted materialization may be costed only with another property than the required
            // one, for example ANY or its own storage distribution, because the cheapest alternative of
            // the group owns the required property. Select it on an available property and enforce the
            // required one afterwards.
            Pair<GroupExpression, PhysicalProperties> alternative =
                    findAlternativeWithProperty(group, requiredMvs, reachableCache);
            if (alternative == null) {
                return null;
            }
            chosenGE = alternative.first;
            selectedProperties = alternative.second;
        }

        if (group.getEnforcers().containsKey(chosenGE)) {
            return choosePlanWithEnforcer(group, chosenGE, properties, requiredMvs, reachableCache,
                    cascadesContext);
        }

        // The materializations which the chosen alternative produces by itself are not required from the
        // children anymore, the others still are. The alternatives of a group are not interchangeable
        // here: the cheapest one may be the base relation, which produces nothing.
        Set<List<String>> childRequiredMvs = new HashSet<>(requiredMvs);
        childRequiredMvs.removeAll(getScanMvIdentifiers(chosenGE.getPlan(), requiredMvs));

        // Handle enforcer marking (same as chooseBestPlan)
        if (chosenGE.getPlan() instanceof PhysicalDistribute
                && group.getEnforcerSpecs().containsKey(
                        ((PhysicalDistribute<?>) chosenGE.getPlan()).getDistributionSpec())) {
            group.addChosenEnforcerId(chosenGE.getId().asInt());
            group.addChosenEnforcerProperties(selectedProperties);
        } else {
            group.setChosenProperties(selectedProperties);
            group.setChosenGroupExpressionId(chosenGE.getId().asInt());
        }

        // Distribute remaining among children and recurse
        List<PhysicalProperties> inputProps = chosenGE.getInputPropertiesList(selectedProperties);
        List<Set<List<String>>> assignedMvs = assignRequiredMaterializations(childRequiredMvs, chosenGE,
                inputProps, reachableCache);
        List<PlanWithCost> childPlans = Lists.newArrayListWithCapacity(chosenGE.arity());
        double memoChildrenCost = 0.0;
        for (int i = 0; i < chosenGE.arity(); i++) {
            Group child = chosenGE.child(i);
            PlanWithCost childPlan = chooseBestPlanWithRequiredMaterializations(
                    child, inputProps.get(i), assignedMvs.get(i), reachableCache, cascadesContext);
            if (childPlan == null) {
                return null;
            }
            childPlans.add(childPlan);
            memoChildrenCost += child.getLowestCostPlan(inputProps.get(i))
                    .map(costAndPlan -> costAndPlan.first.getValue()).orElse(0.0);
        }

        // The children were chosen by the materialization reachability instead of the cost, so the
        // property contract of the costed group expression can be stale: a child can be replaced by a
        // materialization which is laid out by another relation. Re-derive the actual children outputs
        // and enforce the missing properties again, instead of keeping the costed contract.
        boolean contractHolds = childrenProvideProperties(inputProps, childPlans);
        boolean alignedChildren = requiresAlignedChildren(chosenGE.getPlan());
        List<Plan> planChildren = Lists.newArrayListWithCapacity(childPlans.size());
        double childrenCost = 0.0;
        for (int i = 0; i < childPlans.size(); i++) {
            // Every hash distributed child of a join or of a set operation is realigned, not only the
            // replaced one: the data layout pairing of the costed plan does not hold anymore once one
            // child of such a group expression is replaced by a materialization.
            boolean forceDistribution = alignedChildren && !contractHolds
                    && inputProps.get(i).getDistributionSpec() instanceof DistributionSpecHash;
            PlanWithCost childPlan = enforceChildProperties(chosenGE.child(i), childPlans.get(i),
                    inputProps.get(i), forceDistribution, cascadesContext);
            childPlans.set(i, childPlan);
        }
        if (alignedChildren && !contractHolds) {
            alignChildrenDistributions(chosenGE, inputProps, childPlans, cascadesContext);
        }
        for (PlanWithCost childPlan : childPlans) {
            planChildren.add(childPlan.plan);
            childrenCost += childPlan.cost;
        }

        Plan plan = chosenGE.getPlan().withChildren(planChildren);
        if (!(plan instanceof PhysicalPlan)) {
            throw new AnalysisException("Result plan must be PhysicalPlan");
        }
        plan = plan.withGroupExpression(Optional.of(chosenGE));
        PhysicalProperties outputProperties = contractHolds
                ? chosenGE.getOutputProperties(selectedProperties)
                : deriveOutputProperties(plan, planChildren, cascadesContext);
        PhysicalPlan physicalPlan = ((PhysicalPlan) plan)
                .withPhysicalPropertiesAndStats(outputProperties, group.getStatistics());
        // The node cost is what the memo costed for the group expression without its costed children,
        // the children are charged with the forced alternatives and the synthesized enforcers instead.
        double nodeCost = Math.max(0.0, chosenGE.getCostByProperties(selectedProperties) - memoChildrenCost);
        return new PlanWithCost(physicalPlan, nodeCost + childrenCost);
    }

    /**
     * Choose the cheapest group expression of the group which can produce the required materializations
     * together with the required properties.
     */
    private static GroupExpression chooseGroupExpression(Group group, PhysicalProperties properties,
            Set<List<String>> requiredMvs, Map<Group, Set<List<String>>> reachableCache) {
        List<GroupExpression> candidates = Lists.newArrayList(group.getPhysicalExpressions());
        candidates.addAll(group.getEnforcers().values());
        GroupExpression chosenGE = null;
        double chosenCost = Double.MAX_VALUE;
        for (GroupExpression candidate : candidates) {
            if (!candidate.getLowestCostTable().containsKey(properties)) {
                continue;
            }
            double candidateCost = candidate.getCostByProperties(properties);
            if (candidateCost >= chosenCost) {
                continue;
            }
            if (producesRequiredMv(candidate, requiredMvs, reachableCache)) {
                chosenGE = candidate;
                chosenCost = candidateCost;
            }
        }
        return chosenGE;
    }

    /**
     * Find the cheapest group expression which produces the required materializations, together with the
     * property it is costed with.
     */
    private static Pair<GroupExpression, PhysicalProperties> findAlternativeWithProperty(Group group,
            Set<List<String>> requiredMvs, Map<Group, Set<List<String>>> reachableCache) {
        Pair<GroupExpression, PhysicalProperties> chosenAlternative = null;
        double chosenCost = Double.MAX_VALUE;
        for (GroupExpression candidate : group.getPhysicalExpressions()) {
            if (!producesRequiredMv(candidate, requiredMvs, reachableCache)) {
                continue;
            }
            for (Map.Entry<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> propertyAndCost
                    : candidate.getLowestCostTable().entrySet()) {
                double candidateCost = propertyAndCost.getValue().first.getValue();
                if (candidateCost < chosenCost) {
                    chosenAlternative = Pair.of(candidate, propertyAndCost.getKey());
                    chosenCost = candidateCost;
                }
            }
        }
        return chosenAlternative;
    }

    /**
     * Whether the group expression can produce at least one of the required materializations, either by the
     * relation it reads itself or through one of its children.
     * <p>
     * A group of a hinted relation holds both the alternative which reads the materialization and the one
     * which reads the base relation. The materialization is produced by the chosen alternative only, so a
     * group expression which reaches it through a child must not make the requirement disappear for the
     * whole group: the cheapest alternative of the group is often the base relation, and the
     * materialization would never be chosen then.
     */
    private static boolean producesRequiredMv(GroupExpression groupExpression, Set<List<String>> requiredMvs,
            Map<Group, Set<List<String>>> reachableCache) {
        return !getScanMvIdentifiers(groupExpression.getPlan(), requiredMvs).isEmpty()
                || reaches(groupExpression, requiredMvs, reachableCache);
    }

    /**
     * Whether the children of the group expression can reach at least one of the required
     * materializations.
     */
    private static boolean reaches(GroupExpression groupExpression, Set<List<String>> requiredMvs,
            Map<Group, Set<List<String>>> reachableCache) {
        Set<List<String>> childReachable = new HashSet<>();
        for (Group child : groupExpression.children()) {
            Set<List<String>> reachable = reachableCache.get(child);
            if (reachable != null) {
                childReachable.addAll(reachable);
            }
        }
        childReachable.retainAll(requiredMvs);
        return !childReachable.isEmpty();
    }

    /**
     * Assign every remaining materialization to exactly one child, the cheapest child which can reach it.
     * <p>
     * {@link #reaches} only requires one of the children to reach one of the materializations, so every
     * child which can reach a materialization is able to produce it. Requiring all of them to produce it
     * would make the choice depend on every one of them: a child which can not produce the materialization
     * together with its input properties fails the whole group expression, even when another child already
     * produced it. Only the responsible child is planned for the materialization, the other children are
     * planned by the cost based selection without any constraint.
     * <p>
     * A materialization which no child of the group expression can reach is left out, it can only be
     * produced by another alternative of the group.
     */
    private static List<Set<List<String>>> assignRequiredMaterializations(Set<List<String>> requiredMvs,
            GroupExpression groupExpression, List<PhysicalProperties> inputProps,
            Map<Group, Set<List<String>>> reachableCache) {
        List<Set<List<String>>> assignedMvs = Lists.newArrayListWithCapacity(groupExpression.arity());
        for (int i = 0; i < groupExpression.arity(); i++) {
            assignedMvs.add(new HashSet<>());
        }
        for (List<String> requiredMv : requiredMvs) {
            int responsibleChild = -1;
            double responsibleCost = Double.MAX_VALUE;
            for (int i = 0; i < groupExpression.arity(); i++) {
                Group child = groupExpression.child(i);
                if (!reachableCache.getOrDefault(child, ImmutableSet.of()).contains(requiredMv)) {
                    continue;
                }
                // The children are visited in their order, so the first reachable child stays the
                // responsible one when the children are costed the same.
                double childCost = child.getLowestCostPlan(inputProps.get(i))
                        .map(costAndPlan -> costAndPlan.first.getValue()).orElse(Double.MAX_VALUE);
                if (responsibleChild < 0 || childCost < responsibleCost) {
                    responsibleChild = i;
                    responsibleCost = childCost;
                }
            }
            if (responsibleChild < 0) {
                continue;
            }
            assignedMvs.get(responsibleChild).add(requiredMv);
        }
        return assignedMvs;
    }

    /**
     * Plan the materializations behind an enforcer of the group.
     * <p>
     * An enforcer is shared by all the alternatives of the group and the input property it recorded is
     * the one the cheapest alternative provided, so the forced alternative is planned on its own
     * property and the required properties are then enforced on its actual output.
     */
    private static PlanWithCost choosePlanWithEnforcer(Group group, GroupExpression enforcer,
            PhysicalProperties properties, Set<List<String>> requiredMvs,
            Map<Group, Set<List<String>>> reachableCache, CascadesContext cascadesContext) {
        PlanWithCost childPlan = chooseBestPlanWithRequiredMaterializations(group,
                PhysicalProperties.ANY, requiredMvs, reachableCache, cascadesContext);
        if (childPlan == null) {
            return null;
        }
        group.addChosenEnforcerId(enforcer.getId().asInt());
        group.addChosenEnforcerProperties(properties);
        return enforceProperties(childPlan, properties, false, group, cascadesContext);
    }

    /**
     * Whether the children of the plan must be aligned with each other, which is the case for joins and
     * set operations. The other plans only inherit the properties of their single child.
     */
    private static boolean requiresAlignedChildren(Plan plan) {
        return plan instanceof AbstractPhysicalJoin || plan instanceof PhysicalSetOperation;
    }

    /**
     * Check whether the actual physical properties provide the required ones.
     * <p>
     * {@link DistributionSpecHash#satisfy} and {@link DistributionSpecHash#equals} compare the shuffle
     * type and the shuffle columns only, they ignore the relation, the index and the partitions. A
     * materialization which replaces a relation is laid out by its own table, so it compares equal to
     * the distribution of the replaced relation even though the two are not co-located. The layout
     * identity must be compared explicitly when the property regulation of the cost based selection is
     * by-passed.
     */
    @VisibleForTesting
    static boolean providesProperties(PhysicalProperties actual, PhysicalProperties required) {
        return providesDistribution(actual.getDistributionSpec(), required.getDistributionSpec())
                && actual.getOrderSpec().satisfy(required.getOrderSpec());
    }

    private static boolean providesDistribution(DistributionSpec actual, DistributionSpec required) {
        if (!actual.satisfy(required)) {
            return false;
        }
        if (!(required instanceof DistributionSpecHash) || !(actual instanceof DistributionSpecHash)) {
            return true;
        }
        DistributionSpecHash requiredHash = (DistributionSpecHash) required;
        ShuffleType requiredShuffleType = requiredHash.getShuffleType();
        if (requiredShuffleType != ShuffleType.NATURAL && requiredShuffleType != ShuffleType.STORAGE_BUCKETED) {
            // The required distribution does not identify a storage layout.
            return true;
        }
        DistributionSpecHash actualHash = (DistributionSpecHash) actual;
        return actualHash.getTableId() == requiredHash.getTableId()
                && actualHash.getSelectedIndexId() == requiredHash.getSelectedIndexId()
                && actualHash.getPartitionIds().equals(requiredHash.getPartitionIds());
    }

    /**
     * Check whether the actual distribution delivers the shuffle columns of the required one. Unlike
     * {@link #providesDistribution}, a distribution with another storage layout is accepted, because a
     * single child only propagates its distribution to the group expression, so replacing it with a
     * materialization does not need a data movement as long as the shuffle columns keep matching.
     */
    @VisibleForTesting
    static boolean providesShuffleColumns(DistributionSpec actual, DistributionSpec required) {
        if (!(required instanceof DistributionSpecHash) || !(actual instanceof DistributionSpecHash)) {
            return actual.satisfy(required);
        }
        return actual.satisfy(((DistributionSpecHash) required).withShuffleType(ShuffleType.REQUIRE));
    }

    /**
     * Check whether every forced child provides the properties the memo costed the group expression
     * with.
     */
    private static boolean childrenProvideProperties(List<PhysicalProperties> inputProps,
            List<PlanWithCost> childPlans) {
        for (int i = 0; i < childPlans.size(); i++) {
            if (!providesProperties(childPlans.get(i).plan.getPhysicalProperties(), inputProps.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Enforce the input properties which the group expression was costed with on a forced child.
     *
     * @param forceDistribution whether the distribution is enforced even if the child already delivers
     *         the required shuffle columns
     */
    private static PlanWithCost enforceChildProperties(Group childGroup, PlanWithCost childPlan,
            PhysicalProperties requiredProperties, boolean forceDistribution, CascadesContext cascadesContext) {
        if (!forceDistribution
                && providesProperties(childPlan.plan.getPhysicalProperties(), requiredProperties)) {
            return childPlan;
        }
        return enforceProperties(childPlan, requiredProperties, forceDistribution, childGroup, cascadesContext);
    }

    /**
     * Add the enforcers which deliver the required properties, the same way
     * {@link org.apache.doris.nereids.properties.EnforceMissingPropertiesHelper} does it during the cost
     * based selection. A merge sort reads an already locally sorted input, so the local sort has to be
     * placed below the exchange which gathers the sorted data.
     *
     * @param forceDistribution whether the distribution is enforced even if the child already delivers
     *         the required shuffle columns
     */
    private static PlanWithCost enforceProperties(PlanWithCost childPlan, PhysicalProperties requiredProperties,
            boolean forceDistribution, Group childGroup, CascadesContext cascadesContext) {
        PhysicalProperties actualProperties = childPlan.plan.getPhysicalProperties();
        boolean needDistribution = forceDistribution || !providesShuffleColumns(
                actualProperties.getDistributionSpec(), requiredProperties.getDistributionSpec());
        OrderSpec requiredOrder = requiredProperties.getOrderSpec();
        boolean needOrder = !actualProperties.getOrderSpec().satisfy(requiredOrder);
        if (!needDistribution && !needOrder) {
            return childPlan;
        }
        // A merge sort requires an already locally sorted input, so the local sort is enforced below the
        // exchange when the required distribution is gather.
        boolean mergeSort = needOrder && requiredProperties.getDistributionSpec() instanceof DistributionSpecGather;
        PlanWithCost enforcedPlan = childPlan;
        if (mergeSort) {
            enforcedPlan = addSort(enforcedPlan, requiredOrder, SortPhase.LOCAL_SORT, childGroup, cascadesContext);
        }
        if (needDistribution) {
            enforcedPlan = addExchange(enforcedPlan,
                    toExchangeDistribution(requiredProperties.getDistributionSpec()), childGroup, cascadesContext);
        }
        if (mergeSort) {
            enforcedPlan = addSort(enforcedPlan, requiredOrder, SortPhase.MERGE_SORT, childGroup, cascadesContext);
        } else if (needOrder) {
            enforcedPlan = addSort(enforcedPlan, requiredOrder, SortPhase.LOCAL_SORT, childGroup, cascadesContext);
        }
        return enforcedPlan;
    }

    /**
     * The inputs of a join or of a set operation must stay aligned after one of them was replaced by a
     * materialization.
     * <p>
     * When one input is delivered by a global hash exchange, the translator reads the pairing as a
     * partitioned join, whose inputs must be partitioned by the same global hash. An input which keeps
     * its own storage layout in such a pairing is not partitioned by the join keys, so it is exchanged
     * as well, no matter which property the memo costed for it.
     */
    private static void alignChildrenDistributions(GroupExpression groupExpression,
            List<PhysicalProperties> inputProps, List<PlanWithCost> childPlans,
            CascadesContext cascadesContext) {
        boolean hasGlobalHashExchange = false;
        for (PlanWithCost childPlan : childPlans) {
            DistributionSpec distributionSpec = childPlan.plan.getPhysicalProperties().getDistributionSpec();
            if (!(distributionSpec instanceof DistributionSpecHash)) {
                // A broadcast or a gathered input does not take part in the hash pairing, the translator
                // does not read the pairing as a partitioned join for it.
                return;
            }
            if (isGlobalHashExchange(distributionSpec)) {
                hasGlobalHashExchange = true;
            }
        }
        if (!hasGlobalHashExchange) {
            // Every input still delivers its own storage layout, they are co-located or bucket shuffled
            // with each other, which is a valid pairing.
            return;
        }
        for (int i = 0; i < childPlans.size(); i++) {
            PlanWithCost childPlan = childPlans.get(i);
            DistributionSpec distributionSpec = childPlan.plan.getPhysicalProperties().getDistributionSpec();
            if (isGlobalHashExchange(distributionSpec)) {
                continue;
            }
            childPlans.set(i, addExchange(childPlan, exchangeDistributionOf(inputProps.get(i), distributionSpec),
                    groupExpression.child(i), cascadesContext));
        }
    }

    /**
     * Whether the distribution is delivered by a global hash exchange instead of a storage layout.
     */
    @VisibleForTesting
    static boolean isGlobalHashExchange(DistributionSpec distributionSpec) {
        if (!(distributionSpec instanceof DistributionSpecHash)) {
            return false;
        }
        ShuffleType shuffleType = ((DistributionSpecHash) distributionSpec).getShuffleType();
        return shuffleType != ShuffleType.NATURAL && shuffleType != ShuffleType.STORAGE_BUCKETED;
    }

    /**
     * The distribution of a global hash exchange which aligns a child with its siblings: the shuffle
     * columns are the ones the memo required for it, so that the inputs are partitioned by the same
     * keys.
     */
    private static DistributionSpec exchangeDistributionOf(PhysicalProperties requiredProperties,
            DistributionSpec actualDistribution) {
        DistributionSpec requiredDistribution = requiredProperties.getDistributionSpec();
        if (requiredDistribution instanceof DistributionSpecHash) {
            return ((DistributionSpecHash) requiredDistribution).withShuffleType(ShuffleType.EXECUTION_BUCKETED);
        }
        return ((DistributionSpecHash) actualDistribution).withShuffleType(ShuffleType.EXECUTION_BUCKETED);
    }

    /**
     * The distribution of an exchange which delivers the required distribution. A hash distribution
     * without a shuffle type is delivered by a hash shuffle, the same way
     * {@link org.apache.doris.nereids.properties.EnforceMissingPropertiesHelper} does it.
     */
    @VisibleForTesting
    static DistributionSpec toExchangeDistribution(DistributionSpec requiredDistribution) {
        return requiredDistribution instanceof DistributionSpecHash
                ? ((DistributionSpecHash) requiredDistribution).withShuffleType(ShuffleType.EXECUTION_BUCKETED)
                : requiredDistribution;
    }

    /**
     * Add an exchange which delivers the given distribution.
     */
    private static PlanWithCost addExchange(PlanWithCost childPlan, DistributionSpec exchangeDistribution,
            Group childGroup, CascadesContext cascadesContext) {
        PhysicalDistribute<Plan> exchange = new PhysicalDistribute<>(exchangeDistribution, Optional.empty(),
                childPlan.plan.getLogicalProperties(), new PhysicalProperties(exchangeDistribution),
                childPlan.plan.getStats(), childPlan.plan);
        return new PlanWithCost(exchange,
                childPlan.cost + calculateEnforcerCost(cascadesContext, exchange, childGroup, childPlan.plan));
    }

    /**
     * Add a sort which delivers the required order.
     */
    private static PlanWithCost addSort(PlanWithCost childPlan, OrderSpec requiredOrder, SortPhase sortPhase,
            Group childGroup, CascadesContext cascadesContext) {
        PhysicalProperties childProperties = childPlan.plan.getPhysicalProperties();
        PhysicalQuickSort<Plan> sort = new PhysicalQuickSort<>(requiredOrder.getOrderKeys(), sortPhase,
                Optional.empty(), childPlan.plan.getLogicalProperties(),
                new PhysicalProperties(childProperties.getDistributionSpec(), requiredOrder),
                childPlan.plan.getStats(), childPlan.plan);
        return new PlanWithCost(sort,
                childPlan.cost + calculateEnforcerCost(cascadesContext, sort, childGroup, childPlan.plan));
    }

    /**
     * Estimate the cost of an enforcer which is synthesized outside of the memo, it is charged the same
     * way as the enforcers which the cost based selection adds.
     */
    private static double calculateEnforcerCost(CascadesContext cascadesContext, PhysicalPlan enforcer,
            Group childGroup, PhysicalPlan childPlan) {
        GroupExpression groupExpression = new GroupExpression(enforcer, Lists.newArrayList(childGroup));
        // The enforcer belongs to the group it enforces, its cost is estimated from the statistics of
        // that group.
        groupExpression.setOwnerGroup(childGroup);
        try {
            Cost cost = CostCalculator.calculateCost(cascadesContext.getConnectContext(), groupExpression,
                    Lists.newArrayList(childPlan.getPhysicalProperties()),
                    cascadesContext.getStatementContext().getCostWeight());
            return cost.getValue();
        } finally {
            // The group expression is only created for the estimation, keep the memo untouched.
            childGroup.removeParentExpression(groupExpression);
        }
    }

    /**
     * Re-derive the output properties of a forced plan from its actual children, the same way the
     * physical properties are derived during the cost computation.
     */
    private static PhysicalProperties deriveOutputProperties(Plan plan, List<Plan> planChildren,
            CascadesContext cascadesContext) {
        if (plan instanceof PhysicalDistribute) {
            // An exchange is annotated with the distribution it delivers when it is built, the
            // group expression plan kept in the memo is not.
            return new PhysicalProperties(((PhysicalDistribute<?>) plan).getDistributionSpec());
        }
        List<PhysicalProperties> childrenProperties = Lists.newArrayListWithCapacity(planChildren.size());
        for (Plan child : planChildren) {
            childrenProperties.add(getPhysicalProperties(child));
        }
        return plan.accept(new ChildOutputPropertyDeriver(childrenProperties),
                new PlanContext(cascadesContext.getConnectContext(), plan.getGroupExpression().orElse(null),
                        childrenProperties));
    }

    private static PhysicalProperties getPhysicalProperties(Plan plan) {
        return ((PhysicalPlan) plan).getPhysicalProperties();
    }

    /**
     * After final physical plan is chosen, validate USE_MV hint status
     * by checking which required MVs are actually in the chosen plan.
     */
    public static void finalizeUseMvHintStatus(StatementContext statementContext, PhysicalPlan physicalPlan) {
        Optional<UseMvHint> useMvHint = statementContext.getUseMvHint("USE_MV");
        if (!useMvHint.isPresent()) {
            return;
        }
        // Do not override SYNTAX_ERROR status set during init phase
        if (useMvHint.get().isSyntaxError()) {
            return;
        }
        Map<List<String>, Boolean> useMvMap = useMvHint.get().getUseMvTableColumnMap();
        Set<List<String>> useMvQualifiers = new HashSet<>(useMvMap.keySet());
        // The selected scans are the ground truth of which materializations are used. The rewrite
        // success flag of a materialization context can not be used here: a sync materialization is
        // selected directly by the INDEX clause or by the USE_MV hint when the relation is bound, and
        // it then does not go through a successful cost based rewrite.
        Set<List<String>> chosenQualifiers = collectChosenMvIdentifiers(physicalPlan, useMvQualifiers);

        // Check if the required MVs are actually chosen, USE_MV is satisfied when at least one
        // of the hinted MVs is chosen because several hinted MVs may be alternatives of each other
        boolean anyChosen = false;
        for (List<String> qualifier : useMvQualifiers) {
            boolean chosen = chosenQualifiers.contains(qualifier);
            useMvMap.put(qualifier, chosen);
            anyChosen = anyChosen || chosen;
        }
        if (anyChosen) {
            useMvHint.get().setStatus(Hint.HintStatus.SUCCESS);
        } else if (!useMvQualifiers.isEmpty()) {
            useMvHint.get().setStatus(Hint.HintStatus.UNUSED);
            useMvHint.get().setErrorMessage("MV(s) are not used in the final plan: " + useMvQualifiers);
        }
    }

    /**
     * Collect the identifiers of the materializations which are selected in the physical plan.
     *
     * @param candidates the materialization identifiers to collect, the others are ignored
     */
    private static Set<List<String>> collectChosenMvIdentifiers(PhysicalPlan physicalPlan,
            Set<List<String>> candidates) {
        Set<List<String>> chosen = new HashSet<>();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Void>() {
            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation, Void context) {
                chosen.addAll(getScanMvIdentifiers(physicalRelation, candidates));
                return null;
            }

            @Override
            public Void visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan lazyScan,
                    Void context) {
                chosen.addAll(getScanMvIdentifiers(lazyScan, candidates));
                return null;
            }
        }, null);
        return chosen;
    }

    /**
     * A physical plan together with its estimated cost.
     */
    public static final class PlanWithCost {
        private final PhysicalPlan plan;
        private final double cost;

        PlanWithCost(PhysicalPlan plan, double cost) {
            this.plan = plan;
            this.cost = cost;
        }

        public PhysicalPlan getPlan() {
            return plan;
        }

        public double getCost() {
            return cost;
        }
    }
}
