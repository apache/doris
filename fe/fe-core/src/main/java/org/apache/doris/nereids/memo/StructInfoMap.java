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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.MoreFieldsThread;

import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Search StructInfo candidates for a memo group.
 *
 * <p>MV definitions use statement-scope table ids, while memo alternatives use relation ids. This class expands
 * MV table ids to currently visible relation ids before searching candidates.
 */
public class StructInfoMap {
    private final Group ownerGroup;
    // Outer key: relation-id search space expanded from one MV definition.
    // Inner key: exact relation-id set contained by one candidate plan tree.
    private final Map<BitSet, Map<BitSet, StructInfoCandidate>> candidatesByTargetRelationIdSet =
            new LinkedHashMap<>();

    StructInfoMap(Group ownerGroup) {
        this.ownerGroup = ownerGroup;
    }

    /**
     * Get the StructInfo whose exact relation-id set matches the given key in the current owner group.
     */
    public @Nullable StructInfo getStructInfoByRelationIdSet(CascadesContext cascadesContext, BitSet relationIdSet,
            @Nullable Plan originPlan) {
        boolean skipMaterializedViewLeaf = !cascadesContext.getConnectContext().getSessionVariable()
                .isEnableMaterializedViewNestRewrite();
        StructInfoCandidate candidate = getCandidatesByRelationIdSet(relationIdSet, skipMaterializedViewLeaf)
                .get(relationIdSet);
        if (candidate == null) {
            return null;
        }
        return candidate.toStructInfo(originPlan, cascadesContext);
    }

    /**
     * Collect query StructInfo candidates that may match the MV base table ids.
     */
    public List<StructInfo> collectStructInfosByMvBaseTableId(CascadesContext cascadesContext,
            BitSet mvBaseTableIdSet, @Nullable Plan originPlan) {
        BitSet targetRelationIdSet = createTargetRelationIdSet(mvBaseTableIdSet, cascadesContext);
        boolean skipMaterializedViewLeaf = !cascadesContext.getConnectContext().getSessionVariable()
                .isEnableMaterializedViewNestRewrite();
        Map<BitSet, StructInfoCandidate> candidates = getCandidatesByRelationIdSet(
                targetRelationIdSet, skipMaterializedViewLeaf);
        List<StructInfo> structInfos = new ArrayList<>(candidates.size());
        for (StructInfoCandidate candidate : candidates.values()) {
            structInfos.add(candidate.toStructInfo(originPlan, cascadesContext));
        }
        return structInfos;
    }

    private Map<BitSet, StructInfoCandidate> getCandidatesByRelationIdSet(BitSet targetRelationIdSet,
            boolean skipMaterializedViewLeaf) {
        Map<BitSet, StructInfoCandidate> candidates =
                candidatesByTargetRelationIdSet.get(targetRelationIdSet);
        if (candidates != null) {
            return candidates;
        }
        candidates = collectStructInfoCandidates(targetRelationIdSet, skipMaterializedViewLeaf);
        candidatesByTargetRelationIdSet.put((BitSet) targetRelationIdSet.clone(), candidates);
        return candidates;
    }

    void clearCandidateCache() {
        candidatesByTargetRelationIdSet.clear();
    }

    private Map<BitSet, StructInfoCandidate> collectStructInfoCandidates(BitSet targetRelationIdSet,
            boolean skipMaterializedViewLeaf) {
        Map<BitSet, StructInfoCandidate> collectedCandidates = new LinkedHashMap<>();
        for (GroupExpression groupExpression : ownerGroup.getLogicalExpressions()) {
            if (groupExpression.children().isEmpty()) {
                Plan leafPlan = groupExpression.getPlan();
                if (!(leafPlan instanceof LogicalRelation)) {
                    continue;
                }
                BitSet leafRelationIdSet = new BitSet();
                leafRelationIdSet.set(((LogicalRelation) leafPlan).getRelationId().asInt());
                if (!MaterializedViewUtils.containsAll(targetRelationIdSet, leafRelationIdSet)) {
                    continue;
                }
                // Without nested MV rewrite, rewritten MV scan leaves must not become query candidates.
                if (skipMaterializedViewLeaf
                        && leafPlan instanceof CatalogRelation
                        && ((CatalogRelation) leafPlan).getTable() instanceof MTMV) {
                    continue;
                }
                collectedCandidates.putIfAbsent((BitSet) leafRelationIdSet.clone(),
                        StructInfoCandidate.ofLeaf(leafPlan.withGroupExpression(Optional.empty())));
                continue;
            }
            List<Map<BitSet, StructInfoCandidate>> childCandidatesByChild = new ArrayList<>();
            boolean hasEmptyChild = false;
            for (Group child : groupExpression.children()) {
                Map<BitSet, StructInfoCandidate> childCandidates =
                        child.getStructInfoMap().getCandidatesByRelationIdSet(
                                targetRelationIdSet, skipMaterializedViewLeaf);
                if (childCandidates.isEmpty()) {
                    hasEmptyChild = true;
                    break;
                }
                childCandidatesByChild.add(childCandidates);
            }
            if (hasEmptyChild) {
                continue;
            }
            enumerateChildCandidateCombinations(groupExpression, childCandidatesByChild, 0, new BitSet(),
                    new ArrayList<>(childCandidatesByChild.size()), new HashMap<>(), collectedCandidates,
                    targetRelationIdSet);
        }
        return collectedCandidates;
    }

    /*
     * Enumerate child candidate combinations for one group expression. Each combination produces a union of
     * relation ids; only unions contained by targetRelationIdSet can become composite candidates.
     */
    private void enumerateChildCandidateCombinations(GroupExpression groupExpression,
            List<Map<BitSet, StructInfoCandidate>> childCandidatesByChild, int childOffset,
            BitSet currentRelationIdUnion, List<StructInfoCandidate> currentChildren,
            Map<Integer, Set<BitSet>> visitedRelationIdUnionsByOffset,
            Map<BitSet, StructInfoCandidate> candidates, BitSet targetRelationIdSet) {
        // Same relation-id union at the same child offset is equivalent for later matching.
        Set<BitSet> visitedRelationIdUnions = visitedRelationIdUnionsByOffset.computeIfAbsent(
                childOffset, ignored -> new HashSet<>());
        if (!visitedRelationIdUnions.add(currentRelationIdUnion)) {
            return;
        }
        if (childOffset >= childCandidatesByChild.size()) {
            candidates.putIfAbsent(currentRelationIdUnion,
                    StructInfoCandidate.ofComposite(groupExpression, currentChildren));
            return;
        }
        for (Map.Entry<BitSet, StructInfoCandidate> childCandidate
                : childCandidatesByChild.get(childOffset).entrySet()) {
            BitSet nextRelationIdUnion = (BitSet) currentRelationIdUnion.clone();
            nextRelationIdUnion.or(childCandidate.getKey());
            if (!MaterializedViewUtils.containsAll(targetRelationIdSet, nextRelationIdUnion)) {
                continue;
            }
            currentChildren.add(childCandidate.getValue());
            enumerateChildCandidateCombinations(groupExpression, childCandidatesByChild, childOffset + 1,
                    nextRelationIdUnion, currentChildren, visitedRelationIdUnionsByOffset, candidates,
                    targetRelationIdSet);
            currentChildren.remove(currentChildren.size() - 1);
        }
    }

    private BitSet createTargetRelationIdSet(BitSet mvBaseTableIdSet, CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        Multimap<Integer, Integer> tableIdToRelationIds = statementContext.getTableIdToRelationIds();
        BitSet targetRelationIdSet = new BitSet();
        // Include original query relations and nested MV scan relations that have been copied into the memo.
        for (int tableId = mvBaseTableIdSet.nextSetBit(0);
                tableId >= 0; tableId = mvBaseTableIdSet.nextSetBit(tableId + 1)) {
            for (Integer relationId : tableIdToRelationIds.get(tableId)) {
                targetRelationIdSet.set(relationId);
            }
            if (tableId == Integer.MAX_VALUE) {
                break;
            }
        }
        return targetRelationIdSet;
    }

    /*
     * Lightweight representative of one candidate plan tree. The relation-id set is kept as the map key outside
     * this object; StructInfo and rebuilt Plan are created lazily after relation-level filtering.
     */
    private static final class StructInfoCandidate {
        private final GroupExpression groupExpression;
        private final List<StructInfoCandidate> children;
        private final Map<Plan, StructInfo> structInfosByOriginPlan = new IdentityHashMap<>();
        private List<CatalogRelation> relations;
        private Plan materializedPlan;

        private StructInfoCandidate(Plan materializedPlan, GroupExpression groupExpression,
                List<StructInfoCandidate> children,
                List<CatalogRelation> relations) {
            this.materializedPlan = materializedPlan;
            this.groupExpression = groupExpression;
            this.children = children;
            this.relations = relations;
        }

        private static StructInfoCandidate ofLeaf(Plan plan) {
            List<CatalogRelation> relations = new ArrayList<>();
            if (plan instanceof CatalogRelation) {
                relations.add((CatalogRelation) plan);
            }
            return new StructInfoCandidate(plan, null, Collections.emptyList(), relations);
        }

        private static StructInfoCandidate ofComposite(GroupExpression groupExpression,
                List<StructInfoCandidate> children) {
            List<StructInfoCandidate> copiedChildren = new ArrayList<>(children);
            return new StructInfoCandidate(null, groupExpression, copiedChildren, null);
        }

        private List<CatalogRelation> getRelations() {
            if (relations != null) {
                return relations;
            }
            relations = new ArrayList<>();
            for (StructInfoCandidate child : children) {
                relations.addAll(child.getRelations());
            }
            return relations;
        }

        private StructInfo toStructInfo(@Nullable Plan originPlan, CascadesContext cascadesContext) {
            StructInfo structInfo = structInfosByOriginPlan.get(originPlan);
            if (structInfo != null) {
                return structInfo;
            }
            // Rebuilt candidates drop memo group expressions, but aggregate function signatures must keep the
            // original resolved signature. Recomputing them here can change output types under session variables.
            structInfo = MoreFieldsThread.keepFunctionSignature(() -> {
                Plan plan = toPlan();
                return originPlan == null ? StructInfo.of(plan, cascadesContext)
                        : StructInfo.of(plan, originPlan, cascadesContext);
            });
            structInfosByOriginPlan.put(originPlan, structInfo);
            return structInfo;
        }

        private Plan toPlan() {
            if (materializedPlan != null) {
                return materializedPlan;
            }
            List<Plan> childrenPlans = new ArrayList<>(children.size());
            for (StructInfoCandidate child : children) {
                childrenPlans.add(child.toPlan());
            }
            // Rebuild an equivalent plain plan tree without keeping the original memo binding.
            materializedPlan = groupExpression.getPlan()
                    .withChildren(childrenPlans)
                    .withGroupExpression(Optional.empty());
            return materializedPlan;
        }
    }

    @Override
    public String toString() {
        return "StructInfoMap{"
                + "ownerGroup=" + ownerGroup.getGroupId()
                + '}';
    }
}
