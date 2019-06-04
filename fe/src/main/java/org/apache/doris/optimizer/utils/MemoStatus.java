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

package org.apache.doris.optimizer.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.OptMemo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class MemoStatus {
    private static final Logger LOG = LogManager.getLogger(MemoStatus.class);
    private final OptMemo memo;

    public MemoStatus(OptMemo memo) {
        this.memo = memo;
    }

    public boolean checkStatusForExplore() {
        return checkStatus(CheckType.LOGICAL);
    }

    public boolean checkStatusForImplement() {
        return checkStatus(CheckType.PHYSICAL);
    }

    public boolean checkStatus() {
        return checkStatus(CheckType.LOGICAL) && checkStatus(CheckType.PHYSICAL);
    }

    private boolean checkStatus(CheckType type) {
        final List<OptGroup> unoptimizedGroups = Lists.newArrayList();
        final List<MultiExpression> unoptimizedMExprs = Lists.newArrayList();
        collectUnoptimizedGroupAndMExpr(memo.getGroups(), unoptimizedGroups, unoptimizedMExprs, type);
        printStatus(unoptimizedGroups, unoptimizedMExprs);
        return unoptimizedGroups.size() == 0 && unoptimizedMExprs.size() == 0 ? true : false;
    }

    private void collectUnoptimizedGroupAndMExpr(List<OptGroup> groups, List<OptGroup> unoptimizedGroups,
                                                        List<MultiExpression> unoptimizedMExprs, CheckType type) {
        for (OptGroup group : groups) {
            if (!group.isOptimized() && !group.isItemGroup()) {
                unoptimizedGroups.add(group);
            }
            if (type == CheckType.PHYSICAL) {
                MultiExpression firstPhysicalMExpr = group.getFirstPhysicalMultiExpression();
                for (; firstPhysicalMExpr != null; firstPhysicalMExpr = group.nextPhysicalExpr(firstPhysicalMExpr)) {
                    if (!firstPhysicalMExpr.isOptimized()) {
                        unoptimizedMExprs.add(firstPhysicalMExpr);
                    }
                }
            } else if (type == CheckType.LOGICAL) {
                MultiExpression firstLogicalMExpr = group.getFirstLogicalMultiExpression();
                for (; firstLogicalMExpr != null; firstLogicalMExpr = group.nextLogicalExpr(firstLogicalMExpr)) {
                    if (!firstLogicalMExpr.isOptimized()) {
                        unoptimizedMExprs.add(firstLogicalMExpr);
                    }
                }
            } else {
                MultiExpression firstLogicalMExpr = group.getFirstMultiExpression();
                for (; firstLogicalMExpr != null; firstLogicalMExpr = group.nextExpr(firstLogicalMExpr)) {
                    if (!firstLogicalMExpr.isOptimized()) {
                        unoptimizedMExprs.add(firstLogicalMExpr);
                    }
                }
            }
        }
    }

    private static void printStatus(List<OptGroup> unoptimizedGroups,
                                    List<MultiExpression> unoptimizedMExprs) {
        final StringBuilder unoptimizedGroupsLog = new StringBuilder("Unoptimized Group ids:");
        for (OptGroup group : unoptimizedGroups) {
            unoptimizedGroupsLog.append(group.getId()).append(" status:")
                    .append(group.getStatus()).append(" MultiExpression ids:");
            for (MultiExpression mExpr : group.getMultiExpressions()) {
                unoptimizedGroupsLog.append(mExpr.getId()).append(" status:").append(mExpr.getStatus()).append(", ");
            }
        }
        if (unoptimizedGroups.size() > 0) {
            LOG.info(unoptimizedGroupsLog);
        }

        final StringBuilder unimplementedMExprLog = new StringBuilder("Unoptimized MultiExpression ids:");
        for (MultiExpression mExpr : unoptimizedMExprs) {
            unimplementedMExprLog.append(mExpr.getOp()).append(" ")
                    .append(mExpr.getId()).append(" ")
                    .append(mExpr.getOp().toString()).append(", ");
        }
        if (unoptimizedMExprs.size() > 0) {
            LOG.info(unimplementedMExprLog);
        }
    }

    public boolean checkLeakInDag() {
        final Set<Integer> candidateSearchedGroupIds = Sets.newHashSet();
        final Set<Integer> candidateSearchedMExprIds = Sets.newHashSet();
        for (OptGroup group : memo.getGroups()) {
            candidateSearchedGroupIds.add(group.getId());
        }
        for (MultiExpression mExpr : memo.getMExprs().values()) {
            candidateSearchedMExprIds.add(mExpr.getId());
        }

        searchDag(memo.getRoot(), candidateSearchedGroupIds, candidateSearchedMExprIds);

        final StringBuilder groupIdsBuilder = new StringBuilder("Leak Group ids:");
        for (int id : candidateSearchedGroupIds) {
            groupIdsBuilder.append(id).append(", ");
        }
        if (candidateSearchedGroupIds.size() > 0) {
            LOG.info(groupIdsBuilder.toString());
        }

        final StringBuilder mExprIdsBuilder = new StringBuilder("Leak MultiExpression ids:");
        for (int id : candidateSearchedMExprIds) {
            mExprIdsBuilder.append(id).append(", ");
        }
        if (candidateSearchedMExprIds.size() > 0) {
            LOG.info(mExprIdsBuilder.toString());
        }

        return !candidateSearchedGroupIds.isEmpty()
                || !candidateSearchedMExprIds.isEmpty();
    }

    private void searchDag(OptGroup root, Set<Integer> candidateSearchedGroupIds, Set<Integer> candidateSearchedMExprIds) {
        candidateSearchedGroupIds.remove(root.getId());
        for (MultiExpression mExpr : root.getMultiExpressions()) {
            candidateSearchedMExprIds.remove(mExpr.getId());
            for (OptGroup input : mExpr.getInputs()) {
                searchDag(input, candidateSearchedGroupIds, candidateSearchedMExprIds);
            }
        }
    }

    public List<OptGroup> getNonItemGroups() {
        final List<OptGroup> groups = Lists.newArrayList();
        for (OptGroup group : memo.getGroups()) {
            if (!group.isItemGroup()) {
                groups.add(group);
            }
        }
        return groups;
    }

    public List<OptGroup> getItemGroups() {
        final List<OptGroup> groups = Lists.newArrayList();
        for (OptGroup group : memo.getGroups()) {
            if (group.isItemGroup()) {
                groups.add(group);
            }
        }
        return groups;
    }


    enum CheckType {
        LOGICAL,
        PHYSICAL,
        ITEM,
        ALL
    }
}
