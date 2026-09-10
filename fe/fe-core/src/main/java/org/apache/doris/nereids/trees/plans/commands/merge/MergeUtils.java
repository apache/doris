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

package org.apache.doris.nereids.trees.plans.commands.merge;

import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

/**
 * Shared plan-construction helpers for MERGE INTO, used by both the internal OLAP path
 * ({@link MergeIntoCommand}) and the external path
 * ({@link org.apache.doris.nereids.trees.plans.commands.ExternalRowLevelMergePlanBuilder}).
 */
public class MergeUtils {

    private MergeUtils() {
    }

    /**
     * Build the base join between merge target and source, with the target on the LEFT (probe)
     * side. Doris builds the hash table on the right child, and the target side is structurally
     * the wide one: it must carry every table column plus the row identity for the sink, while
     * the source usually only carries join keys and new values. Keeping the target on the probe
     * side also lets RuntimeFilterGenerator prune the target scan with runtime filters built
     * from the source side: INNER and RIGHT_OUTER joins may produce runtime filters while
     * LEFT_OUTER is in its denied list.
     *
     * <p>Unmatched source rows are only needed by WHEN NOT MATCHED clauses, so without them the
     * join is INNER; with them, RIGHT OUTER preserves exactly the unmatched source rows, which
     * is equivalent to the previous "source LEFT OUTER JOIN target" shape.
     */
    public static LogicalPlan buildMergeJoin(LogicalPlan targetPlan, LogicalPlan source,
            Expression onClause, boolean hasNotMatchedClauses) {
        JoinType joinType = hasNotMatchedClauses ? JoinType.RIGHT_OUTER_JOIN : JoinType.INNER_JOIN;
        return new LogicalJoin<>(joinType,
                ImmutableList.of(), ImmutableList.of(onClause),
                targetPlan, source, JoinReorderContext.EMPTY);
    }
}
