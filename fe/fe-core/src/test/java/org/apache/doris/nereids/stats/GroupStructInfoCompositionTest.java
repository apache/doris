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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit test of the contract that a memo group is described once per query: the parts of a group are
 * memoized on it and a parent composes the parts of its children, so asking for the struct info of a
 * node never describes the sub tree of that node a second time (see
 * {@link GroupStructInfo#partsOf(Group)}).
 */
public class GroupStructInfoCompositionTest {

    private static final GroupStructInfo.LiteralMode NO_LITERAL = GroupStructInfo.LiteralMode.NO_LITERAL;
    private static final GroupStructInfo.LiteralMode WITH_LITERAL = GroupStructInfo.LiteralMode.WITH_LITERAL;

    /** A filter over a scan, joined with two more scans: leaves and chain members in one memo. */
    private Memo joinChainMemo() {
        LogicalOlapScan t1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan t2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan t3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(), t1);
        LogicalJoin<LogicalFilter<LogicalOlapScan>, LogicalOlapScan> inner =
                new LogicalJoin<>(JoinType.INNER_JOIN, filter, t2, null);
        LogicalJoin<LogicalJoin<LogicalFilter<LogicalOlapScan>, LogicalOlapScan>, LogicalOlapScan> root =
                new LogicalJoin<>(JoinType.INNER_JOIN, inner, t3, null);
        return new Memo(null, root);
    }

    @Test
    void testAGroupIsNeverDescribedTwice() {
        Memo memo = joinChainMemo();
        GroupStructInfo.resetDescribedGroupCount();
        Map<Group, GroupStructInfo.HboStructParts> first = new LinkedHashMap<>();
        for (Group group : memo.getGroups()) {
            first.put(group, GroupStructInfo.partsOf(group));
        }
        int describedOnce = GroupStructInfo.getDescribedGroupCount();
        Assertions.assertTrue(describedOnce > 0, "no group was described");
        Assertions.assertTrue(describedOnce <= memo.getGroups().size(),
                "a group was described more than once in the first pass");
        // the same object comes back: every later request (both literal modes, every group) is
        // answered from the memo of that group
        for (Group group : memo.getGroups()) {
            GroupStructInfo.of(group, WITH_LITERAL);
            GroupStructInfo.of(group, NO_LITERAL);
            Assertions.assertSame(first.get(group), GroupStructInfo.partsOf(group));
        }
        Assertions.assertEquals(describedOnce, GroupStructInfo.getDescribedGroupCount(),
                "a group was described again after its parts had been built");
    }

    @Test
    void testTheRootDescribesTheChainInOnePass() {
        Memo memo = joinChainMemo();
        GroupStructInfo.resetDescribedGroupCount();
        GroupStructInfo.partsOf(memo.getRoot());
        int described = GroupStructInfo.getDescribedGroupCount();
        // one description for the root and one per scan of the chain: the inner join of a flattened
        // chain is not described at all, its sub tree is part of the canonical of the chain
        Assertions.assertEquals(memo.getGroups().size() - 1, described,
                "the root did not describe the chain in one pass");
    }

    @Test
    void testAParentComposesThePartsOfItsChildren() {
        Memo memo = joinChainMemo();
        GroupStructInfo.HboStructParts rootParts = GroupStructInfo.partsOf(memo.getRoot());
        Assertions.assertTrue(rootParts.isValid(), rootParts.getSkipReason());
        String rootCanonical = rootParts.canonical(NO_LITERAL);
        String rootShape = rootParts.shape(NO_LITERAL);
        int scans = 0;
        String filterCanonical = null;
        for (Group group : memo.getGroups()) {
            GroupStructInfo.HboStructParts parts = GroupStructInfo.partsOf(group);
            String canonical = parts.canonical(NO_LITERAL);
            if (canonical.startsWith("F{")) {
                filterCanonical = canonical;
                continue;
            }
            if (!canonical.startsWith("S{")) {
                // a chain member is replaced by the canonical of the whole chain
                continue;
            }
            scans++;
            // the child is not rebuilt: its canonical and its shape are literally the parts which
            // appear inside the canonical of the parent
            Assertions.assertTrue(rootCanonical.contains(canonical),
                    "the canonical of the chain does not contain " + canonical);
            Assertions.assertTrue(rootShape.contains(parts.shape(NO_LITERAL)),
                    "the shape of the chain does not contain " + parts.shape(NO_LITERAL));
        }
        Assertions.assertEquals(3, scans, "the chain has three scans");
        Assertions.assertNotNull(filterCanonical, "the filter of the memo was not described");
        Assertions.assertTrue(rootCanonical.contains(filterCanonical),
                "the canonical of the chain does not contain the filter leaf");
        // a shape is never empty and the fingerprint is the sha256 of it
        Assertions.assertFalse(rootShape.isEmpty(), "the shape of the root is empty");
        Assertions.assertEquals(Hashing.sha256().hashString(rootShape, StandardCharsets.UTF_8).toString(),
                rootParts.fingerprint(NO_LITERAL),
                "the fingerprint of a group is not the sha256 of its shape");
    }
}
