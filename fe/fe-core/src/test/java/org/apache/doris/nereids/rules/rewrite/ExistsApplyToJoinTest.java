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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

class ExistsApplyToJoinTest implements MemoPatternMatchSupported {

    @Test
    public void testCorrelatedExists() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        List<Slot> leftSlots = left.getOutput();
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(0, "t2", 1);
        List<Slot> rightSlots = right.getOutput();
        EqualTo equalTo = new EqualTo(leftSlots.get(0), rightSlots.get(0));
        Exists exists = new Exists(right, false);
        LogicalApply<LogicalOlapScan, LogicalOlapScan> apply =
                new LogicalApply<>(ImmutableList.of(leftSlots.get(0), rightSlots.get(0)),
                        exists, Optional.of(equalTo), Optional.empty(),
                        false, false, false, left, right);
        PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyTopDown(new ExistsApplyToJoin())
                .matchesFromRoot(logicalJoin(
                        logicalOlapScan(),
                        logicalOlapScan()
                ).when(j -> j.getJoinType().equals(JoinType.LEFT_SEMI_JOIN)));
    }

    @Test
    public void testUnCorrelatedExists() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        List<Slot> leftSlots = left.getOutput();
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(0, "t2", 1);
        List<Slot> rightSlots = right.getOutput();
        EqualTo equalTo = new EqualTo(leftSlots.get(0), rightSlots.get(0));
        Exists exists = new Exists(right, false);
        LogicalApply<LogicalOlapScan, LogicalOlapScan> apply =
                new LogicalApply<>(Collections.emptyList(),
                        exists, Optional.of(equalTo), Optional.empty(),
                        false, false, false, left, right);
        PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyTopDown(new ExistsApplyToJoin())
                .matchesFromRoot(logicalJoin(
                        logicalOlapScan(),
                        logicalLimit(logicalOlapScan())
                ).when(j -> j.getJoinType().equals(JoinType.CROSS_JOIN)));
    }

    @Test
    public void testUnCorrelatedNotExists() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        List<Slot> leftSlots = left.getOutput();
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(0, "t2", 1);
        List<Slot> rightSlots = right.getOutput();
        EqualTo equalTo = new EqualTo(leftSlots.get(0), rightSlots.get(0));
        Exists exists = new Exists(right, true);
        LogicalApply<LogicalOlapScan, LogicalOlapScan> apply =
                new LogicalApply<>(Collections.emptyList(),
                        exists, Optional.of(equalTo), Optional.empty(),
                        false, false, false, left, right);
        PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyTopDown(new ExistsApplyToJoin())
                .matchesFromRoot(logicalFilter(logicalJoin(
                        logicalOlapScan(),
                        logicalAggregate(logicalLimit(logicalOlapScan()))).when(
                            j -> j.getJoinType().equals(JoinType.CROSS_JOIN)))
                );
    }

    @Test
    public void testCorrelatedNotExists() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        List<Slot> leftSlots = left.getOutput();
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(0, "t2", 1);
        List<Slot> rightSlots = right.getOutput();
        EqualTo equalTo = new EqualTo(leftSlots.get(0), rightSlots.get(0));
        Exists exists = new Exists(right, true);
        LogicalApply<LogicalOlapScan, LogicalOlapScan> apply =
                new LogicalApply<>(ImmutableList.of(leftSlots.get(0), rightSlots.get(0)),
                        exists, Optional.of(equalTo), Optional.empty(),
                        false, false, false, left, right);
        PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyTopDown(new ExistsApplyToJoin())
                .matchesFromRoot(logicalJoin(
                        logicalOlapScan(),
                        logicalOlapScan()
                ).when(j -> j.getJoinType().equals(JoinType.LEFT_ANTI_JOIN)));
    }

}
