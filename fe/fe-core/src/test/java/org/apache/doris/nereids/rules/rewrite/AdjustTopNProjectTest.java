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

import com.google.common.collect.ImmutableList;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AdjustTopNProjectTest implements MemoPatternMatchSupported {
    LogicalOlapScan score = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
    @Test
    void testTopNProject() {
        Slot sid = score.getOutput().get(0);
        Alias decodeSid = new Alias(new DecodeAsVarchar(sid));
        Alias aliasSid = new Alias(sid);
        LogicalProject<LogicalOlapScan> bottomProject = new LogicalProject<>(
                ImmutableList.of(decodeSid, aliasSid), score);
        List<OrderKey> orderKeys = ImmutableList.of(new OrderKey(aliasSid.toSlot(), true, true));
        LogicalTopN topN = new LogicalTopN(orderKeys, 1, 1, bottomProject);
        LogicalProject topProject = new LogicalProject(ImmutableList.of(decodeSid.toSlot()), topN);
        PlanChecker.from(MemoTestUtils.createConnectContext(), topProject)
                .applyTopDown(ImmutableList.of(new MergeProjects().build(), new AdjustTopNProject().build()))
                .matches(
                        logicalProject(
                                logicalTopN(logicalOlapScan())));
    }
}
