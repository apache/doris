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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class UnboundIcebergTableSinkTest {
    @Test
    public void testBranchNameSurvivesPlanCopies() {
        Plan child = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of());
        UnboundIcebergTableSink<Plan> sink = new UnboundIcebergTableSink<>(
                ImmutableList.of("catalog", "db", "table"),
                ImmutableList.of("old_name"),
                ImmutableList.of(),
                ImmutableList.of(),
                child);
        sink = sink.withBranchName(Optional.of("historical_branch"));

        Plan replacementChild = new LogicalOneRowRelation(new RelationId(2), ImmutableList.of());
        UnboundIcebergTableSink<?> copied = (UnboundIcebergTableSink<?>) sink.withChildren(
                ImmutableList.of(replacementChild));

        Assertions.assertEquals(Optional.of("historical_branch"), copied.getBranchName());
        Assertions.assertSame(replacementChild, copied.child());
    }
}
