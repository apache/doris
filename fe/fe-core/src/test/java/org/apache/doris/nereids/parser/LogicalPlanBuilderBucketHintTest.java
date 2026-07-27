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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Verify that the {@code BUCKET(id, ...)} table hint is parsed into
 * {@link UnboundRelation#getBucketIds()}, mirroring the existing
 * {@code TABLET(id, ...)} hint.
 */
public class LogicalPlanBuilderBucketHintTest {

    private final NereidsParser parser = new NereidsParser();

    private UnboundRelation getUnboundRelation(String sql) {
        LogicalPlan plan = parser.parseSingle(sql);
        Optional<UnboundRelation> relation = plan.collectFirst(UnboundRelation.class::isInstance);
        Assertions.assertTrue(relation.isPresent(), "no UnboundRelation found in plan of: " + sql);
        return relation.get();
    }

    @Test
    public void testParseBucketHint() {
        UnboundRelation relation = getUnboundRelation("SELECT * FROM t BUCKET(0, 2)");
        Assertions.assertEquals(ImmutableList.of(0L, 2L), relation.getBucketIds());
        Assertions.assertTrue(relation.getTabletIds().isEmpty());
    }

    @Test
    public void testParseSingleBucketHint() {
        UnboundRelation relation = getUnboundRelation("SELECT * FROM t BUCKET(3)");
        Assertions.assertEquals(ImmutableList.of(3L), relation.getBucketIds());
    }

    @Test
    public void testTabletHintStillParsed() {
        UnboundRelation relation = getUnboundRelation("SELECT * FROM t TABLET(10, 20)");
        Assertions.assertEquals(ImmutableList.of(10L, 20L), relation.getTabletIds());
        Assertions.assertTrue(relation.getBucketIds().isEmpty());
    }

    @Test
    public void testNoHint() {
        UnboundRelation relation = getUnboundRelation("SELECT * FROM t");
        Assertions.assertTrue(relation.getBucketIds().isEmpty());
        Assertions.assertTrue(relation.getTabletIds().isEmpty());
    }

    @Test
    public void testBucketWithPartitionHint() {
        UnboundRelation relation = getUnboundRelation("SELECT * FROM t PARTITION(p1) BUCKET(1)");
        Assertions.assertEquals(ImmutableList.of(1L), relation.getBucketIds());
    }
}
