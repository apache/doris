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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.trees.plans.RelationId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Eleventh review round: matching / decompile safety.
 *
 * - The distributed TopN fold must test the EXACT continuation identity
 *   (inner.limit == outer.limit + outer.offset, overflow-safe instead of the old
 *   `inner <= outer` which was false for every positive offset and true for unrelated
 *   semantic pairs).
 * - The temporary-partition namespace takes part in the scan digest and the L3 scan
 *   identity (PARTITION(p) vs TEMPORARY PARTITION(p) may share names across a lifecycle
 *   transition).
 * - Partition / tablet selections are real multisets: the old size + HashSet test
 *   equated (p1, p1, p2) with (p1, p2, p2).
 */
public class SPMRound11SafetyTest {

    // ==================== R11-5: overflow-safe TopN continuation identity ====================

    @Test
    public void testTopNContinuationLimitIdentity() {
        // Nereids builds MERGE(L, O) -> LOCAL(L+O, 0): the ONLY foldable pair
        Assertions.assertTrue(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(100, 0, 100));
        Assertions.assertTrue(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(100, 1, 101),
                "LIMIT 100 OFFSET 1 keeps 101 rows locally - the old `inner <= outer` test"
                        + " was false for every positive offset");
        Assertions.assertFalse(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(100, 1, 100),
                "a local stage that kept only L rows cannot be this merge stage's local half");
        Assertions.assertFalse(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(5, 0, 2),
                "two unrelated SEMANTIC TopNs (inner LIMIT 2, outer LIMIT 5) must NOT fold:"
                        + " the fold would return 5 rows instead of 2");
        // unlimited stages stay unlimited
        Assertions.assertTrue(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(
                Long.MAX_VALUE, 0, Long.MAX_VALUE));
        Assertions.assertFalse(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(
                Long.MAX_VALUE, 1, Long.MAX_VALUE));
        // overflow is checked, never wrapped: MAX-5 + 6 would wrap negative
        Assertions.assertTrue(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(
                Long.MAX_VALUE - 5, 5, Long.MAX_VALUE));
        Assertions.assertFalse(SPMPlan2SQLBuilder.isExactTopNContinuationLimit(
                Long.MAX_VALUE - 5, 6, 0),
                "the sum would overflow: the identity must fail instead of wrapping negative");
    }

    // ==================== R11-7: temporary-partition namespace ====================

    @Test
    public void testTemporaryPartitionNamespaceInDigestAndIdentity() {
        UnboundRelation formal = new UnboundRelation(new RelationId(1),
                List.of("db", "t"), List.of("p1"), false);
        UnboundRelation temporary = new UnboundRelation(new RelationId(2),
                List.of("db", "t"), List.of("p1"), true);

        Assertions.assertNotEquals(formal.toDigest(), temporary.toDigest(),
                "PARTITION(p1) and TEMPORARY PARTITION(p1) are different namespaces");
        Assertions.assertTrue(formal.toDigest().contains("PARTITION(?)"),
                "partition selection enters the digest: " + formal.toDigest());
        Assertions.assertFalse(formal.toDigest().contains("TEMPORARY"), formal.toDigest());
        Assertions.assertTrue(temporary.toDigest().contains("TEMPORARY PARTITION(?)"),
                temporary.toDigest());
        UnboundRelation noPartition = new UnboundRelation(new RelationId(3), List.of("db", "t"));
        Assertions.assertFalse(noPartition.toDigest().contains("PARTITION"),
                "no selection, no marker: " + noPartition.toDigest());

        Assertions.assertFalse(SPMPlanTreeSupport.sameScanIdentityForTest(formal, temporary),
                "the L3 scan identity must compare the namespace in both directions");
        Assertions.assertTrue(SPMPlanTreeSupport.sameScanIdentityForTest(formal,
                new UnboundRelation(new RelationId(4), List.of("db", "t"), List.of("p1"),
                        false)),
                "the same formal selection still matches");
        Assertions.assertTrue(SPMPlanTreeSupport.sameScanIdentityForTest(temporary,
                new UnboundRelation(new RelationId(5), List.of("db", "t"), List.of("p1"),
                        true)),
                "the same temporary selection still matches");
        Assertions.assertTrue(SPMPlanTreeSupport.sameScanIdentityForTest(
                new UnboundRelation(new RelationId(6), List.of("db", "t"),
                        List.of("p1", "p2"), false),
                new UnboundRelation(new RelationId(7), List.of("db", "t"),
                        List.of("p2", "p1"), false)),
                "the selection order does not change which data is read");
    }

    // ==================== R11-10: selections as real multisets ====================

    @Test
    public void testSelectionComparisonIsAMultiset() {
        Assertions.assertTrue(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1", "p2"), List.of("p2", "p1")), "order is irrelevant");
        Assertions.assertFalse(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1", "p1", "p2"), List.of("p1", "p2", "p2")),
                "same size, same hash SET, but different multiplicities: a match could"
                        + " double a DIFFERENT partition than the user asked for");
        Assertions.assertTrue(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1", "p1", "p2"), List.of("p1", "p1", "p2")));
        Assertions.assertTrue(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1", "p1", "p2"), List.of("p2", "p1", "p1")));
        Assertions.assertFalse(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1", "p1"), List.of("p1")),
                "a duplicate entry is a different multiset");
        Assertions.assertFalse(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1"), List.of("p1", "p1")), "size check is symmetric");
        Assertions.assertTrue(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(null, null));
        Assertions.assertFalse(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                null, List.of("p1")));
        Assertions.assertFalse(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of("p1"), null));
        Assertions.assertTrue(SPMPlanTreeSupport.sameSelectionIgnoreOrderForTest(
                List.of(1L, 2L), List.of(2L, 1L)), "tablet ids are compared the same way");
    }
}
