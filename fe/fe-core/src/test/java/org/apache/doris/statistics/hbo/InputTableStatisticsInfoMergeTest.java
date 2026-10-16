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

package org.apache.doris.statistics.hbo;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Unit test of the fold two runtime nodes with one fingerprint perform: the provider stores one entry per
 * fingerprint, so the input statistics of both nodes have to be carried by that entry - the drift
 * comparison reads the rows of every recorded scan.
 */
public class InputTableStatisticsInfoMergeTest {

    @Test
    public void testTheInputStatisticsOfTwoNodesAreConcatenated() {
        PlanStatistics first = Mockito.mock(PlanStatistics.class);
        PlanStatistics second = Mockito.mock(PlanStatistics.class);
        InputTableStatisticsInfo one = new InputTableStatisticsInfo(Optional.of(ImmutableList.of(first)));
        InputTableStatisticsInfo other = new InputTableStatisticsInfo(Optional.of(ImmutableList.of(second)));

        Assertions.assertEquals(ImmutableList.of(first, second),
                one.merge(other).getInputTableStatistics().get());
        // a node without input statistics contributes nothing and neither side is lost
        Assertions.assertEquals(one, one.merge(new InputTableStatisticsInfo(Optional.empty())));
        Assertions.assertEquals(other, new InputTableStatisticsInfo(Optional.empty()).merge(other));
    }

    @Test
    public void testAnEntryWithoutInputTablesIsNotACandidateForPublishing() {
        // the read side reads an empty input list as a manual wildcard which matches by fingerprint
        // alone, so a published entry has to carry the rows of at least one scan - otherwise a query
        // with another predicate would inherit its count, while the fingerprint alone does not
        // distinguish the predicates of two queries
        PlanStatistics scan = Mockito.mock(PlanStatistics.class);
        Assertions.assertFalse(new InputTableStatisticsInfo(Optional.empty()).hasInputTables());
        Assertions.assertFalse(new InputTableStatisticsInfo(Optional.of(ImmutableList.of())).hasInputTables());
        Assertions.assertTrue(new InputTableStatisticsInfo(Optional.of(ImmutableList.of(scan))).hasInputTables());
        // and the fold keeps the information of both sides
        InputTableStatisticsInfo merged = new InputTableStatisticsInfo(Optional.of(ImmutableList.of(scan)))
                .merge(new InputTableStatisticsInfo(Optional.of(ImmutableList.of(scan))));
        Assertions.assertTrue(merged.hasInputTables());
        Assertions.assertFalse(new InputTableStatisticsInfo(Optional.empty())
                .merge(new InputTableStatisticsInfo(Optional.of(ImmutableList.of()))).hasInputTables());
    }
}
