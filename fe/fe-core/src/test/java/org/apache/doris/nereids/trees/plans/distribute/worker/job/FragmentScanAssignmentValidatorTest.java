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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.nereids.trees.plans.distribute.NereidsSpecifyInstances;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DictionarySink;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.planner.TVFTableSink;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The planning-time scan assignment check: a fragment may hold several olap scans only with colocate
 * or bucket shuffle semantics, and may not mix an olap scan with another kind of scan. These cases
 * cover the branches mirrored from {@link UnassignedJobBuilder#buildJob}.
 */
public class FragmentScanAssignmentValidatorTest {

    @Test
    public void testSingleOlapScanIsAccepted() {
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragmentWith(mockOlapScan())));
    }

    @Test
    public void testFragmentWithoutScanIsAccepted() {
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragmentWith()));
    }

    @Test
    public void testSeveralOlapScansInOneFragmentAreRejected() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), mockOlapScan());
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment)));
        Assertions.assertTrue(exception.getMessage().contains("Not supported multiple scan multiple OlapTable"),
                exception.getMessage());
    }

    @Test
    public void testSeveralOlapScansAreAcceptedWithColocatePlanNode() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), mockOlapScan());
        fragment.setHasColocatePlanNode(true);
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment));
    }

    @Test
    public void testSeveralOlapScansAreAcceptedWithBucketShuffleJoin() {
        HashJoinNode bucketShuffleJoin = Mockito.mock(HashJoinNode.class);
        Mockito.when(bucketShuffleJoin.isBucketShuffle()).thenReturn(true);
        PlanFragment fragment = fragmentWith(ImmutableList.of(mockOlapScan(), mockOlapScan(), bucketShuffleJoin));
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment));
    }

    @Test
    public void testOlapScanMixedWithAnotherScanIsRejected() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), Mockito.mock(SchemaScanNode.class));
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment)));
        Assertions.assertTrue(exception.getMessage().contains("has both OlapScanNode and Other ScanNode"),
                exception.getMessage());
    }

    @Test
    public void testFragmentWithSpecifyInstancesIsNotChecked() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), mockOlapScan());
        fragment.specifyInstances = Optional.of(Mockito.mock(NereidsSpecifyInstances.class));
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment));
    }

    @Test
    public void testFragmentWithDictionarySinkIsNotChecked() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), mockOlapScan());
        fragment.setSink(Mockito.mock(DictionarySink.class));
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment));
    }

    @Test
    public void testFragmentWithLocalTvfSinkIsNotChecked() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), mockOlapScan());
        TVFTableSink tvfSink = Mockito.mock(TVFTableSink.class);
        Mockito.when(tvfSink.getTvfName()).thenReturn("local");
        Mockito.when(tvfSink.getBackendId()).thenReturn(1L);
        fragment.setSink(tvfSink);
        FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment));
    }

    @Test
    public void testFragmentWithOtherTvfSinkIsChecked() {
        PlanFragment fragment = fragmentWith(mockOlapScan(), mockOlapScan());
        TVFTableSink tvfSink = Mockito.mock(TVFTableSink.class);
        Mockito.when(tvfSink.getTvfName()).thenReturn("numbers");
        fragment.setSink(tvfSink);
        Assertions.assertThrows(IllegalStateException.class,
                () -> FragmentScanAssignmentValidator.validate(ImmutableList.of(fragment)));
    }

    private PlanFragment fragmentWith(PlanNode... nodes) {
        return fragmentWith(ImmutableList.copyOf(nodes));
    }

    private PlanFragment fragmentWith(List<PlanNode> nodes) {
        PlanNode root = Mockito.mock(PlanNode.class);
        Mockito.when(root.getChildren()).thenReturn(Lists.newArrayList());
        Mockito.when(root.collectInCurrentFragment(Mockito.any())).thenAnswer(invocation -> {
            Predicate<PlanNode> predicate = invocation.getArgument(0);
            return nodes.stream().filter(predicate).collect(Collectors.toList());
        });
        return new PlanFragment(new PlanFragmentId(0), root, DataPartition.RANDOM);
    }

    private OlapScanNode mockOlapScan() {
        return Mockito.mock(OlapScanNode.class);
    }
}
