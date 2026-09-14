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

import org.apache.doris.planner.DictionarySink;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.TVFTableSink;
import org.apache.doris.thrift.TExplainLevel;

import java.util.List;

/**
 * Planning-time counterpart of the scan assignment checks in {@link UnassignedJobBuilder}.
 *
 * <p>The scan assignment can only place a fragment that holds a single olap scan
 * ({@code UnassignedScanSingleOlapTableJob}); a fragment that holds several of them is only legal
 * together with colocate or bucket shuffle semantics, and one that mixes an olap scan with another
 * kind of scan is not assignable at all. Those rules used to be enforced exclusively when the query
 * is executed, so a planner defect that elides an exchange and lets a second olap scan be absorbed
 * into a fragment produced a plan that {@code EXPLAIN} accepted and only the {@code SELECT}
 * rejected - see the recursive union / bucketed aggregation and the GroupJoin cases.
 *
 * <p>Validating the same rules right after the fragments are built turns such a plan into a
 * planning-time failure, which makes it reproducible from {@code EXPLAIN} and from an FE unit test
 * without a cluster. The checks below deliberately mirror the branches of
 * {@link UnassignedJobBuilder#buildJob} that bypass the scan based assignment.
 */
public class FragmentScanAssignmentValidator {

    private FragmentScanAssignmentValidator() {
    }

    /**
     * Rejects a translated plan whose fragments could not be assigned to scan workers.
     */
    public static void validate(List<PlanFragment> fragments) {
        for (PlanFragment fragment : fragments) {
            if (!assignsScanNodes(fragment)) {
                continue;
            }
            List<ScanNode> scanNodes = collectScanNodesInThisFragment(fragment);
            if (scanNodes.isEmpty()) {
                continue;
            }
            int olapScanNodeNum = olapScanNodeNum(scanNodes);
            if (olapScanNodeNum == scanNodes.size()) {
                if (olapScanNodeNum > 1 && !shouldAssignByBucket(fragment)) {
                    throw new IllegalStateException("Not supported multiple scan multiple OlapTable but "
                            + "not contains colocate join or bucket shuffle join: "
                            + fragment.getExplainString(TExplainLevel.VERBOSE));
                }
            } else if (olapScanNodeNum > 0) {
                throw new IllegalStateException("Cannot generate unassignedJob for fragment"
                        + " has both OlapScanNode and Other ScanNode: "
                        + fragment.getExplainString(TExplainLevel.VERBOSE));
            }
        }
    }

    /** Mirrors the early branches of {@link UnassignedJobBuilder#buildJob}. */
    private static boolean assignsScanNodes(PlanFragment fragment) {
        if (fragment.specifyInstances.isPresent()) {
            return false;
        }
        if (fragment.getSink() instanceof DictionarySink) {
            return false;
        }
        if (fragment.getSink() instanceof TVFTableSink) {
            TVFTableSink tvfSink = (TVFTableSink) fragment.getSink();
            return !"local".equals(tvfSink.getTvfName()) || tvfSink.getBackendId() == -1;
        }
        return true;
    }

    private static List<ScanNode> collectScanNodesInThisFragment(PlanFragment planFragment) {
        return planFragment.getPlanRoot().collectInCurrentFragment(ScanNode.class::isInstance);
    }

    private static int olapScanNodeNum(List<ScanNode> scanNodes) {
        int olapScanNodeNum = 0;
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                olapScanNodeNum++;
            }
        }
        return olapScanNodeNum;
    }

    private static boolean shouldAssignByBucket(PlanFragment fragment) {
        return fragment.hasColocatePlanNode() || fragment.hasBucketShuffleNode();
    }
}
