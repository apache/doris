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

import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.distribute.worker.LoadBalanceScanWorkerSelector;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExplainLevel;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * UnassignedJobBuilder.
 * build UnassignedJob by fragment
 */
public class UnassignedJobBuilder {
    private final ScanWorkerSelector scanWorkerSelector = new LoadBalanceScanWorkerSelector();

    /**
     * build job from fragment.
     */
    public static FragmentIdMapping<UnassignedJob> buildJobs(FragmentIdMapping<PlanFragment> fragments) {
        UnassignedJobBuilder builder = new UnassignedJobBuilder();

        FragmentLineage fragmentLineage = buildFragmentLineage(fragments);
        FragmentIdMapping<UnassignedJob> unassignedJobs = new FragmentIdMapping<>();

        // build from leaf to parent
        for (Entry<PlanFragmentId, PlanFragment> kv : fragments.entrySet()) {
            PlanFragmentId fragmentId = kv.getKey();
            PlanFragment fragment = kv.getValue();

            ListMultimap<ExchangeNode, UnassignedJob> inputJobs = findInputJobs(
                    fragmentLineage, fragmentId, unassignedJobs);
            UnassignedJob unassignedJob = builder.buildJob(fragment, inputJobs);
            unassignedJobs.put(fragmentId, unassignedJob);
        }
        return unassignedJobs;
    }

    private UnassignedJob buildJob(
            PlanFragment planFragment, ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        List<ScanNode> scanNodes = collectScanNodesInThisFragment(planFragment);
        if (planFragment.specifyInstances.isPresent()) {
            return buildSpecifyInstancesJob(planFragment, scanNodes, inputJobs);
        } else if (!scanNodes.isEmpty() || isLeafFragment(planFragment)) {
            return buildLeafOrScanJob(planFragment, scanNodes, inputJobs);
        } else {
            return buildShuffleJob(planFragment, inputJobs);
        }
    }

    private UnassignedJob buildLeafOrScanJob(
            PlanFragment planFragment, List<ScanNode> scanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        int olapScanNodeNum = olapScanNodeNum(scanNodes);

        UnassignedJob unassignedJob = null;
        if (!scanNodes.isEmpty() && olapScanNodeNum == scanNodes.size()) {
            // we need assign a backend which contains the data,
            // so that the OlapScanNode can find the data in the backend
            // e.g. select * from olap_table
            unassignedJob = buildScanOlapTableJob(planFragment, (List) scanNodes, inputJobs, scanWorkerSelector);
        } else if (scanNodes.isEmpty()) {
            // select constant without table,
            // e.g. select 100 union select 200
            unassignedJob = buildQueryConstantJob(planFragment);
        } else if (olapScanNodeNum == 0) {
            ScanNode scanNode = scanNodes.get(0);
            if (scanNode instanceof SchemaScanNode) {
                // select * from information_schema.tables
                unassignedJob = buildScanMetadataJob(planFragment, (SchemaScanNode) scanNode, scanWorkerSelector);
            } else {
                // only scan external tables or cloud tables or table valued functions
                // e,g. select * from numbers('number'='100')
                unassignedJob = buildScanRemoteTableJob(planFragment, scanNodes, inputJobs, scanWorkerSelector);
            }
        }

        if (unassignedJob != null) {
            return unassignedJob;
        }

        throw new IllegalStateException("Unsupported build UnassignedJob for fragment: "
                + planFragment.getExplainString(TExplainLevel.VERBOSE));
    }

    private UnassignedJob buildSpecifyInstancesJob(
            PlanFragment planFragment, List<ScanNode> scanNodes, ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        return new UnassignedSpecifyInstancesJob(planFragment, scanNodes, inputJobs);
    }

    private UnassignedJob buildScanOlapTableJob(
            PlanFragment planFragment, List<OlapScanNode> olapScanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs,
            ScanWorkerSelector scanWorkerSelector) {
        if (shouldAssignByBucket(planFragment)) {
            return new UnassignedScanBucketOlapTableJob(
                    planFragment, olapScanNodes, inputJobs, scanWorkerSelector);
        } else if (olapScanNodes.size() == 1) {
            return new UnassignedScanSingleOlapTableJob(
                    planFragment, olapScanNodes.get(0), inputJobs, scanWorkerSelector);
        } else {
            throw new IllegalStateException("Not supported multiple scan multiple "
                    + "OlapTable but not contains colocate join or bucket shuffle join: "
                    + planFragment.getExplainString(TExplainLevel.VERBOSE));
        }
    }

    private List<ScanNode> collectScanNodesInThisFragment(PlanFragment planFragment) {
        return planFragment.getPlanRoot().collectInCurrentFragment(ScanNode.class::isInstance);
    }

    private int olapScanNodeNum(List<ScanNode> scanNodes) {
        int olapScanNodeNum = 0;
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                olapScanNodeNum++;
            }
        }
        return olapScanNodeNum;
    }

    private boolean isLeafFragment(PlanFragment planFragment) {
        return planFragment.getChildren().isEmpty();
    }

    private UnassignedQueryConstantJob buildQueryConstantJob(PlanFragment planFragment) {
        return new UnassignedQueryConstantJob(planFragment);
    }

    private UnassignedJob buildScanMetadataJob(
            PlanFragment fragment, SchemaScanNode schemaScanNode, ScanWorkerSelector scanWorkerSelector) {
        return new UnassignedScanMetadataJob(fragment, schemaScanNode, scanWorkerSelector);
    }

    private UnassignedJob buildScanRemoteTableJob(
            PlanFragment planFragment, List<ScanNode> scanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs,
            ScanWorkerSelector scanWorkerSelector) {
        if (scanNodes.size() == 1) {
            return new UnassignedScanSingleRemoteTableJob(
                    planFragment, scanNodes.get(0), inputJobs, scanWorkerSelector);
        } else if (UnassignedGatherScanMultiRemoteTablesJob.canApply(scanNodes)) {
            // select * from numbers("number" = "10") a union all select * from numbers("number" = "20") b;
            // use an instance to scan table a and table b
            return new UnassignedGatherScanMultiRemoteTablesJob(planFragment, scanNodes, inputJobs);
        } else {
            return null;
        }
    }

    private UnassignedShuffleJob buildShuffleJob(
            PlanFragment planFragment, ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        return new UnassignedShuffleJob(planFragment, inputJobs);
    }

    private static ListMultimap<ExchangeNode, UnassignedJob> findInputJobs(
            FragmentLineage lineage, PlanFragmentId fragmentId, FragmentIdMapping<UnassignedJob> unassignedJobs) {
        ListMultimap<ExchangeNode, UnassignedJob> inputJobs = ArrayListMultimap.create();
        Map<PlanNodeId, ExchangeNode> exchangeNodes = lineage.parentFragmentToExchangeNode.get(fragmentId);
        if (exchangeNodes != null) {
            for (Entry<PlanNodeId, ExchangeNode> idToExchange : exchangeNodes.entrySet()) {
                PlanNodeId exchangeId = idToExchange.getKey();
                ExchangeNode exchangeNode = idToExchange.getValue();
                List<PlanFragmentId> childFragmentIds = lineage.exchangeToChildFragment.get(exchangeId);
                for (PlanFragmentId childFragmentId : childFragmentIds) {
                    inputJobs.put(exchangeNode, unassignedJobs.get(childFragmentId));
                }
            }
        }
        return inputJobs;
    }

    private static List<ExchangeNode> collectExchangeNodesInThisFragment(PlanFragment planFragment) {
        return planFragment
                .getPlanRoot()
                .collectInCurrentFragment(ExchangeNode.class::isInstance);
    }

    private static FragmentLineage buildFragmentLineage(
            FragmentIdMapping<PlanFragment> fragments) {
        ListMultimap<PlanNodeId, PlanFragmentId> exchangeToChildFragment = ArrayListMultimap.create();
        FragmentIdMapping<Map<PlanNodeId, ExchangeNode>> parentFragmentToExchangeNode = new FragmentIdMapping<>();

        for (PlanFragment fragment : fragments.values()) {
            PlanFragmentId fragmentId = fragment.getFragmentId();

            // 1. link child fragment to exchange node
            DataSink sink = fragment.getSink();
            if (sink instanceof DataStreamSink) {
                PlanNodeId exchangeNodeId = sink.getExchNodeId();
                exchangeToChildFragment.put(exchangeNodeId, fragmentId);
            } else if (sink instanceof MultiCastDataSink) {
                MultiCastDataSink multiCastDataSink = (MultiCastDataSink) sink;
                for (DataStreamSink dataStreamSink : multiCastDataSink.getDataStreamSinks()) {
                    PlanNodeId exchangeNodeId = dataStreamSink.getExchNodeId();
                    exchangeToChildFragment.put(exchangeNodeId, fragmentId);
                }
            }

            // 2. link parent fragment to exchange node
            List<ExchangeNode> exchangeNodes = collectExchangeNodesInThisFragment(fragment);
            Map<PlanNodeId, ExchangeNode> exchangeNodesInFragment = Maps.newLinkedHashMap();
            for (ExchangeNode exchangeNode : exchangeNodes) {
                exchangeNodesInFragment.put(exchangeNode.getId(), exchangeNode);
            }
            parentFragmentToExchangeNode.put(fragmentId, exchangeNodesInFragment);
        }

        return new FragmentLineage(parentFragmentToExchangeNode, exchangeToChildFragment);
    }

    private static boolean shouldAssignByBucket(PlanFragment fragment) {
        if (fragment.hasColocatePlanNode()) {
            return true;
        }
        if (enableBucketShuffleJoin() && fragment.hasBucketShuffleJoin()) {
            return true;
        }
        return false;
    }

    private static boolean enableBucketShuffleJoin() {
        if (ConnectContext.get() != null) {
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (!sessionVariable.isEnableBucketShuffleJoin() && !sessionVariable.isEnableNereidsPlanner()) {
                return false;
            }
        }
        return true;
    }

    // the class support find exchange nodes in the fragment, and find child fragment by exchange node id
    private static class FragmentLineage {
        private final FragmentIdMapping<Map<PlanNodeId, ExchangeNode>> parentFragmentToExchangeNode;
        private final ListMultimap<PlanNodeId, PlanFragmentId> exchangeToChildFragment;

        public FragmentLineage(
                FragmentIdMapping<Map<PlanNodeId, ExchangeNode>> parentFragmentToExchangeNode,
                ListMultimap<PlanNodeId, PlanFragmentId> exchangeToChildFragment) {
            this.parentFragmentToExchangeNode = parentFragmentToExchangeNode;
            this.exchangeToChildFragment = exchangeToChildFragment;
        }
    }
}
