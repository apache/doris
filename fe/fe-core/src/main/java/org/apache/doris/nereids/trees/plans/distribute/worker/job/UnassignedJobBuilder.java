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

import org.apache.doris.nereids.StatementContext;
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
import org.apache.doris.thrift.TExplainLevel;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.Iterator;
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
    public static FragmentIdMapping<UnassignedJob> buildJobs(
            StatementContext statementContext, FragmentIdMapping<PlanFragment> fragments) {
        UnassignedJobBuilder builder = new UnassignedJobBuilder();

        FragmentLineage fragmentLineage = buildFragmentLineage(fragments);
        FragmentIdMapping<UnassignedJob> unassignedJobs = new FragmentIdMapping<>();

        // build from leaf to parent
        Iterator<Entry<PlanFragmentId, PlanFragment>> iterator = fragments.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<PlanFragmentId, PlanFragment> kv = iterator.next();
            boolean isTopFragment = !iterator.hasNext();

            PlanFragmentId fragmentId = kv.getKey();
            PlanFragment fragment = kv.getValue();

            ListMultimap<ExchangeNode, UnassignedJob> inputJobs = findInputJobs(
                    fragmentLineage, fragmentId, unassignedJobs);
            UnassignedJob unassignedJob = builder.buildJob(statementContext, fragment, inputJobs, isTopFragment);
            unassignedJobs.put(fragmentId, unassignedJob);
        }

        return unassignedJobs;
    }

    private UnassignedJob buildJob(
            StatementContext statementContext, PlanFragment planFragment,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs, boolean isTopFragment) {
        List<ScanNode> scanNodes = collectScanNodesInThisFragment(planFragment);
        if (planFragment.specifyInstances.isPresent()) {
            return buildSpecifyInstancesJob(statementContext, planFragment, scanNodes, inputJobs);
        } else if (scanNodes.isEmpty() && isTopFragment
                && statementContext.getGroupCommitMergeBackend() != null) {
            return new UnassignedGroupCommitJob(statementContext, planFragment, scanNodes, inputJobs);
        } else if (!scanNodes.isEmpty() || isLeafFragment(planFragment)) {
            return buildLeafOrScanJob(statementContext, planFragment, scanNodes, inputJobs);
        } else {
            return buildShuffleJob(statementContext, planFragment, inputJobs);
        }
    }

    private UnassignedJob buildLeafOrScanJob(
            StatementContext statementContext, PlanFragment planFragment, List<ScanNode> scanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        int olapScanNodeNum = olapScanNodeNum(scanNodes);

        UnassignedJob unassignedJob = null;
        if (!scanNodes.isEmpty() && olapScanNodeNum == scanNodes.size()) {
            // we need assign a backend which contains the data,
            // so that the OlapScanNode can find the data in the backend
            // e.g. select * from olap_table
            unassignedJob = buildScanOlapTableJob(
                    statementContext, planFragment, (List) scanNodes, inputJobs, scanWorkerSelector
            );
        } else if (scanNodes.isEmpty()) {
            // select constant without table,
            // e.g. select 100 union select 200
            unassignedJob = buildQueryConstantJob(statementContext, planFragment);
        } else if (olapScanNodeNum == 0) {
            ScanNode scanNode = scanNodes.get(0);
            if (scanNode instanceof SchemaScanNode) {
                // select * from information_schema.tables
                unassignedJob = buildScanMetadataJob(
                        statementContext, planFragment, (SchemaScanNode) scanNode, scanWorkerSelector
                );
            } else {
                // only scan external tables or cloud tables or table valued functions
                // e,g. select * from numbers('number'='100')
                unassignedJob = buildScanRemoteTableJob(
                        statementContext, planFragment, scanNodes, inputJobs, scanWorkerSelector
                );
            }
        }

        if (unassignedJob != null) {
            return unassignedJob;
        }

        throw new IllegalStateException("Cannot generate unassignedJob for fragment"
                + " has both OlapScanNode and Other ScanNode: "
                + planFragment.getExplainString(TExplainLevel.VERBOSE));
    }

    private UnassignedJob buildSpecifyInstancesJob(
            StatementContext statementContext, PlanFragment planFragment,
            List<ScanNode> scanNodes, ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        return new UnassignedSpecifyInstancesJob(statementContext, planFragment, scanNodes, inputJobs);
    }

    private UnassignedJob buildScanOlapTableJob(
            StatementContext statementContext, PlanFragment planFragment, List<OlapScanNode> olapScanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs,
            ScanWorkerSelector scanWorkerSelector) {
        if (shouldAssignByBucket(planFragment)) {
            return new UnassignedScanBucketOlapTableJob(
                    statementContext, planFragment, olapScanNodes, inputJobs, scanWorkerSelector);
        } else if (olapScanNodes.size() == 1) {
            return new UnassignedScanSingleOlapTableJob(
                    statementContext, planFragment, olapScanNodes.get(0), inputJobs, scanWorkerSelector);
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

    private UnassignedQueryConstantJob buildQueryConstantJob(
            StatementContext statementContext, PlanFragment planFragment) {
        return new UnassignedQueryConstantJob(statementContext, planFragment);
    }

    private UnassignedJob buildScanMetadataJob(
            StatementContext statementContext, PlanFragment fragment,
            SchemaScanNode schemaScanNode, ScanWorkerSelector scanWorkerSelector) {
        return new UnassignedScanMetadataJob(statementContext, fragment, schemaScanNode, scanWorkerSelector);
    }

    private UnassignedJob buildScanRemoteTableJob(
            StatementContext statementContext, PlanFragment planFragment, List<ScanNode> scanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs,
            ScanWorkerSelector scanWorkerSelector) {
        if (scanNodes.size() == 1) {
            return new UnassignedScanSingleRemoteTableJob(
                    statementContext, planFragment, scanNodes.get(0), inputJobs, scanWorkerSelector);
        } else if (UnassignedGatherScanMultiRemoteTablesJob.canApply(scanNodes)) {
            // select * from numbers("number" = "10") a union all select * from numbers("number" = "20") b;
            // use an instance to scan table a and table b
            return new UnassignedGatherScanMultiRemoteTablesJob(statementContext, planFragment, scanNodes, inputJobs);
        } else {
            return null;
        }
    }

    private UnassignedJob buildShuffleJob(
            StatementContext statementContext, PlanFragment planFragment,
            ListMultimap<ExchangeNode, UnassignedJob> inputJobs) {
        if (planFragment.isPartitioned()) {
            return new UnassignedShuffleJob(statementContext, planFragment, inputJobs);
        } else {
            return new UnassignedGatherJob(statementContext, planFragment, inputJobs);
        }
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
        if (fragment.hasBucketShuffleJoin()) {
            return true;
        }
        return false;
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
