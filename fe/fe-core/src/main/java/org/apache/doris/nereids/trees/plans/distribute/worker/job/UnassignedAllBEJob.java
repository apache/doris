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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.planner.DictionarySink;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/** UnassignedAllBEJob */
public class UnassignedAllBEJob extends AbstractUnassignedJob {
    private static final Logger LOG = LogManager.getLogger(UnassignedAllBEJob.class);

    public UnassignedAllBEJob(StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToUpstreamJob) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToUpstreamJob);
    }

    // ExchangeNode -> upstreamFragment -> AssignedJob(instances of upstreamFragment)
    @Override
    public List<AssignedJob> computeAssignedJobs(DistributeContext distributeContext,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        ConnectContext connectContext = statementContext.getConnectContext();
        DistributedPlanWorkerManager workerManager = distributeContext.workerManager;

        DictionarySink sink = (DictionarySink) fragment.getSink();
        // it may be ScanNode or optimized to EmptySetNode. use universay function to get the deepest source.
        PlanNode rootNode = fragment.getDeepestLinearSource();
        List<Backend> bes;
        if (sink.allowAdaptiveLoad() && rootNode instanceof OlapScanNode) {
            Dictionary dictionary = sink.getDictionary();
            long lastVersion = dictionary.getSrcVersion();
            long usingVersion = ((OlapScanNode) rootNode).getMaxVersion();
            if (LOG.isDebugEnabled()) {
                LOG.debug("UnassignedAllBEJob check load: dictionary={}, lastVersion={}, usingVersion={}",
                        dictionary.getName(), lastVersion, usingVersion);
            }
            if (usingVersion > lastVersion) {
                // load new data
                bes = computeFullLoad(workerManager, inputJobs);
            } else {
                // try to load only for the BEs which is outdated
                bes = computePartiallLoad(workerManager, inputJobs, dictionary, sink);
                statementContext.setPartialLoadDictionary(true);
            }
        } else {
            // we explicitly request all BEs to load data. or ExternalTable. (or EmptySetNode - should not happen)
            bes = computeFullLoad(workerManager, inputJobs);
        }

        List<AssignedJob> assignedJobs = Lists.newArrayList();
        for (int i = 0; i < bes.size(); ++i) {
            // every time one BE is selected
            DistributedPlanWorker worker = workerManager.getWorker(bes.get(i));
            if (worker != null) {
                assignedJobs.add(assignWorkerAndDataSources(i, connectContext.nextInstanceId(), worker,
                        new DefaultScanSource(ImmutableMap.of())));
            } else {
                throw new IllegalArgumentException(
                        "worker " + bes.get(i).getAddress() + " not found");
            }
        }

        if (rootNode instanceof OlapScanNode) {
            // set the version of source table we are going to load
            statementContext.setDictionaryUsedSrcVersion(((OlapScanNode) rootNode).getMaxVersion());
        } else if (rootNode instanceof EmptySetNode) {
            // this will make always reload. but we think this shouldn't happen now.
            LOG.warn("EmptySetNode should not be used in DictionarySink");
            statementContext.setDictionaryUsedSrcVersion(0);
        } else { // external table
            // we've checked before construct the load
            ExternalScanNode node = (ExternalScanNode) rootNode;
            MTMVRelatedTableIf table = (MTMVRelatedTableIf) node.getTableIf();
            try {
                MTMVSnapshotIf snapshot = table.getTableSnapshot(MvccUtil.getSnapshotFromContext(table));
                statementContext.setDictionaryUsedSrcVersion(snapshot.getSnapshotVersion());
            } catch (AnalysisException e) {
                throw new IllegalArgumentException("getTableSnapshot failed: " + e.getMessage());
            }
        }
        statementContext.setUsedBackendsDistributing(bes);
        return assignedJobs;
    }

    private List<Backend> computeFullLoad(DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        // input jobs from upstream fragment - may have many instances.
        ExchangeNode exchange = inputJobs.keySet().iterator().next(); // random one - should be same for any exchange.
        int expectInstanceNum = exchange.getNumInstances();

        // for Coordinator to know the right parallelism of DictionarySink
        exchange.getFragment().setParallelExecNum(expectInstanceNum);

        List<Backend> bes = workerManager.getAllBackends(true);
        if (bes.size() != expectInstanceNum) {
            // BE number changed when planning
            throw new IllegalArgumentException("BE number should be " + expectInstanceNum + ", but is " + bes.size());
        }
        return bes;
    }

    private List<Backend> computePartiallLoad(DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs, Dictionary dictionary, DictionarySink sink) {
        // dictionary's src version(bundled with dictionary's version) is same with usingVersion(otherwise FullLoad)
        // so we can just use the src version to find the outdated backends
        List<Backend> outdateBEs = dictionary.filterOutdatedBEs(workerManager.getAllBackends(true));

        // reset all exchange node's instance number to the number of outdated backends
        PlanFragment fragment = inputJobs.keySet().iterator().next().getFragment(); // random one exchange
        for (ExchangeNode exchange : inputJobs.keySet()) {
            exchange.setNumInstances(outdateBEs.size());
        }
        // for Coordinator to know the right parallelism and BEs of DictionarySink
        sink.setPartialLoadBEs(outdateBEs);
        fragment.setParallelExecNum(outdateBEs.size());

        return outdateBEs;
    }
}
