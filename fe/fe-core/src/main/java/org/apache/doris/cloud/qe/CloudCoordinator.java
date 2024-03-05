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

package org.apache.doris.cloud.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CloudCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    public CloudCoordinator(ConnectContext context, Analyzer analyzer,
                            Planner planner, StatsErrorEstimator statsErrorEstimator) {
        super(context, analyzer, planner, statsErrorEstimator);
    }

    public CloudCoordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable, List<PlanFragment> fragments,
                       List<ScanNode> scanNodes, String timezone, boolean loadZeroTolerance) {
        super(jobId, queryId, descTable, fragments, scanNodes, timezone, loadZeroTolerance);
    }

    protected void prepare() {
        String cluster = null;
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            if (!Strings.isNullOrEmpty(context.getSessionVariable().getCloudCluster())) {
                cluster = context.getSessionVariable().getCloudCluster();
                try {
                    ((CloudEnv) Env.getCurrentEnv()).checkCloudClusterPriv(cluster);
                } catch (Exception e) {
                    LOG.warn("get cluster by session context exception", e);
                    return;
                }
                LOG.debug("get cluster by session context cluster: {}", cluster);
            } else {
                cluster = context.getCloudCluster();
                LOG.debug("get cluster by context {}", cluster);
            }
        } else {
            LOG.warn("connect context is null in coordinator prepare");
            return;
        }

        if (Strings.isNullOrEmpty(cluster)) {
            LOG.warn("invalid clusterName: {}", cluster);
            return;
        }

        this.idToBackend = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudIdToBackend(cluster);
        super.prepare();
    }

    protected void processFragmentAssignmentAndParams() throws Exception {
        prepare();
        if (idToBackend == null || idToBackend.isEmpty()) {
            LOG.warn("no available backends, idToBackend {}", idToBackend);
            String clusterName = ConnectContext.get() != null
                    ? ConnectContext.get().getCloudCluster() : "ctx empty cant get clusterName";
            throw new Exception("no available backends, the cluster maybe not be set or been dropped clusterName = "
                + clusterName);
        }
        computeScanRangeAssignment();
        super.computeFragmentExecParams();
    }

    protected void computeScanRangeAssignment() throws Exception {
        setVisibleVersionForOlapScanNode();
        super.computeScanRangeAssignment();
    }

    // In cloud mode, meta read lock is not enough to keep a snapshot of the partition versions.
    // After all scan node are collected, it is possible to gain a snapshot of the partition version.
    private void setVisibleVersionForOlapScanNode() throws RpcException, UserException {
        List<CloudPartition> partitions = new ArrayList<>();
        Set<Long> partitionSet = new HashSet<>();
        for (ScanNode node : scanNodes) {
            if (!(node instanceof OlapScanNode)) {
                continue;
            }

            OlapScanNode scanNode = (OlapScanNode) node;
            OlapTable table = scanNode.getOlapTable();
            for (Long id : scanNode.getSelectedPartitionIds()) {
                if (!partitionSet.contains(id)) {
                    partitionSet.add(id);
                    partitions.add((CloudPartition) table.getPartition(id));
                }
            }
        }

        if (partitions.isEmpty()) {
            return;
        }

        List<Long> versions = CloudPartition.getSnapshotVisibleVersion(partitions);
        assert versions.size() == partitions.size() : "the got num versions is not equals to acquired num versions";
        if (versions.stream().anyMatch(x -> x <= 0)) {
            int size = versions.size();
            for (int i = 0; i < size; ++i) {
                if (versions.get(i) <= 0) {
                    LOG.warn("partition {} getVisibleVersion error, the visibleVersion is {}",
                            partitions.get(i).getId(), versions.get(i));
                    throw new UserException("partition " + partitions.get(i).getId()
                        + " getVisibleVersion error, the visibleVersion is " + versions.get(i));
                }
            }
        }

        // ATTN: the table ids are ignored here because the both id are allocated from a same id generator.
        Map<Long, Long> visibleVersionMap = IntStream.range(0, versions.size())
                .boxed()
                .collect(Collectors.toMap(i -> partitions.get(i).getId(), versions::get));

        for (ScanNode node : scanNodes) {
            if (!(node instanceof OlapScanNode)) {
                continue;
            }

            OlapScanNode scanNode = (OlapScanNode) node;
            scanNode.updateScanRangeVersions(visibleVersionMap);
        }
    }
}
