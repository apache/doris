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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.clone.BackendLoadStatistic;
import org.apache.doris.clone.ClusterLoadStatistic;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

// show proc "/cluster_balance/cluster_load_stat";
public class ClusterLoadStatisticProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BeId").add("Cluster").add("Available").add("UsedCapacity").add("Capacity")
            .add("UsedPercent").add("ReplicaNum").add("CapCoeff").add("ReplCoeff").add("Score")
            .add("Class")
            .build();

    private Map<String, ClusterLoadStatistic> statMap;

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        statMap = Catalog.getCurrentCatalog().getTabletScheduler().getStatisticMap();

        statMap.values().stream().forEach(t -> {
            List<List<String>> statistics = t.getClusterStatistic();
            statistics.stream().forEach(v -> {
                result.addRow(v);
            });
        });

        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String beIdStr) throws AnalysisException {
        long beId = -1L;
        try {
            beId = Long.valueOf(beIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid be id format: " + beIdStr);
        }

        if (statMap == null) {
            statMap = Catalog.getCurrentCatalog().getTabletScheduler().getStatisticMap();
        }
        
        String clusterName = null;
        for (ClusterLoadStatistic clusterStat : statMap.values()) {
            BackendLoadStatistic beStat = clusterStat.getBackendLoadStatistic(beId);
            if (beStat != null) {
                clusterName = beStat.getClusterName();
            }
        }
        if (clusterName != null) {
            return new BackendLoadStatisticProcNode(statMap.get(clusterName), beId);
        }

        throw new AnalysisException("Invalid be id: " + beIdStr);
    }

}
