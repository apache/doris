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
import org.apache.doris.clone.ClusterLoadStatistic;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;

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
    private TStorageMedium medium;

    public ClusterLoadStatisticProcDir(TStorageMedium medium) {
        this.medium = medium;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        statMap = Catalog.getCurrentCatalog().getTabletScheduler().getStatisticMap();

        statMap.values().forEach(t -> {
            List<List<String>> statistics = t.getClusterStatistic(medium);
            statistics.forEach(result::addRow);
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

        Backend be = Catalog.getCurrentSystemInfo().getBackend(beId);
        if (be == null) {
            throw new AnalysisException("backend " + beId + " does not exist");
        }
        return new BackendProcNode(be);
    }

}
