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

package org.apache.doris.clone;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.system.SystemInfoService;

import java.util.List;

/*
 * LoadBalancer run at a fix interval.
 * Each run will re-calculate the load score of all backends
 */
public class LoadBalancer extends Daemon {

    private ClusterLoadStatistic clusterLoadStatistic;

    public LoadBalancer() {
        super("load balancer", 60 * 1000);
        clusterLoadStatistic = new ClusterLoadStatistic(Catalog.getInstance(),
                Catalog.getCurrentSystemInfo(),
                Catalog.getCurrentInvertedIndex());
    }

    @Override
    protected void runOneCycle() {
        clusterLoadStatistic.init(SystemInfoService.DEFAULT_CLUSTER);
    }

    public BackendLoadStatistic getBackendStatistic(long beId) {
        return clusterLoadStatistic.getBackendLoadStatistic(beId);
    }

    public List<List<String>> getClusterStatisticInfo() {
        return clusterLoadStatistic.getCLusterStatistic();
    }

    public List<List<String>> getBackendStatisticInfo(long beId) {
        return clusterLoadStatistic.getBackendStatistic(beId);
    }
}
