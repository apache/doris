// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ClusterLoadStatisticProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BeId").add("UsedCapacity").add("Capacity").add("UsedPercent")
            .add("ReplicaNum").add("Score")
            .build();

    private ClusterLoadStatistic statistic;

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        statistic = new ClusterLoadStatistic(Catalog.getCurrentCatalog(),
                Catalog.getCurrentSystemInfo(),
                Catalog.getCurrentInvertedIndex());
        statistic.init(SystemInfoService.DEFAULT_CLUSTER);
        List<List<String>> statistics = statistic.getCLusterStatistic();
        result.setRows(statistics);
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

        if (statistic == null) {
            statistic = new ClusterLoadStatistic(Catalog.getCurrentCatalog(),
                    Catalog.getCurrentSystemInfo(),
                    Catalog.getCurrentInvertedIndex());
            statistic.init(SystemInfoService.DEFAULT_CLUSTER);
        }

        return new BackendLoadStatisticProcNode(statistic, beId);
    }

}
