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

import org.apache.doris.clone.LoadStatisticForTag;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;


public class BackendLoadStatisticProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("RootPath").add("PathHash").add("StorageMedium")
            .add("DataUsedCapacity").add("TotalCapacity").add("TotalUsedPct")
            .add("ClassInOneBE").add("ClassInAllBE").add("State")
            .build();

    private final LoadStatisticForTag statistic;
    private final long beId;

    public BackendLoadStatisticProcNode(LoadStatisticForTag statistic, long beId) {
        this.statistic = statistic;
        this.beId = beId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        if (statistic != null) {
            result.setRows(statistic.getBackendStatistic(beId));
        }
        return result;
    }
}
