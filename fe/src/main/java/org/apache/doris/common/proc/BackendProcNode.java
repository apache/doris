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

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.system.Backend;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class BackendProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("RootPath").add("DataUsedCapacity").add("AvailCapacity").add("TotalCapacity")
            .add("UsedPct").add("State").add("PathHash")
            .build();

    private Backend backend;

    public BackendProcNode(Backend backend) {
        this.backend = backend;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(backend);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);

        for (Map.Entry<String, DiskInfo> entry : backend.getDisks().entrySet()) {
            List<String> info = Lists.newArrayList();
            info.add(entry.getKey());
            
            // data used
            long dataUsedB = entry.getValue().getDataUsedCapacityB();
            Pair<Double, String> dataUsedUnitPair = DebugUtil.getByteUint(dataUsedB);
            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(dataUsedUnitPair.first) + " "
                    + dataUsedUnitPair.second);
            
            // avail
            long availB = entry.getValue().getAvailableCapacityB();
            Pair<Double, String> availUnitPair = DebugUtil.getByteUint(availB);
            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(availUnitPair.first) + " " + availUnitPair.second);
            
            // total
            long totalB = entry.getValue().getTotalCapacityB();
            Pair<Double, String> totalUnitPair = DebugUtil.getByteUint(totalB);
            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalUnitPair.first) + " "  + totalUnitPair.second);
           
            // used percent
            double used = 0.0;
            if (totalB <= 0) {
                used = 0.0;
            } else {
                used = (double) (totalB - availB) * 100 / totalB;
            }
            info.add(String.format("%.2f", used) + " %");

            result.addRow(info);
        }

        return result;
    }

}
