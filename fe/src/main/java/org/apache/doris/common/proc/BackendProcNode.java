// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.system.Backend;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class BackendProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("RootPath").add("TotalCapacity").add("DataUsedCapacity").add("DiskAvailableCapacity").add("State")
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

        for (String infoString : backend.getDiskInfosAsString()) {
            String[] infos = infoString.split("\\|");
            Preconditions.checkState(infos.length == 5);

            Pair<Double, String> totalUnitPair = DebugUtil.getByteUint(Long.valueOf(infos[1]));
            Pair<Double, String> dataUsedUnitPair = DebugUtil.getByteUint(Long.valueOf(infos[2]));
            Pair<Double, String> diskAvailableUnitPair = DebugUtil.getByteUint(Long.valueOf(infos[3]));

            String readableTotalCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalUnitPair.first) + " "
                    + totalUnitPair.second;
            String readableDataUsedCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(dataUsedUnitPair.first) + " "
                    + dataUsedUnitPair.second;
            String readableDiskAvailableCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(
                    diskAvailableUnitPair.first) + " " + diskAvailableUnitPair.second;

            result.addRow(Lists.newArrayList(infos[0], readableTotalCapacity, readableDataUsedCapacity,
                  readableDiskAvailableCapacity, infos[4]));
        }

        long totalCapacityB = backend.getTotalCapacityB();
        Pair<Double, String> unitPair = DebugUtil.getByteUint(totalCapacityB);
        String readableTotalCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(unitPair.first) + " " + unitPair.second;

        long dataUsedCapacityB = backend.getDataUsedCapacityB();
        unitPair = DebugUtil.getByteUint(dataUsedCapacityB);
        String readableDataUsedCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(unitPair.first) + " "
                + unitPair.second;

        long diskAvailableCapacityB = backend.getAvailableCapacityB();
        unitPair = DebugUtil.getByteUint(diskAvailableCapacityB);
        String readableDiskAvailableCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(unitPair.first) + " "
                + unitPair.second;
        result.addRow(Lists.newArrayList("Total", readableTotalCapacity, readableDataUsedCapacity,
              readableDiskAvailableCapacity, ""));

        return result;
    }

}
