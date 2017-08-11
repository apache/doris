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

package com.baidu.palo.common.proc;

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.util.DebugUtil;
import com.baidu.palo.system.Backend;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class BackendProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("RootPath").add("TotalCapacity").add("AvailableCapacity").add("State")
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
            Preconditions.checkState(infos.length == 4);

            Pair<Double, String> totalUnitPair = DebugUtil.getByteUint(Long.valueOf(infos[1]));
            Pair<Double, String> availableUnitPair = DebugUtil.getByteUint(Long.valueOf(infos[2]));

            String readableTotalCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalUnitPair.first) + " "
                    + totalUnitPair.second;
            String readableAvailableCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(availableUnitPair.first) + " "
                    + availableUnitPair.second;

            result.addRow(Lists.newArrayList(infos[0], readableTotalCapacity, readableAvailableCapacity, infos[3]));
        }

        long totalCapacityB = backend.getTotalCapacityB();
        Pair<Double, String> unitPair = DebugUtil.getByteUint(totalCapacityB);
        String readableTotalCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(unitPair.first) + " " + unitPair.second;

        long availableCapacityB = backend.getAvailableCapacityB();
        unitPair = DebugUtil.getByteUint(availableCapacityB);
        String readableAvailableCapacity = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(unitPair.first) + " "
                + unitPair.second;
        result.addRow(Lists.newArrayList("Total", readableTotalCapacity, readableAvailableCapacity, ""));

        return result;
    }

}
