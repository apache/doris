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

package org.apache.doris.qe;

import mockit.Mocked;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.Planner;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatorTest extends Coordinator {
    static Planner planner = new Planner();
    static ConnectContext context = new ConnectContext(null);
    static {
        context.setQueryId(new TUniqueId(1, 2));
    }
    @Mocked
    static Catalog catalog;
    @Mocked
    static EditLog editLog;
    @Mocked
    static FrontendOptions frontendOptions;
    static Analyzer analyzer = new Analyzer(catalog, null);


    public CoordinatorTest() {
        super(context, analyzer, planner);
    }

    private static Coordinator coor;

    @Test
    public void testComputeColocateJoinInstanceParam()  {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);

        // 1. set fragmentToBucketSeqToAddress in coordinator
        Map<Integer, TNetworkAddress> bucketSeqToAddress = new HashMap<>();
        TNetworkAddress address = new TNetworkAddress();
        for (int i = 0; i < 3; i++) {
            bucketSeqToAddress.put(i, address);
        }
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentToBucketSeqToAddress = new HashMap<>();
        fragmentToBucketSeqToAddress.put(planFragmentId, bucketSeqToAddress);
        Deencapsulation.setField(coordinator, "fragmentIdToSeqToAddressMap", fragmentToBucketSeqToAddress);

        // 2. set bucketSeqToScanRange in coordinator
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> ScanRangeMap = new HashMap<>();
        ScanRangeMap.put(1, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, ScanRangeMap);
        }
        Deencapsulation.setField(coordinator, "bucketSeqToScanRange", bucketSeqToScanRange);

        FragmentExecParams params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 1, params);
        Assert.assertEquals(1, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 2, params);
        Assert.assertEquals(2, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 3, params);
        Assert.assertEquals(3, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 5, params);
        Assert.assertEquals(3, params.instanceExecParams.size());
    }
}

