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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        int scanNodeId = 1;
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        fragmentIdToScanNodeIds.put(planFragmentId, new HashSet<>());
        fragmentIdToScanNodeIds.get(planFragmentId).add(scanNodeId);
        Deencapsulation.setField(coordinator, "fragmentIdToScanNodeIds", fragmentIdToScanNodeIds);

        // 1. set fragmentToBucketSeqToAddress in coordinator
        Map<Integer, TNetworkAddress> bucketSeqToAddress = new HashMap<>();
        TNetworkAddress address = new TNetworkAddress();
        for (int i = 0; i < 3; i++) {
            bucketSeqToAddress.put(i, address);
        }
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentToBucketSeqToAddress = new HashMap<>();
        fragmentToBucketSeqToAddress.put(planFragmentId, bucketSeqToAddress);
        Deencapsulation.setField(coordinator, "fragmentIdToSeqToAddressMap", fragmentToBucketSeqToAddress);

        // 2. set bucketSeqToScanRange in coordinator
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> ScanRangeMap = new HashMap<>();
        ScanRangeMap.put(scanNodeId, new ArrayList<>());
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

    @Test
    public void testIsBucketShuffleJoin()  {
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        int scanNodeId = 1;
        // The "fragmentIdToScanNodeIds" we created here is useless in this test.
        // It is only for creating the BucketShuffleJoinController.
        // So the fragment id and scan node id in it is meaningless.
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        fragmentIdToScanNodeIds.put(planFragmentId, new HashSet<>());
        fragmentIdToScanNodeIds.get(planFragmentId).add(scanNodeId);
        Coordinator.BucketShuffleJoinController bucketShuffleJoinController
                = new Coordinator.BucketShuffleJoinController(fragmentIdToScanNodeIds);

        PlanNodeId testPlanNodeId = new PlanNodeId(-1);
        TupleId testTupleId = new TupleId(-1);
        ArrayList<TupleId> tupleIdArrayList = new ArrayList<>();
        tupleIdArrayList.add(testTupleId);

        ArrayList<Expr> testJoinexprs = new ArrayList<>();
        BinaryPredicate binaryPredicate = new BinaryPredicate();
        testJoinexprs.add(binaryPredicate);

        HashJoinNode hashJoinNode = new HashJoinNode(testPlanNodeId, new EmptySetNode(testPlanNodeId, tupleIdArrayList),
                new EmptySetNode(testPlanNodeId, tupleIdArrayList), new TableRef(), testJoinexprs, new ArrayList<>());
        hashJoinNode.setFragment(new PlanFragment(new PlanFragmentId(-1), hashJoinNode,
                new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, testJoinexprs)));

        // hash join node is not bucket shuffle join
        Assert.assertEquals(false,
                Deencapsulation.invoke(bucketShuffleJoinController, "isBucketShuffleJoin", -1, hashJoinNode));

        // the fragment id is different from hash join node
        hashJoinNode.setFragment(new PlanFragment(new PlanFragmentId(-2), hashJoinNode,
                new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, testJoinexprs)));
        hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.BUCKET_SHUFFLE);
        Assert.assertEquals(false,
                Deencapsulation.invoke(bucketShuffleJoinController, "isBucketShuffleJoin", -1, hashJoinNode));

        hashJoinNode.setFragment(new PlanFragment(new PlanFragmentId(-1), hashJoinNode,
                new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, testJoinexprs)));
        Assert.assertEquals(true,
                Deencapsulation.invoke(bucketShuffleJoinController, "isBucketShuffleJoin", -1, hashJoinNode));

        // the fragment id is in cache, so not do check node again
        Assert.assertEquals(true,
                Deencapsulation.invoke(bucketShuffleJoinController, "isBucketShuffleJoin", -1));

    }

    @Test
    public void testComputeScanRangeAssignmentByBucketq()  {
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        int scanNodeId = 1;
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        fragmentIdToScanNodeIds.put(planFragmentId, new HashSet<>());
        fragmentIdToScanNodeIds.get(planFragmentId).add(scanNodeId);
        Coordinator.BucketShuffleJoinController bucketShuffleJoinController
                = new Coordinator.BucketShuffleJoinController(fragmentIdToScanNodeIds);

        // init olap scan node of bucket shuffle join
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        OlapTable olapTable = new OlapTable();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(66, new ArrayList<>());
        Deencapsulation.setField(olapTable, "defaultDistributionInfo", hashDistributionInfo);
        tupleDescriptor.setTable(olapTable);

        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(scanNodeId), tupleDescriptor, "test");
        ArrayListMultimap<Integer, TScanRangeLocations> bucketseq2localtion = ArrayListMultimap.create();

        // each olaptable bucket have the same TScanRangeLocations, be id is {0, 1, 2}
        TScanRangeLocations tScanRangeLocations = new TScanRangeLocations();
        TScanRangeLocation tScanRangeLocation0 = new TScanRangeLocation();
        tScanRangeLocation0.backend_id = 0;
        TScanRangeLocation tScanRangeLocation1 = new TScanRangeLocation();
        tScanRangeLocation1.backend_id = 1;
        TScanRangeLocation tScanRangeLocation2 = new TScanRangeLocation();
        tScanRangeLocation2.backend_id = 2;

        tScanRangeLocations.locations = new ArrayList<>();
        tScanRangeLocations.locations.add(tScanRangeLocation0);
        tScanRangeLocations.locations.add(tScanRangeLocation1);
        tScanRangeLocations.locations.add(tScanRangeLocation2);
        for (int i = 0; i < 66; i++) {
            bucketseq2localtion.put(i, tScanRangeLocations);
        }

        Deencapsulation.setField(olapScanNode, "bucketSeq2locations", bucketseq2localtion);
        olapScanNode.setFragment(new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED)));

        // init all backend
        Backend backend0 = new Backend();
        backend0.setAlive(true);
        Backend backend1 = new Backend();
        backend1.setAlive(true);
        Backend backend2 = new Backend();
        backend2.setAlive(true);

        // init all be network address
        TNetworkAddress be0 = new TNetworkAddress("0.0.0.0", 1000);
        TNetworkAddress be1 = new TNetworkAddress("0.0.0.1", 2000);
        TNetworkAddress be2 = new TNetworkAddress("0.0.0.2", 3000);

        HashMap<Long, Backend> idToBackend = new HashMap<>();
        idToBackend.put(0l, backend0);
        idToBackend.put(1l, backend1);
        idToBackend.put(2l, backend2);

        Map<TNetworkAddress, Long> addressToBackendID = new HashMap<>();
        addressToBackendID.put(be0, 0l);
        addressToBackendID.put(be1, 1l);
        addressToBackendID.put(be2, 2l);

        Deencapsulation.invoke(bucketShuffleJoinController, "computeScanRangeAssignmentByBucket",
                olapScanNode, ImmutableMap.copyOf(idToBackend), addressToBackendID);

        Assert.assertEquals(java.util.Optional.of(66).get(),
                Deencapsulation.invoke(bucketShuffleJoinController, "getFragmentBucketNum", new PlanFragmentId(1)));

        Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBuckendIdBucketCountMap =
                Deencapsulation.getField(bucketShuffleJoinController, "fragmentIdToBuckendIdBucketCountMap");

        long targetBeCount = fragmentIdToBuckendIdBucketCountMap.values().
                stream().flatMap(buckend2BucketCountMap -> buckend2BucketCountMap.values().stream())
                .filter(count -> count == 22).count();
        Assert.assertEquals(targetBeCount, 3);
    }

    @Test
    public void testComputeScanRangeAssignmentByBucket()  {
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        int scanNodeId = 1;
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        fragmentIdToScanNodeIds.put(planFragmentId, new HashSet<>());
        fragmentIdToScanNodeIds.get(planFragmentId).add(scanNodeId);
        Coordinator.BucketShuffleJoinController bucketShuffleJoinController
                = new Coordinator.BucketShuffleJoinController(fragmentIdToScanNodeIds);

        // init olap scan node of bucket shuffle join
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        OlapTable olapTable = new OlapTable();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(66, new ArrayList<>());
        Deencapsulation.setField(olapTable, "defaultDistributionInfo", hashDistributionInfo);
        tupleDescriptor.setTable(olapTable);

        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(scanNodeId), tupleDescriptor, "test");
        ArrayListMultimap<Integer, TScanRangeLocations> bucketseq2localtion = ArrayListMultimap.create();

        // each olaptable bucket have the same TScanRangeLocations, be id is {0, 1, 2}
        TScanRangeLocations tScanRangeLocations = new TScanRangeLocations();
        TScanRangeLocation tScanRangeLocation0 = new TScanRangeLocation();
        tScanRangeLocation0.backend_id = 0;
        TScanRangeLocation tScanRangeLocation1 = new TScanRangeLocation();
        tScanRangeLocation1.backend_id = 1;
        TScanRangeLocation tScanRangeLocation2 = new TScanRangeLocation();
        tScanRangeLocation2.backend_id = 2;

        tScanRangeLocations.locations = new ArrayList<>();
        tScanRangeLocations.locations.add(tScanRangeLocation0);
        tScanRangeLocations.locations.add(tScanRangeLocation1);
        tScanRangeLocations.locations.add(tScanRangeLocation2);
        for (int i = 0; i < 66; i++) {
            bucketseq2localtion.put(i, tScanRangeLocations);
        }

        Deencapsulation.setField(olapScanNode, "bucketSeq2locations", bucketseq2localtion);
        olapScanNode.setFragment(new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED)));


        // init all backend
        Backend backend0 = new Backend();
        backend0.setAlive(true);
        Backend backend1 = new Backend();
        backend1.setAlive(true);
        Backend backend2 = new Backend();
        backend2.setAlive(true);

        // init all be network address
        TNetworkAddress be0 = new TNetworkAddress("0.0.0.0", 1000);
        TNetworkAddress be1 = new TNetworkAddress("0.0.0.1", 2000);
        TNetworkAddress be2 = new TNetworkAddress("0.0.0.2", 3000);

        HashMap<Long, Backend> idToBackend = new HashMap<>();
        idToBackend.put(0l, backend0);
        idToBackend.put(1l, backend1);
        idToBackend.put(2l, backend2);

        Map<TNetworkAddress, Long> addressToBackendID = new HashMap<>();
        addressToBackendID.put(be0, 0l);
        addressToBackendID.put(be1, 1l);
        addressToBackendID.put(be2, 2l);

        Deencapsulation.invoke(bucketShuffleJoinController, "computeScanRangeAssignmentByBucket",
                olapScanNode, ImmutableMap.copyOf(idToBackend), addressToBackendID);

        Assert.assertEquals(java.util.Optional.of(66).get(),
                Deencapsulation.invoke(bucketShuffleJoinController, "getFragmentBucketNum", new PlanFragmentId(1)));

        Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBuckendIdBucketCountMap =
                Deencapsulation.getField(bucketShuffleJoinController, "fragmentIdToBuckendIdBucketCountMap");

        long targetBeCount = fragmentIdToBuckendIdBucketCountMap.values()
                .stream()
                .flatMap(buckend2BucketCountMap -> buckend2BucketCountMap.values().stream())
                .filter(count -> count == 22).count();
        Assert.assertEquals(targetBeCount, 3);
    }

    @Test
    public void testComputeBucketShuffleJoinInstanceParam()  {
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        int scanNodeId = 1;

        // set fragment id to scan node ids map
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        fragmentIdToScanNodeIds.put(planFragmentId, new HashSet<>());
        fragmentIdToScanNodeIds.get(planFragmentId).add(scanNodeId);
        Coordinator.BucketShuffleJoinController bucketShuffleJoinController
                = new Coordinator.BucketShuffleJoinController(fragmentIdToScanNodeIds);

        // 1. set fragmentToBucketSeqToAddress in bucketShuffleJoinController
        Map<Integer, TNetworkAddress> bucketSeqToAddress = new HashMap<>();
        TNetworkAddress address = new TNetworkAddress();
        for (int i = 0; i < 3; i++) {
            bucketSeqToAddress.put(i, address);
        }
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentToBucketSeqToAddress = new HashMap<>();
        fragmentToBucketSeqToAddress.put(planFragmentId, bucketSeqToAddress);
        Deencapsulation.setField(bucketShuffleJoinController, "fragmentIdToSeqToAddressMap", fragmentToBucketSeqToAddress);

        // 2. set bucketSeqToScanRange in bucketShuffleJoinController
        Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = new HashMap<>();
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> ScanRangeMap = new HashMap<>();
        ScanRangeMap.put(scanNodeId, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, ScanRangeMap);
        }
        fragmentIdBucketSeqToScanRangeMap.put(planFragmentId, bucketSeqToScanRange);
        Deencapsulation.setField(bucketShuffleJoinController, "fragmentIdBucketSeqToScanRangeMap", fragmentIdBucketSeqToScanRangeMap);


        FragmentExecParams params = new FragmentExecParams(null);
        Deencapsulation.invoke(bucketShuffleJoinController, "computeInstanceParam", planFragmentId, 1, params);
        Assert.assertEquals(1, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(bucketShuffleJoinController, "computeInstanceParam", planFragmentId, 2, params);
        Assert.assertEquals(2, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(bucketShuffleJoinController, "computeInstanceParam", planFragmentId, 3, params);
        Assert.assertEquals(3, params.instanceExecParams.size());

        params = new FragmentExecParams(null);
        Deencapsulation.invoke(bucketShuffleJoinController, "computeInstanceParam", planFragmentId, 5, params);
        Assert.assertEquals(3, params.instanceExecParams.size());
    }

    @Test
    public void testComputeScanRangeAssignmentByScheduler()  {
        Coordinator coordinator = new Coordinator(context, analyzer, planner);
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        int scanNodeId = 1;
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        fragmentIdToScanNodeIds.put(planFragmentId, new HashSet<>());
        fragmentIdToScanNodeIds.get(planFragmentId).add(scanNodeId);

        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        OlapTable olapTable = new OlapTable();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(66, new ArrayList<>());
        Deencapsulation.setField(olapTable, "defaultDistributionInfo", hashDistributionInfo);
        tupleDescriptor.setTable(olapTable);

        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(scanNodeId), tupleDescriptor, "test");
        // each olaptable bucket have the same TScanRangeLocations, be id is {0, 1, 2}
        TScanRangeLocations tScanRangeLocations = new TScanRangeLocations();
        TScanRangeLocation tScanRangeLocation0 = new TScanRangeLocation();
        tScanRangeLocation0.backend_id = 0;
        tScanRangeLocation0.server = new TNetworkAddress("0.0.0.0", 9050);
        TScanRangeLocation tScanRangeLocation1 = new TScanRangeLocation();
        tScanRangeLocation1.backend_id = 1;
        tScanRangeLocation1.server = new TNetworkAddress("0.0.0.1", 9050);
        TScanRangeLocation tScanRangeLocation2 = new TScanRangeLocation();
        tScanRangeLocation2.backend_id = 2;
        tScanRangeLocation2.server = new TNetworkAddress("0.0.0.2", 9050);
        tScanRangeLocations.locations = new ArrayList<>();
        tScanRangeLocations.locations.add(tScanRangeLocation0);
        tScanRangeLocations.locations.add(tScanRangeLocation1);
        tScanRangeLocations.locations.add(tScanRangeLocation2);

        TScanRangeLocations tScanRangeLocations1 = new TScanRangeLocations();
        TScanRangeLocation tScanRangeLocation3 = new TScanRangeLocation();
        tScanRangeLocation3.backend_id = 0;
        tScanRangeLocation3.server = new TNetworkAddress("0.0.0.0", 9050);
        TScanRangeLocation tScanRangeLocation4 = new TScanRangeLocation();
        tScanRangeLocation4.backend_id = 1;
        tScanRangeLocation4.server = new TNetworkAddress("0.0.0.1", 9050);
        TScanRangeLocation tScanRangeLocation5 = new TScanRangeLocation();
        tScanRangeLocation5.backend_id = 2;
        tScanRangeLocation5.server = new TNetworkAddress("0.0.0.2", 9050);
        tScanRangeLocations1.locations = new ArrayList<>();
        tScanRangeLocations1.locations.add(tScanRangeLocation3);
        tScanRangeLocations1.locations.add(tScanRangeLocation4);
        tScanRangeLocations1.locations.add(tScanRangeLocation5);

        olapScanNode.setFragment(new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED)));

        // init all backend
        Backend backend0 = new Backend(0, "0.0.0.0", 9060);
        backend0.setAlive(false);
        backend0.setBePort(9050);
        Backend backend1 = new Backend(1, "0.0.0.1", 9060);
        backend1.setAlive(true);
        backend1.setBePort(9050);
        Backend backend2 = new Backend(2, "0.0.0.2", 9060);
        backend2.setAlive(true);
        backend2.setBePort(9050);

        ImmutableMap<Long, Backend> idToBackend =
                new ImmutableMap.Builder<Long, Backend>().
                put(0l, backend0).
                put(1l, backend1).
                put(2l, backend2).build();
        Deencapsulation.setField(coordinator, "idToBackend", idToBackend);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        List<TScanRangeLocations> locations = new ArrayList<>();
        locations.add(tScanRangeLocations);
        locations.add(tScanRangeLocations1);
        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByScheduler",
                olapScanNode, locations, assignment);
        for (Map.Entry entry:assignment.entrySet()) {
            Map<Integer, List<TScanRangeParams>> addr = (HashMap<Integer, List<TScanRangeParams>>) entry.getValue();
            for (Map.Entry item:addr.entrySet()) {
                List<TScanRangeParams> params = (List<TScanRangeParams>) item.getValue();
                Assert.assertTrue(params.size() == 2);
            }
        }
    }
}

