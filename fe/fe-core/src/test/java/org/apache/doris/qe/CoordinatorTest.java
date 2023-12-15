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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.resource.Tag;
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
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Mocked;
import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class CoordinatorTest extends Coordinator {
    @Mocked
    static Env env;
    @Mocked
    static EditLog editLog;
    @Mocked
    static FrontendOptions frontendOptions;

    static ConnectContext context = new ConnectContext();
    static Analyzer analyzer = new Analyzer(env, context);
    static OriginalPlanner originalPlanner = new OriginalPlanner(analyzer);

    static {
        context.setQueryId(new TUniqueId(1, 2));
        context.setQualifiedUser("root");
    }

    public CoordinatorTest() {
        super(context, analyzer, originalPlanner);
    }

    private static Coordinator coor;

    @Test
    public void testComputeColocateJoinInstanceParam()  {
        Coordinator coordinator = new Coordinator(context, analyzer, originalPlanner);

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
        Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = new HashMap<>();
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> scanRangeMap = new HashMap<>();
        List<TScanRangeParams> scanRangeParamsList = new ArrayList<>();
        scanRangeParamsList.add(new TScanRangeParams());

        scanRangeMap.put(scanNodeId, scanRangeParamsList);
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, scanRangeMap);
        }
        fragmentIdBucketSeqToScanRangeMap.put(planFragmentId, bucketSeqToScanRange);
        Deencapsulation.setField(coordinator, "fragmentIdTobucketSeqToScanRangeMap", fragmentIdBucketSeqToScanRangeMap);

        FragmentExecParams params = new FragmentExecParams(null);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 1, params);
        Assert.assertEquals(1, params.instanceExecParams.size());

        // check whether one instance have 3 tablet to scan
        for (FInstanceExecParam instanceExecParam : params.instanceExecParams) {
            for (List<TScanRangeParams> tempScanRangeParamsList : instanceExecParam.perNodeScanRanges.values()) {
                Assert.assertEquals(3, tempScanRangeParamsList.size());
            }
        }

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

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, new BoolLiteral(true),
                        new BoolLiteral(true));
        testJoinexprs.add(binaryPredicate);

        HashJoinNode hashJoinNode = new HashJoinNode(testPlanNodeId, new EmptySetNode(testPlanNodeId, tupleIdArrayList),
                        new EmptySetNode(testPlanNodeId, tupleIdArrayList), new TableRef(), testJoinexprs,
                        new ArrayList<>());

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
        // init all be network address
        TNetworkAddress be0 = new TNetworkAddress("0.0.0.0", 1000);
        TNetworkAddress be1 = new TNetworkAddress("0.0.0.1", 2000);
        TNetworkAddress be2 = new TNetworkAddress("0.0.0.2", 3000);

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
        tScanRangeLocation0.server = be0;
        TScanRangeLocation tScanRangeLocation1 = new TScanRangeLocation();
        tScanRangeLocation1.backend_id = 1;
        tScanRangeLocation1.server = be1;
        TScanRangeLocation tScanRangeLocation2 = new TScanRangeLocation();
        tScanRangeLocation2.backend_id = 2;
        tScanRangeLocation2.server = be2;

        tScanRangeLocations.locations = new ArrayList<>();
        tScanRangeLocations.locations.add(tScanRangeLocation0);
        tScanRangeLocations.locations.add(tScanRangeLocation1);
        tScanRangeLocations.locations.add(tScanRangeLocation2);
        for (int i = 0; i < 66; i++) {
            bucketseq2localtion.put(i, tScanRangeLocations);
        }

        Deencapsulation.setField(olapScanNode, "bucketSeq2locations", bucketseq2localtion);
        Deencapsulation.setField(olapScanNode, "totalTabletsNum", 66);
        olapScanNode.setFragment(new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED)));

        // init all backend
        Backend backend0 = new Backend();
        backend0.setAlive(true);
        Backend backend1 = new Backend();
        backend1.setAlive(true);
        Backend backend2 = new Backend();
        backend2.setAlive(true);

        HashMap<Long, Backend> idToBackend = new HashMap<>();
        idToBackend.put(0L, backend0);
        idToBackend.put(1L, backend1);
        idToBackend.put(2L, backend2);

        Map<TNetworkAddress, Long> addressToBackendID = new HashMap<>();
        addressToBackendID.put(be0, 0L);
        addressToBackendID.put(be1, 1L);
        addressToBackendID.put(be2, 2L);

        Map<TNetworkAddress, Long> replicaNumPerHost = Maps.newHashMap();
        replicaNumPerHost.put(be0, 66L);
        replicaNumPerHost.put(be1, 66L);
        replicaNumPerHost.put(be2, 66L);

        Deencapsulation.invoke(bucketShuffleJoinController, "computeScanRangeAssignmentByBucket",
                olapScanNode, ImmutableMap.copyOf(idToBackend), addressToBackendID, replicaNumPerHost);

        Assert.assertEquals(java.util.Optional.of(66).get(),
                Deencapsulation.invoke(bucketShuffleJoinController, "getFragmentBucketNum", new PlanFragmentId(1)));

        Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBuckendIdBucketCountMap =
                Deencapsulation.getField(bucketShuffleJoinController, "fragmentIdToBuckendIdBucketCountMap");

        long targetBeCount = fragmentIdToBuckendIdBucketCountMap.values()
                .stream().flatMap(buckend2BucketCountMap -> buckend2BucketCountMap.values().stream())
                .filter(count -> count == 22).count();
        Assert.assertEquals(targetBeCount, 3);
    }

    @Test
    public void testComputeScanRangeAssignmentByColocate()  {
        Coordinator coordinator = new Coordinator(context, analyzer, originalPlanner);
        int bucketNum = 5;
        int replicaNum = 3;
        int backendNum = 10;

        // init left scan node
        TupleDescriptor leftTuple = new TupleDescriptor(new TupleId(-1));
        OlapTable leftTable = new OlapTable();
        HashDistributionInfo distributionInfoLeft = new HashDistributionInfo(bucketNum, Lists.newArrayList(new Column("k1", PrimitiveType.INT)));
        Deencapsulation.setField(leftTable, "defaultDistributionInfo", distributionInfoLeft);
        leftTuple.setTable(leftTable);
        OlapScanNode leftScanNode = new OlapScanNode(new PlanNodeId(1), leftTuple, "left");

        // init right scan node
        TupleDescriptor rightTuple = new TupleDescriptor(new TupleId(-2));
        OlapTable rightTable = new OlapTable();
        HashDistributionInfo distributionInfoRight = new HashDistributionInfo(bucketNum, Lists.newArrayList(new Column("k1", PrimitiveType.INT)));
        Deencapsulation.setField(rightTable, "defaultDistributionInfo", distributionInfoRight);
        rightTuple.setTable(rightTable);
        OlapScanNode rightScanNode = new OlapScanNode(new PlanNodeId(2), rightTuple, "right");

        // init hash join node
        BinaryPredicate predicate = new BinaryPredicate(Operator.EQ, new BoolLiteral(true), new BoolLiteral(true));
        List<Expr> joinExpr = Lists.newArrayList(predicate);
        HashJoinNode hashJoinNode = new HashJoinNode(new PlanNodeId(0), leftScanNode, rightScanNode, new TableRef(), joinExpr, new ArrayList<>());
        PlanFragment root = new PlanFragment(new PlanFragmentId(0), hashJoinNode, new DataPartition(TPartitionType.HASH_PARTITIONED, joinExpr));
        root.setPlanRoot(hashJoinNode);

        // init colocate group
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        GroupId groupId = new GroupId(1000, 1);
        colocateTableIndex.addTableToGroup(1000, leftTable, "abc", groupId);
        colocateTableIndex.addTableToGroup(1000, rightTable, "abc", groupId);
        List<List<Long>> backendsPerBucketSeq = generateBackendsPerBucketSeq(bucketNum, replicaNum, backendNum);
        colocateTableIndex.addBackendsPerBucketSeqByTag(groupId, Tag.DEFAULT_BACKEND_TAG, backendsPerBucketSeq, ReplicaAllocation.DEFAULT_ALLOCATION);

        Deencapsulation.setField(coordinator, "idToBackend", generateBackends(backendNum));

        ArrayListMultimap<Integer, TScanRangeLocations> bucketseq2localtions
                = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);

        // case 1 complete match
        Deencapsulation.setField(leftScanNode, "bucketSeq2locations", bucketseq2localtions);
        Deencapsulation.setField(rightScanNode, "bucketSeq2locations", bucketseq2localtions);

        Map<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        Map<TNetworkAddress, Long> replicaNumPerHost = getReplicaNumPerHostForOlapTable(bucketseq2localtions.values());

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", leftScanNode, assignedBytesPerHost, replicaNumPerHost);

        Map<PlanFragmentId, Map<Integer, TScanRangeLocations>> fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        Map<Integer, TScanRangeLocations> locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> backends = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> backends1 = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + backends + "|" + backends1);
            Assert.assertTrue(backends.containsAll(backends1));
        }

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", rightScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));
        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());
        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> backends = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> backends1 = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + backends + "|" + backends1);
            Assert.assertTrue(backends.containsAll(backends1));
        }


        // case 2 partial match
        coordinator = new Coordinator(context, analyzer, originalPlanner);
        Deencapsulation.setField(coordinator, "idToBackend", generateBackends(backendNum));

        ArrayListMultimap<Integer, TScanRangeLocations> b1 = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);
        shuffleLocations(b1, 0, 0, 1);
        ArrayListMultimap<Integer, TScanRangeLocations> b2 = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);

        Deencapsulation.setField(leftScanNode, "bucketSeq2locations", b1);
        Deencapsulation.setField(rightScanNode, "bucketSeq2locations", b2);

        assignedBytesPerHost = Maps.newHashMap();
        ArrayList<TScanRangeLocations> locations = Lists.newArrayList(b1.values());
        locations.addAll(b2.values());
        replicaNumPerHost = getReplicaNumPerHostForOlapTable(locations);

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", leftScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> match = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> original = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + match + "|" + original);
            if (seq != 0) {
                Assert.assertEquals(match.size(), replicaNum);
                Assert.assertTrue(match.containsAll(original));
            } else {
                Assert.assertEquals(2, match.size());
                Assert.assertTrue(original.containsAll(match));
            }
        }

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", rightScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> match = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> original = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + match + "|" + original);
            if (seq != 0) {
                Assert.assertEquals(match.size(), replicaNum);
                Assert.assertTrue(match.containsAll(original));
            } else {
                Assert.assertEquals(2, match.size());
                Assert.assertTrue(original.containsAll(match));
            }
        }

        // case 2.1 partial match
        coordinator = new Coordinator(context, analyzer, originalPlanner);
        Deencapsulation.setField(coordinator, "idToBackend", generateBackends(backendNum));

        b1 = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);
        shuffleLocations(b1, 0, 0, 2);
        b2 = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);

        Deencapsulation.setField(leftScanNode, "bucketSeq2locations", b1);
        Deencapsulation.setField(rightScanNode, "bucketSeq2locations", b2);

        assignedBytesPerHost = Maps.newHashMap();
        locations = Lists.newArrayList(b1.values());
        locations.addAll(b2.values());
        replicaNumPerHost = getReplicaNumPerHostForOlapTable(locations);

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", leftScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> match = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> original = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + match + "|" + original);
            if (seq != 0) {
                Assert.assertEquals(match.size(), replicaNum);
                Assert.assertTrue(match.containsAll(original));
            } else {
                Assert.assertEquals(1, match.size());
                Assert.assertTrue(original.containsAll(match));
            }
        }

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", rightScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> match = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> original = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + match + "|" + original);
            if (seq != 0) {
                Assert.assertEquals(match.size(), replicaNum);
                Assert.assertTrue(match.containsAll(original));
            } else {
                Assert.assertEquals(1, match.size());
                Assert.assertTrue(original.containsAll(match));
            }
        }

        // case 2.2 partial match
        coordinator = new Coordinator(context, analyzer, originalPlanner);
        Deencapsulation.setField(coordinator, "idToBackend", generateBackends(backendNum));

        coordinator = new Coordinator(context, analyzer, originalPlanner);
        Deencapsulation.setField(coordinator, "idToBackend", generateBackends(backendNum));

        b1 = generateBucketSeq2locations(backendsPerBucketSeq, 1, bucketNum, replicaNum);
        shuffleLocations(b1, 0, 0, 2);
        b2 = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);
        shuffleLocations(b2, 1, 1, 1);

        Deencapsulation.setField(leftScanNode, "bucketSeq2locations", b1);
        Deencapsulation.setField(rightScanNode, "bucketSeq2locations", b2);

        assignedBytesPerHost = Maps.newHashMap();
        locations = Lists.newArrayList(b1.values());
        locations.addAll(b2.values());
        replicaNumPerHost = getReplicaNumPerHostForOlapTable(locations);

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", leftScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> match = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> original = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + match + "|" + original);
            if (seq != 0) {
                Assert.assertEquals(match.size(), replicaNum);
                Assert.assertTrue(match.containsAll(original));
            } else {
                Assert.assertEquals(1, match.size());
                Assert.assertTrue(original.containsAll(match));
            }
        }

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", rightScanNode, assignedBytesPerHost, replicaNumPerHost);
        fragmentIdToSeqToLocationsMap = Deencapsulation.getField(coordinator, "fragmentIdToSeqToLocationsMap");
        locationsMap = fragmentIdToSeqToLocationsMap.get(new PlanFragmentId(0));

        Assert.assertEquals(locationsMap.size(), backendsPerBucketSeq.size());

        for (Map.Entry<Integer, TScanRangeLocations> entry : locationsMap.entrySet()) {
            Integer seq = entry.getKey();
            Set<Long> match = entry.getValue().locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            Set<Long> original = Sets.newHashSet(backendsPerBucketSeq.get(seq));
            System.out.println(seq + ":" + match + "|" + original);
            if (seq != 0) {
                if (seq == 1) {
                    Assert.assertEquals(match.size(), 2);
                    Assert.assertTrue(original.containsAll(match));
                } else {
                    Assert.assertEquals(match.size(), replicaNum);
                    Assert.assertTrue(match.containsAll(original));
                }
            } else {
                Assert.assertEquals(1, match.size());
                Assert.assertTrue(original.containsAll(match));
            }
        }

        // case 2.3 partial match
        coordinator = new Coordinator(context, analyzer, originalPlanner);
        Deencapsulation.setField(coordinator, "idToBackend", generateBackends(backendNum));

        b1 = generateBucketSeq2locations(backendsPerBucketSeq, 1, bucketNum, replicaNum);
        shuffleLocations(b1, 0, 0, 3);
        b2 = generateBucketSeq2locations(backendsPerBucketSeq, 2, bucketNum, replicaNum);

        Deencapsulation.setField(leftScanNode, "bucketSeq2locations", b1);
        Deencapsulation.setField(rightScanNode, "bucketSeq2locations", b2);

        assignedBytesPerHost = Maps.newHashMap();
        locations = Lists.newArrayList(b1.values());
        locations.addAll(b2.values());
        replicaNumPerHost = getReplicaNumPerHostForOlapTable(locations);

        try {
            Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByColocate", leftScanNode, assignedBytesPerHost, replicaNumPerHost);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "errCode = 2, detailMessage = there is no scanNode Backend for Colocate");
        }
    }

    private Map<TNetworkAddress, Long> getReplicaNumPerHostForOlapTable(Collection<TScanRangeLocations> locationsSet) {
        Map<TNetworkAddress, Long> replicaNumPerHost = Maps.newHashMap();
        for (TScanRangeLocations locations : locationsSet) {
            for (TScanRangeLocation location : locations.locations) {
                if (replicaNumPerHost.containsKey(location.server)) {
                    replicaNumPerHost.put(location.server, replicaNumPerHost.get(location.server) + 1L);
                } else {
                    replicaNumPerHost.put(location.server, 1L);
                }
            }

        }
        return replicaNumPerHost;
    }

    private void shuffleLocations(ArrayListMultimap<Integer, TScanRangeLocations> bucketseq2localtions, int bucketSeq, int partition, int replica) {
        List<TScanRangeLocations> locationsList = bucketseq2localtions.get(bucketSeq);
        TScanRangeLocations locations = locationsList.get(partition);
        for (int i = 0; i < replica; i++) {
            Set<Long> backends = locations.locations.stream().map(b -> b.backend_id).collect(Collectors.toSet());
            TScanRangeLocation location = locations.getLocations().get(i);
            long beId;
            do {
                beId = new Random().nextInt(10);
            } while (backends.contains(beId));
            location.setBackendId(beId);
            location.setServer(new TNetworkAddress("0.0.0." + beId, 9060));
        }
    }

    private List<List<Long>> generateBackendsPerBucketSeq(int bucketNum, int replicaNum, int backendNum) {
        List<List<Long>> seqs = new ArrayList<>();
        for (int i = 0; i < bucketNum; i++) {
            Set<Long> backends = new HashSet<>();
            do {
                backends.add((long) new Random().nextInt(backendNum));
            } while (backends.size() != replicaNum);
            seqs.add(Lists.newArrayList(backends));
        }
        return seqs;
    }

    private ArrayListMultimap<Integer, TScanRangeLocations> generateBucketSeq2locations(List<List<Long>> backendsPerBucketSeq, int partitionsNum, int bucketNum, int replicaNum) {
        ArrayListMultimap<Integer, TScanRangeLocations> bucketseq2localtions = ArrayListMultimap.create();
        for (int i = 0; i < partitionsNum; i++) {
            for (int k = 0; k < bucketNum; k++) {
                TScanRangeLocations locations = new TScanRangeLocations();
                for (int j = 0; j < replicaNum; j++) {
                    long beId = backendsPerBucketSeq.get(k).get(j);
                    TScanRangeLocation location = new TScanRangeLocation(new TNetworkAddress("0.0.0." + beId, 9050));
                    location.backend_id = beId;
                    locations.addToLocations(location);
                }
                bucketseq2localtions.put(k, locations);
            }
        }
        return bucketseq2localtions;
    }

    private ImmutableMap<Long, Backend> generateBackends(int backendNum) {
        Builder<Long, Backend> builder = ImmutableMap.builder();
        for (long i = 0; i < backendNum; i++) {
            Backend backend = new Backend(i, "0.0.0." + i, 9060);
            backend.setAlive(true);
            builder.put(i, backend);
        }
        return builder.build();
    }


    @Test
    public void testColocateJoinAssignment()  {
        Coordinator coordinator = new Coordinator(context, analyzer, originalPlanner);

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
        Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = new HashMap<>();
        BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
        Map<Integer, List<TScanRangeParams>> scanRangeMap = new HashMap<>();
        scanRangeMap.put(scanNodeId, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, scanRangeMap);
        }
        fragmentIdBucketSeqToScanRangeMap.put(planFragmentId, bucketSeqToScanRange);
        Deencapsulation.setField(coordinator, "fragmentIdTobucketSeqToScanRangeMap", fragmentIdBucketSeqToScanRangeMap);

        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(scanNodeId), tupleDescriptor, "test");
        PlanFragment fragment = new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED));
        FragmentExecParams params = new FragmentExecParams(fragment);
        Deencapsulation.invoke(coordinator, "computeColocateJoinInstanceParam", planFragmentId, 1, params);
        StringBuilder sb = new StringBuilder();
        params.appendTo(sb);
        Assert.assertTrue(sb.toString().contains("range=[id1,range=[]]"));
    }

    @Test
    public void testComputeScanRangeAssignmentByBucket()  {
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
        tScanRangeLocation0.server = be0;
        TScanRangeLocation tScanRangeLocation1 = new TScanRangeLocation();
        tScanRangeLocation1.backend_id = 1;
        tScanRangeLocation1.server = be1;
        TScanRangeLocation tScanRangeLocation2 = new TScanRangeLocation();
        tScanRangeLocation2.backend_id = 2;
        tScanRangeLocation2.server = be2;

        tScanRangeLocations.locations = new ArrayList<>();
        tScanRangeLocations.locations.add(tScanRangeLocation0);
        tScanRangeLocations.locations.add(tScanRangeLocation1);
        tScanRangeLocations.locations.add(tScanRangeLocation2);
        for (int i = 0; i < 66; i++) {
            bucketseq2localtion.put(i, tScanRangeLocations);
        }

        Deencapsulation.setField(olapScanNode, "bucketSeq2locations", bucketseq2localtion);
        Deencapsulation.setField(olapScanNode, "totalTabletsNum", 66);
        olapScanNode.setFragment(new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED)));

        HashMap<Long, Backend> idToBackend = new HashMap<>();
        idToBackend.put(0L, backend0);
        idToBackend.put(1L, backend1);
        idToBackend.put(2L, backend2);

        Map<TNetworkAddress, Long> addressToBackendID = new HashMap<>();
        addressToBackendID.put(be0, 0L);
        addressToBackendID.put(be1, 1L);
        addressToBackendID.put(be2, 2L);

        Map<TNetworkAddress, Long> replicaNumPerHost = new HashMap<>();
        replicaNumPerHost.put(be0, 66L);
        replicaNumPerHost.put(be1, 66L);
        replicaNumPerHost.put(be2, 66L);

        Deencapsulation.invoke(bucketShuffleJoinController, "computeScanRangeAssignmentByBucket",
                olapScanNode, ImmutableMap.copyOf(idToBackend), addressToBackendID, replicaNumPerHost);
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
        Map<Integer, List<TScanRangeParams>> scanRangeMap = new HashMap<>();
        scanRangeMap.put(scanNodeId, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, scanRangeMap);
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
    public void testBucketShuffleAssignment()  {
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
        Map<Integer, List<TScanRangeParams>> scanRangeMap = new HashMap<>();
        scanRangeMap.put(scanNodeId, new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            bucketSeqToScanRange.put(i, scanRangeMap);
        }
        fragmentIdBucketSeqToScanRangeMap.put(planFragmentId, bucketSeqToScanRange);
        Deencapsulation.setField(bucketShuffleJoinController, "fragmentIdBucketSeqToScanRangeMap", fragmentIdBucketSeqToScanRangeMap);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(scanNodeId), tupleDescriptor, "test");
        PlanFragment fragment = new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED));

        FragmentExecParams params = new FragmentExecParams(fragment);
        Deencapsulation.invoke(bucketShuffleJoinController, "computeInstanceParam", planFragmentId, 1, params);
        Assert.assertEquals(1, params.instanceExecParams.size());
        StringBuilder sb = new StringBuilder();
        params.appendTo(sb);
        Assert.assertTrue(sb.toString().contains("range=[id1,range=[]]"));
    }

    @Test
    public void testComputeScanRangeAssignmentByScheduler()  {
        Coordinator coordinator = new Coordinator(context, analyzer, originalPlanner);
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
                new ImmutableMap.Builder<Long, Backend>()
                    .put(0L, backend0)
                    .put(1L, backend1)
                    .put(2L, backend2)
                    .build();
        Deencapsulation.setField(coordinator, "idToBackend", idToBackend);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        List<TScanRangeLocations> locations = new ArrayList<>();
        Map<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        Map<TNetworkAddress, Long> replicaNumPerHost = new HashMap<>();
        replicaNumPerHost.put(tScanRangeLocation0.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation1.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation2.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation3.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation4.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation5.server, 1L);
        locations.add(tScanRangeLocations);
        locations.add(tScanRangeLocations1);
        Deencapsulation.invoke(coordinator, "computeScanRangeAssignmentByScheduler",
                olapScanNode, locations, assignment, assignedBytesPerHost, replicaNumPerHost);
        for (Map.Entry entry : assignment.entrySet()) {
            Map<Integer, List<TScanRangeParams>> addr = (HashMap<Integer, List<TScanRangeParams>>) entry.getValue();
            for (Map.Entry item : addr.entrySet()) {
                List<TScanRangeParams> params = (List<TScanRangeParams>) item.getValue();
                Assert.assertTrue(params.size() == 2);
            }
        }
    }

    @Test
    public void testGetExecHostPortForFragmentIDAndBucketSeq()  {
        Coordinator coordinator = new Coordinator(context, analyzer, originalPlanner);
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
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
        // init all backend
        Backend backend0 = new Backend(0, "0.0.0.0", 9060);
        backend0.setAlive(true);
        backend0.setBePort(9050);
        Backend backend1 = new Backend(1, "0.0.0.1", 9060);
        backend1.setAlive(true);
        backend1.setBePort(9050);
        Backend backend2 = new Backend(2, "0.0.0.2", 9060);
        backend2.setAlive(true);
        backend2.setBePort(9050);

        ImmutableMap<Long, Backend> idToBackend =
                new ImmutableMap.Builder<Long, Backend>()
                    .put(0L, backend0)
                    .put(1L, backend1)
                    .put(2L, backend2)
                    .build();
        Deencapsulation.setField(coordinator, "idToBackend", idToBackend);
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
        fragmentIdToSeqToAddressMap.put(planFragmentId, new HashedMap());
        Deencapsulation.setField(coordinator, "fragmentIdToSeqToAddressMap", fragmentIdToSeqToAddressMap);
        List<TScanRangeLocations> locations = new ArrayList<>();
        locations.add(tScanRangeLocations);

        Map<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        Map<TNetworkAddress, Long> replicaNumPerHost = Maps.newHashMap();
        replicaNumPerHost.put(tScanRangeLocation0.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation1.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation2.server, 1L);

        Deencapsulation.invoke(coordinator, "getExecHostPortForFragmentIDAndBucketSeq", tScanRangeLocations,
                planFragmentId, 1, assignedBytesPerHost, replicaNumPerHost);
        Deencapsulation.invoke(coordinator, "getExecHostPortForFragmentIDAndBucketSeq", tScanRangeLocations,
                planFragmentId, 2, assignedBytesPerHost, replicaNumPerHost);
        Deencapsulation.invoke(coordinator, "getExecHostPortForFragmentIDAndBucketSeq", tScanRangeLocations,
                planFragmentId, 3, assignedBytesPerHost, replicaNumPerHost);
        List<String> hosts = new ArrayList<>();
        for (Map.Entry item : assignedBytesPerHost.entrySet()) {
            Assert.assertTrue((Long) item.getValue() == 1);
            TNetworkAddress addr = (TNetworkAddress) item.getKey();
            hosts.add(addr.hostname);
        }
        Assert.assertTrue(hosts.size() == 3);
    }

    @Test
    public void testBucketShuffleWithUnaliveBackend()  {
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
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
        Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();
        addressToBackendID.put(tScanRangeLocation0.server, tScanRangeLocation0.backend_id);
        addressToBackendID.put(tScanRangeLocation1.server, tScanRangeLocation1.backend_id);
        addressToBackendID.put(tScanRangeLocation2.server, tScanRangeLocation2.backend_id);

        ImmutableMap<Long, Backend> idToBackend =
                new ImmutableMap.Builder<Long, Backend>()
                    .put(0L, backend0)
                    .put(1L, backend1)
                    .put(2L, backend2)
                    .build();
        Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBuckendIdBucketCountMap = Maps.newHashMap();
        Map<Long, Integer> backendIdBucketCountMap = new HashMap<Long, Integer>();
        fragmentIdToBuckendIdBucketCountMap.put(planFragmentId, backendIdBucketCountMap);
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = new HashMap<>();
        BucketShuffleJoinController controller = new BucketShuffleJoinController(fragmentIdToScanNodeIds);
        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
        fragmentIdToSeqToAddressMap.put(planFragmentId, new HashMap<Integer, TNetworkAddress>());
        Map<TNetworkAddress, Long> replicaNumPerHost = Maps.newHashMap();
        replicaNumPerHost.put(tScanRangeLocation0.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation1.server, 1L);
        replicaNumPerHost.put(tScanRangeLocation2.server, 1L);
        Deencapsulation.setField(controller,  "fragmentIdToBuckendIdBucketCountMap", fragmentIdToBuckendIdBucketCountMap);
        Deencapsulation.setField(controller, "fragmentIdToSeqToAddressMap", fragmentIdToSeqToAddressMap);
        Deencapsulation.invoke(controller, "getExecHostPortForFragmentIDAndBucketSeq",
                tScanRangeLocations, planFragmentId, 1, idToBackend, addressToBackendID, replicaNumPerHost);
        Assert.assertTrue(backendIdBucketCountMap.size() == 1);
        List<Long> backendIds = new ArrayList<Long>();
        List<Integer> counts = new ArrayList<Integer>();
        for (Map.Entry<Long, Integer> item : backendIdBucketCountMap.entrySet()) {
            backendIds.add(item.getKey());
            counts.add(item.getValue());
        }
        Assert.assertTrue(backendIds.get(0) == 1);
        Assert.assertTrue(counts.get(0) == 1);
    }

    @Test
    public void testComputeScanRangeAssignment()  {
        Coordinator coordinator = new Coordinator(context, analyzer, originalPlanner);

        //TScanRangeLocations
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

        //scanNode1
        PlanFragmentId planFragmentId = new PlanFragmentId(1);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        OlapTable olapTable = new OlapTable();
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(66, new ArrayList<>());
        Deencapsulation.setField(olapTable, "defaultDistributionInfo", hashDistributionInfo);
        tupleDescriptor.setTable(olapTable);
        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(1), tupleDescriptor, "test");
        PlanFragment fragment = new PlanFragment(planFragmentId, olapScanNode,
                new DataPartition(TPartitionType.UNPARTITIONED));
        olapScanNode.setFragment(fragment);
        List<TScanRangeLocations> locations = new ArrayList<>();
        locations.add(tScanRangeLocations);
        Deencapsulation.setField(olapScanNode, "scanRangeLocations", locations);

        //scanNode2
        PlanFragmentId planFragmentId2 = new PlanFragmentId(2);
        TupleDescriptor tupleDescriptor2 = new TupleDescriptor(new TupleId(-1));
        OlapTable olapTable2 = new OlapTable();
        HashDistributionInfo hashDistributionInfo2 = new HashDistributionInfo(66, new ArrayList<>());
        Deencapsulation.setField(olapTable2, "defaultDistributionInfo", hashDistributionInfo2);
        tupleDescriptor2.setTable(olapTable2);
        OlapScanNode olapScanNode2 = new OlapScanNode(new PlanNodeId(2), tupleDescriptor2, "test2");
        PlanFragment fragment2 = new PlanFragment(planFragmentId2, olapScanNode2,
                new DataPartition(TPartitionType.UNPARTITIONED));
        olapScanNode2.setFragment(fragment2);
        List<TScanRangeLocations> locations2 = new ArrayList<>();
        locations2.add(tScanRangeLocations);
        Deencapsulation.setField(olapScanNode2, "scanRangeLocations", locations2);

        //scanNode3
        PlanFragmentId planFragmentId3 = new PlanFragmentId(3);
        TupleDescriptor tupleDescriptor3 = new TupleDescriptor(new TupleId(-1));
        OlapTable olapTable3 = new OlapTable();
        HashDistributionInfo hashDistributionInfo3 = new HashDistributionInfo(66, new ArrayList<>());
        Deencapsulation.setField(olapTable3, "defaultDistributionInfo", hashDistributionInfo3);
        tupleDescriptor3.setTable(olapTable3);
        OlapScanNode olapScanNode3 = new OlapScanNode(new PlanNodeId(3), tupleDescriptor3, "test3");
        PlanFragment fragment3 = new PlanFragment(planFragmentId3, olapScanNode3,
                new DataPartition(TPartitionType.UNPARTITIONED));
        olapScanNode3.setFragment(fragment3);
        List<TScanRangeLocations> locations3 = new ArrayList<>();
        locations3.add(tScanRangeLocations);
        Deencapsulation.setField(olapScanNode3, "scanRangeLocations", locations3);

        //scan nodes
        List<ScanNode> scanNodes = new ArrayList<>();
        scanNodes.add(olapScanNode);
        scanNodes.add(olapScanNode2);
        scanNodes.add(olapScanNode3);
        Deencapsulation.setField(coordinator, "scanNodes", scanNodes);

        //fragmentIdToScanNodeIds
        Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = Maps.newHashMap();
        Set<Integer> ids1 = new HashSet<>();
        ids1.add(1);
        fragmentIdToScanNodeIds.put(planFragmentId, ids1);
        Set<Integer> ids2 = new HashSet<>();
        ids1.add(2);
        fragmentIdToScanNodeIds.put(planFragmentId, ids2);
        Set<Integer> ids3 = new HashSet<>();
        ids1.add(3);
        fragmentIdToScanNodeIds.put(planFragmentId, ids3);
        Deencapsulation.setField(coordinator, "fragmentIdToScanNodeIds", fragmentIdToScanNodeIds);

        //fragmentExecParamsMap
        Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();
        fragmentExecParamsMap.put(planFragmentId, new FragmentExecParams(fragment));
        fragmentExecParamsMap.put(planFragmentId2, new FragmentExecParams(fragment2));
        fragmentExecParamsMap.put(planFragmentId3, new FragmentExecParams(fragment3));
        Deencapsulation.setField(coordinator, "fragmentExecParamsMap", fragmentExecParamsMap);

        // init all backend
        Backend backend0 = new Backend(0, "0.0.0.0", 9060);
        backend0.setAlive(true);
        backend0.setBePort(9050);
        Backend backend1 = new Backend(1, "0.0.0.1", 9060);
        backend1.setAlive(true);
        backend1.setBePort(9050);
        Backend backend2 = new Backend(2, "0.0.0.2", 9060);
        backend2.setAlive(true);
        backend2.setBePort(9050);

        ImmutableMap<Long, Backend> idToBackend =
                new ImmutableMap.Builder<Long, Backend>()
                    .put(0L, backend0)
                    .put(1L, backend1)
                    .put(2L, backend2)
                    .build();
        Deencapsulation.setField(coordinator, "idToBackend", idToBackend);

        Deencapsulation.invoke(coordinator, "computeScanRangeAssignment");
        Set<String> hostNames = Sets.newHashSet();
        hostNames.add("0.0.0.0");
        hostNames.add("0.0.0.1");
        hostNames.add("0.0.0.2");
        FragmentScanRangeAssignment assignment = fragmentExecParamsMap.get(fragment.getFragmentId()).scanRangeAssignment;
        Assert.assertTrue(assignment.size() == 1);
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            TNetworkAddress host = entry.getKey();
            Assert.assertTrue(hostNames.contains(host.hostname));
            hostNames.remove(host.hostname);
        }

        FragmentScanRangeAssignment assignment2 = fragmentExecParamsMap.get(fragment2.getFragmentId()).scanRangeAssignment;
        Assert.assertTrue(assignment2.size() == 1);
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment2.entrySet()) {
            TNetworkAddress host = entry.getKey();
            Assert.assertTrue(hostNames.contains(host.hostname));
            hostNames.remove(host.hostname);
        }

        FragmentScanRangeAssignment assignment3 = fragmentExecParamsMap.get(fragment3.getFragmentId()).scanRangeAssignment;
        Assert.assertTrue(assignment3.size() == 1);
        for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : assignment3.entrySet()) {
            TNetworkAddress host = entry.getKey();
            Assert.assertTrue(hostNames.contains(host.hostname));
            hostNames.remove(host.hostname);
        }
    }
}
