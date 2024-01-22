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

package org.apache.doris.planner;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.FederationBackendPolicy;
import org.apache.doris.planner.external.FileSplit;
import org.apache.doris.planner.external.NodeSelectionStrategy;
import org.apache.doris.planner.external.SplitWeight;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class FederationBackendPolicyTest {
    private static <K, V> boolean areMultimapsEqualIgnoringOrder(
            Multimap<K, V> multimap1, Multimap<K, V> multimap2) {
        Collection<Map.Entry<K, V>> entries1 = multimap1.entries();
        Collection<Map.Entry<K, V>> entries2 = multimap2.entries();

        return entries1.containsAll(entries2) && entries2.containsAll(entries1);
    }

    @Test
    public void testComputeScanRangeAssignmentRemote() throws UserException {
        SystemInfoService service = new SystemInfoService();

        Backend backend1 = new Backend(10002L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(10003L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(10004L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 103711014, 103711014, 0, null, Collections.emptyList()));

        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 3;
        Assertions.assertEquals(policy.numBackends(), backendNum);

        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        for (Backend backend : assignment.keySet()) {
            Collection<Split> assignedSplits = assignment.get(backend);
            long scanBytes = 0L;
            for (Split split : assignedSplits) {
                FileSplit fileSplit = (FileSplit) split;
                scanBytes += fileSplit.getLength();
            }
            System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
        }
        // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        // int variance = 5 * scanRangeSize;
        Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
        for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
            System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
            // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        }

    }

    @Test
    public void testComputeScanRangeAssignmentConsistentHash() throws UserException {
        SystemInfoService service = new SystemInfoService();

        Backend backend1 = new Backend(10002L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(10003L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(10004L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 103711014, 103711014, 0, null, Collections.emptyList()));

        FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        policy.init();
        int backendNum = 3;
        Assertions.assertEquals(policy.numBackends(), backendNum);

        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        for (Backend backend : assignment.keySet()) {
            Collection<Split> assignedSplits = assignment.get(backend);
            long scanBytes = 0L;
            for (Split split : assignedSplits) {
                FileSplit fileSplit = (FileSplit) split;
                scanBytes += fileSplit.getLength();
            }
            System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
        }
        // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        // int variance = 5 * scanRangeSize;
        Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
        for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
            System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
            // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        }

    }

    @Test
    public void testComputeScanRangeAssignmentLocal() throws UserException {
        SystemInfoService service = new SystemInfoService();

        Backend backend1 = new Backend(10002L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(10003L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(10004L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, new String[] {"172.30.0.100"}, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, new String[] {"172.30.0.106"}, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 103711014, 103711014, 0, null, Collections.emptyList()));

        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 3;
        Assertions.assertEquals(policy.numBackends(), backendNum);
        int totalSplitNum = 0;
        List<Boolean> checkedLocalSplit = new ArrayList<>();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
        for (Backend backend : assignment.keySet()) {
            Collection<Split> assignedSplits = assignment.get(backend);
            for (Split split : assignedSplits) {
                FileSplit fileSplit = (FileSplit) split;
                ++totalSplitNum;
                if (fileSplit.getPath().equals(new Path(
                        "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"))) {
                    Assert.assertEquals("172.30.0.100", backend.getHost());
                    checkedLocalSplit.add(true);
                } else if (fileSplit.getPath().equals(new Path(
                        "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"))) {
                    Assert.assertEquals("172.30.0.106", backend.getHost());
                    checkedLocalSplit.add(true);
                }
            }
        }
        Assert.assertEquals(2, checkedLocalSplit.size());
        Assert.assertEquals(9, totalSplitNum);
        // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        // int variance = 5 * scanRangeSize;
        // Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
        // for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
        //     System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
        //     // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        // }

    }

    @Test
    public void testComputeScanRangeAssigmentRandom() throws UserException {
        SystemInfoService service = new SystemInfoService();
        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        Random random = new Random();
        int backendNum = random.nextInt(100 - 1) + 1;

        int minOctet3 = 0;
        int maxOctet3 = 250;
        int minOctet4 = 1;
        int maxOctet4 = 250;
        Set<Integer> backendIds = new HashSet<>();
        Set<String> ipAddresses = new HashSet<>();
        for (int i = 0; i < backendNum; i++) {
            String ipAddress;
            do {
                int octet3 = random.nextInt((maxOctet3 - minOctet3) + 1) + minOctet3;
                int octet4 = random.nextInt((maxOctet4 - minOctet4) + 1) + minOctet4;
                ipAddress = 192 + "." + 168 + "." + octet3 + "." + octet4;
            } while (!ipAddresses.add(ipAddress));

            int backendId;
            do {
                backendId = random.nextInt(90000) + 10000;
            } while (!backendIds.add(backendId));

            Backend backend = new Backend(backendId, ipAddress, 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        List<TScanRangeLocations> tScanRangeLocationsList = new ArrayList<>();
        List<Split> remoteSplits = new ArrayList<>();
        int splitCount = random.nextInt(1000 - 100) + 100;
        for (int i = 0; i < splitCount; ++i) {
            long splitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new Path(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, splitLength, splitLength, 0, null, Collections.emptyList());
            remoteSplits.add(split);
            tScanRangeLocationsList.add(
                    getScanRangeLocations(split.getPath().toString(), split.getStart(), split.getLength()));
        }

        List<TScanRangeLocations> localScanRangeLocationsList = new ArrayList<>();
        List<Split> localSplits = new ArrayList<>();
        int localSplitCount = random.nextInt(1000 - 100) + 100;
        Set<String> totalLocalHosts = new HashSet<>();
        for (int i = 0; i < localSplitCount; ++i) {
            int localHostNum = random.nextInt(3 - 1) + 1;
            Set<String> localHosts = new HashSet<>();
            String localHost;
            for (int j = 0; j < localHostNum; ++j) {
                do {
                    localHost = service.getAllBackends().get(random.nextInt(service.getAllBackends().size())).getHost();
                } while (!localHosts.add(localHost));
                totalLocalHosts.add(localHost);
            }
            long localSplitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new Path(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, localSplitLength, localSplitLength, 0, localHosts.toArray(new String[0]),
                    Collections.emptyList());
            localSplits.add(split);
            localScanRangeLocationsList.add(
                    getScanRangeLocations(split.getPath().toString(), split.getStart(), split.getLength()));
        }

        ListMultimap<Backend, Split> result = null;
        for (int i = 0; i < 3; ++i) {
            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            List<TScanRangeLocations> totalScanRangeLocationsList = new ArrayList<>();
            totalScanRangeLocationsList.addAll(tScanRangeLocationsList);
            totalScanRangeLocationsList.addAll(localScanRangeLocationsList);
            // Collections.shuffle(totalScanRangeLocationsList);
            policy.init();
            Assertions.assertEquals(policy.numBackends(), backendNum);
            int totalSplitNum = 0;
            int minSplitNumPerNode = Integer.MAX_VALUE;

            List<Split> totalSplits = new ArrayList<>();
            totalSplits.addAll(remoteSplits);
            totalSplits.addAll(localSplits);
            // Collections.shuffle(totalSplits);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(totalSplits);
            if (i == 0) {
                result = ArrayListMultimap.create(assignment);
            } else {
                Assertions.assertTrue(areMultimapsEqualIgnoringOrder(result, assignment));

            }
            for (Backend backend : assignment.keySet()) {
                Collection<Split> splits = assignment.get(backend);
                if (splits.size() < minSplitNumPerNode) {
                    minSplitNumPerNode = splits.size();
                }

                long scanBytes = 0L;
                for (Split split : splits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    ++totalSplitNum;
                    if (fileSplit.getHosts() != null && fileSplit.getHosts().length > 0) {
                        for (String host : fileSplit.getHosts()) {
                            Assert.assertTrue(totalLocalHosts.contains(host));
                        }
                    }
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, splits.size(), scanBytes);
            }
            Assert.assertEquals(totalSplits.size(), totalSplitNum);
            // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
            // int variance = 5 * scanRangeSize;
            SplitWeight.rawValueForStandardSplitCount(1);
            Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
            long splitNumVariance = 1;
            long splitWeightVariance = 5 * SplitWeight.rawValueForStandardSplitCount(1);
            Long minSplitWeight = stats.values()
                    .stream()
                    .min(Long::compare).orElse(0L);
            for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
                System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
                Assert.assertTrue(Math.abs(entry.getValue() - minSplitWeight) <= splitWeightVariance);
            }
            for (Backend backend : assignment.keySet()) {
                Collection<Split> splits = assignment.get(backend);
                Assert.assertTrue(Math.abs(splits.size() - minSplitNumPerNode) <= splitNumVariance);
            }
        }
    }

    @Test
    public void testComputeScanRangeAssigmentNonAlive() throws UserException {
        SystemInfoService service = new SystemInfoService();
        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        Random random = new Random();
        int backendNum = random.nextInt(100 - 1) + 1;

        int minOctet3 = 0;
        int maxOctet3 = 250;
        int minOctet4 = 1;
        int maxOctet4 = 250;
        Set<Integer> backendIds = new HashSet<>();
        Set<String> ipAddresses = new HashSet<>();
        int aliveBackendNum = 0;
        for (int i = 0; i < backendNum; i++) {
            String ipAddress;
            do {
                int octet3 = random.nextInt((maxOctet3 - minOctet3) + 1) + minOctet3;
                int octet4 = random.nextInt((maxOctet4 - minOctet4) + 1) + minOctet4;
                ipAddress = 192 + "." + 168 + "." + octet3 + "." + octet4;
            } while (!ipAddresses.add(ipAddress));

            int backendId;
            do {
                backendId = random.nextInt(90000) + 10000;
            } while (!backendIds.add(backendId));

            Backend backend = new Backend(backendId, ipAddress, 9050);
            if (i % 2 == 0) {
                ++aliveBackendNum;
                backend.setAlive(true);
            } else {
                backend.setAlive(false);
            }
            service.addBackend(backend);
        }

        List<TScanRangeLocations> tScanRangeLocationsList = new ArrayList<>();
        List<Split> remoteSplits = new ArrayList<>();
        int splitCount = random.nextInt(1000 - 100) + 100;
        for (int i = 0; i < splitCount; ++i) {
            long splitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new Path(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, splitLength, splitLength, 0, null, Collections.emptyList());
            remoteSplits.add(split);
            tScanRangeLocationsList.add(
                    getScanRangeLocations(split.getPath().toString(), split.getStart(), split.getLength()));
        }

        List<TScanRangeLocations> localScanRangeLocationsList = new ArrayList<>();
        List<Split> localSplits = new ArrayList<>();
        int localSplitCount = random.nextInt(1000 - 100) + 100;
        Set<String> totalLocalHosts = new HashSet<>();
        for (int i = 0; i < localSplitCount; ++i) {
            int localHostNum = random.nextInt(3 - 1) + 1;
            Set<String> localHosts = new HashSet<>();
            String localHost;
            for (int j = 0; j < localHostNum; ++j) {
                do {
                    localHost = service.getAllBackends().get(random.nextInt(service.getAllBackends().size())).getHost();
                } while (!localHosts.add(localHost));
                totalLocalHosts.add(localHost);
            }
            long localSplitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new Path(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, localSplitLength, localSplitLength, 0, localHosts.toArray(new String[0]),
                    Collections.emptyList());
            localSplits.add(split);
            localScanRangeLocationsList.add(
                    getScanRangeLocations(split.getPath().toString(), split.getStart(), split.getLength()));
        }

        Multimap<Backend, Split> result = null;
        for (int i = 0; i < 3; ++i) {
            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            List<TScanRangeLocations> totalScanRangeLocationsList = new ArrayList<>();
            totalScanRangeLocationsList.addAll(tScanRangeLocationsList);
            totalScanRangeLocationsList.addAll(localScanRangeLocationsList);
            // Collections.shuffle(totalScanRangeLocationsList);
            policy.init();
            // policy.setScanRangeLocationsList(totalScanRangeLocationsList);
            Assertions.assertEquals(policy.numBackends(), aliveBackendNum);
            // for (TScanRangeLocations scanRangeLocations : tScanRangeLocationsList) {
            //     System.out.println(policy.getNextConsistentBe(scanRangeLocations).getId());
            // }
            int totalSplitNum = 0;
            List<Split> totalSplits = new ArrayList<>();
            totalSplits.addAll(remoteSplits);
            totalSplits.addAll(localSplits);
            // Collections.shuffle(totalSplits);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(totalSplits);
            if (i == 0) {
                result = ArrayListMultimap.create(assignment);
            } else {
                Assertions.assertEquals(result, assignment);
            }
            for (Backend backend : assignment.keySet()) {
                Collection<Split> splits = assignment.get(backend);
                long scanBytes = 0L;
                for (Split split : splits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    ++totalSplitNum;
                    if (fileSplit.getHosts() != null && fileSplit.getHosts().length > 0) {
                        for (String host : fileSplit.getHosts()) {
                            Assert.assertTrue(totalLocalHosts.contains(host));
                        }
                    }
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, splits.size(), scanBytes);
            }
            Assert.assertEquals(totalSplits.size(), totalSplitNum);
            // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
            // int variance = 5 * scanRangeSize;
            Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
            for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
                System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
                // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
            }
        }
    }

    @Test
    public void testComputeScanRangeAssignmentNodeChanged() throws UserException {
        SystemInfoService service = new SystemInfoService();

        Backend backend1 = new Backend(10002L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(10003L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(10004L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new Path(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 103711014, 103711014, 0, null, Collections.emptyList()));

        Map<Split, Backend> originSplitAssignedBackends = new HashMap<>();
            {
            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            policy.init();
            int backendNum = 3;
            Assertions.assertEquals(policy.numBackends(), backendNum);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

            for (Backend backend : assignment.keySet()) {
                Collection<Split> assignedSplits = assignment.get(backend);
                long scanBytes = 0L;
                for (Split split : assignedSplits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    originSplitAssignedBackends.put(split, backend);
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
            }
            // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
            // int variance = 5 * scanRangeSize;
            Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
            for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
                System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
                // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
            }
            }

        // remove a node
        // {
        //     service.dropBackend(backend3.getId());
        //     int changed = 0;
        //
        //     FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        //     policy.init();
        //     int backendNum = 2;
        //     Assertions.assertEquals(policy.numBackends(), backendNum);
        //     Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
        //
        //     for (Backend backend : assignment.keySet()) {
        //         Collection<Split> assignedSplits = assignment.get(backend);
        //         long ScanBytes = 0L;
        //         for (Split split : assignedSplits) {
        //             FileSplit fileSplit = (FileSplit) split;
        //             ScanBytes += fileSplit.getLength();
        //             Backend origin = originSplitAssignedBackends.get(split);
        //             if (!backend.equals(origin)) {
        //                 changed += 1;
        //             }
        //         }
        //         System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), ScanBytes);
        //     }
        //
        //     // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        //     // int variance = 5 * scanRangeSize;
        //     Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
        //     for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
        //         System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
        //         // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        //     }
        //
        //     float moveRatio = changed * 1.0f / assignment.values().size();
        //     System.out.printf("Remove a node: move ratio = %.2f\n", moveRatio);
        // }
        //
        // // add a node
        // {
        //     Backend backend4 = new Backend(10004L, "172.30.0.128", 9050);
        //     backend4.setAlive(true);
        //     service.addBackend(backend4);
        //     int changed = 0;
        //
        //     FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        //     policy.init();
        //     int backendNum = 3;
        //     Assertions.assertEquals(policy.numBackends(), backendNum);
        //     Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
        //
        //     for (Backend backend : assignment.keySet()) {
        //         Collection<Split> assignedSplits = assignment.get(backend);
        //         long ScanBytes = 0L;
        //         for (Split split : assignedSplits) {
        //             FileSplit fileSplit = (FileSplit) split;
        //             ScanBytes += fileSplit.getLength();
        //             Backend origin = originSplitAssignedBackends.get(split);
        //             if (!backend.equals(origin)) {
        //                 changed += 1;
        //             }
        //         }
        //         System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), ScanBytes);
        //     }
        //     // int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        //     // int variance = 5 * scanRangeSize;
        //     Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
        //     for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
        //         System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
        //         // Assert.assertTrue(Math.abs(entry.getValue() - avg) < variance);
        //     }
        //
        //     float moveRatio = changed * 1.0f / assignment.values().size();
        //     System.out.printf("Add a node, move ratio = %.2f\n", moveRatio);
        // }

    }


    private TScanRangeLocations getScanRangeLocations(String path, long startOffset, long size) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);
        scanRange.getExtScanRange().getFileScanRange().addToRanges(createRangeDesc(path, startOffset, size));
        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);
        return locations;
    }

    private TFileRangeDesc createRangeDesc(String path, long startOffset, long size) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setPath(path);
        rangeDesc.setStartOffset(startOffset);
        rangeDesc.setSize(size);
        return rangeDesc;
    }
}

