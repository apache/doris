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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.NodeSelectionStrategy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
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
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class FederationBackendPolicyTest {
    @Mocked
    private Env env;

    @Test
    public void testRemoteSplits() throws UserException {
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
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));

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
    }

    @Test
    public void testHasLocalSplits() throws UserException {
        SystemInfoService service = new SystemInfoService();

        Backend backend1 = new Backend(30002L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(30003L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(30004L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, new String[] {"172.30.0.100"}, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, new String[] {"172.30.0.106"}, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));

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
                if (fileSplit.getPath().getPath().equals(new Path(
                        "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"))) {
                    Assert.assertEquals("172.30.0.100", backend.getHost());
                    checkedLocalSplit.add(true);
                } else if (fileSplit.getPath().getPath().equals(new Path(
                        "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"))) {
                    Assert.assertEquals("172.30.0.106", backend.getHost());
                    checkedLocalSplit.add(true);
                }
            }
        }
        Assert.assertEquals(2, checkedLocalSplit.size());
        Assert.assertEquals(8, totalSplitNum);

        int maxAssignedSplitNum = Integer.MIN_VALUE;
        int minAssignedSplitNum = Integer.MAX_VALUE;
        for (Backend backend : assignment.keySet()) {
            Collection<Split> assignedSplits = assignment.get(backend);
            long scanBytes = 0L;
            for (Split split : assignedSplits) {
                FileSplit fileSplit = (FileSplit) split;
                scanBytes += fileSplit.getLength();
            }
            if (assignedSplits.size() <= minAssignedSplitNum) {
                minAssignedSplitNum = assignedSplits.size();
            }
            if (assignedSplits.size() >= maxAssignedSplitNum) {
                maxAssignedSplitNum = assignedSplits.size();
            }
            System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
        }
        Assert.assertTrue(Math.abs(maxAssignedSplitNum - minAssignedSplitNum) <= Config.split_assigner_max_split_num_variance);

    }

    @Test
    public void testConsistentHash() throws UserException {
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
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));

        FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        policy.init();
        int backendNum = 3;
        Assertions.assertEquals(policy.numBackends(), backendNum);

        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        int maxAssignedSplitNum = Integer.MIN_VALUE;
        int minAssignedSplitNum = Integer.MAX_VALUE;
        for (Backend backend : assignment.keySet()) {
            Collection<Split> assignedSplits = assignment.get(backend);
            long scanBytes = 0L;
            for (Split split : assignedSplits) {
                FileSplit fileSplit = (FileSplit) split;
                scanBytes += fileSplit.getLength();
            }
            if (assignedSplits.size() <= minAssignedSplitNum) {
                minAssignedSplitNum = assignedSplits.size();
            }
            if (assignedSplits.size() >= maxAssignedSplitNum) {
                maxAssignedSplitNum = assignedSplits.size();
            }
            System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
        }
        Assert.assertTrue(Math.abs(maxAssignedSplitNum - minAssignedSplitNum) <= Config.split_assigner_max_split_num_variance);

    }

    public static void sortSplits(List<Split> splits) {
        splits.sort((split1, split2) -> {
            int pathComparison = split1.getPathString().compareTo(split2.getPathString());
            if (pathComparison != 0) {
                return pathComparison;
            }

            int startComparison = Long.compare(split1.getStart(), split2.getStart());
            if (startComparison != 0) {
                return startComparison;
            }
            return Long.compare(split1.getLength(), split2.getLength());
        });
    }

    @Test
    public void testGenerateRandomly() throws UserException {
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

        List<Split> remoteSplits = new ArrayList<>();
        int splitCount = random.nextInt(1000 - 100) + 100;
        for (int i = 0; i < splitCount; ++i) {
            long splitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new LocationPath(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, splitLength, splitLength, 0, null, Collections.emptyList());
            remoteSplits.add(split);
        }

        List<Split> localSplits = new ArrayList<>();
        int localSplitCount = random.nextInt(1000 - 100) + 100;
        Set<String> totalLocalHosts = new HashSet<>();
        for (int i = 0; i < localSplitCount; ++i) {
            int localHostNum = random.nextInt(3 - 1) + 1;
            Set<String> localHosts = new HashSet<>();
            String localHost;
            List<Backend> backends = service.getAllBackendsByAllCluster().values().asList();
            for (int j = 0; j < localHostNum; ++j) {
                do {
                    localHost = backends.get(random.nextInt(backends.size())).getHost();
                } while (!localHosts.add(localHost));
                totalLocalHosts.add(localHost);
            }
            long localSplitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new LocationPath(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, localSplitLength, localSplitLength, 0, localHosts.toArray(new String[0]),
                    Collections.emptyList());
            localSplits.add(split);
        }

        ListMultimap<Backend, Split> result = null;
        // Run 3 times to ensure the same results
        for (int i = 0; i < 3; ++i) {
            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            policy.init();
            Assertions.assertEquals(policy.numBackends(), backendNum);
            int totalSplitNum = 0;

            List<Split> totalSplits = new ArrayList<>();
            totalSplits.addAll(remoteSplits);
            totalSplits.addAll(localSplits);
            sortSplits(totalSplits);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(totalSplits);
            if (i == 0) {
                result = ArrayListMultimap.create(assignment);
            } else {
                Assertions.assertTrue(areMultimapsEqualIgnoringOrder(result, assignment));

            }
            int maxAssignedSplitNum = Integer.MIN_VALUE;
            int minAssignedSplitNum = Integer.MAX_VALUE;
            for (Backend backend : assignment.keySet()) {
                Collection<Split> assignedSplits = assignment.get(backend);
                if (assignedSplits.size() <= minAssignedSplitNum) {
                    minAssignedSplitNum = assignedSplits.size();
                }
                if (assignedSplits.size() >= maxAssignedSplitNum) {
                    maxAssignedSplitNum = assignedSplits.size();
                }

                long scanBytes = 0L;
                for (Split split : assignedSplits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    ++totalSplitNum;
                    if (fileSplit.getHosts() != null && fileSplit.getHosts().length > 0) {
                        for (String host : fileSplit.getHosts()) {
                            Assert.assertTrue(totalLocalHosts.contains(host));
                        }
                    }
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
            }
            Assert.assertEquals(totalSplits.size(), totalSplitNum);

            Assert.assertTrue(Math.abs(maxAssignedSplitNum - minAssignedSplitNum) <= Config.split_assigner_max_split_num_variance);
        }
    }

    @Test
    public void testNonAliveNodes() throws UserException {
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

        List<Split> remoteSplits = new ArrayList<>();
        int splitCount = random.nextInt(1000 - 100) + 100;
        for (int i = 0; i < splitCount; ++i) {
            long splitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new LocationPath(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, splitLength, splitLength, 0, null, Collections.emptyList());
            remoteSplits.add(split);
        }

        List<Split> localSplits = new ArrayList<>();
        int localSplitCount = random.nextInt(1000 - 100) + 100;
        Set<String> totalLocalHosts = new HashSet<>();
        for (int i = 0; i < localSplitCount; ++i) {
            int localHostNum = random.nextInt(3 - 1) + 1;
            Set<String> localHosts = new HashSet<>();
            String localHost;
            List<Backend> backends = service.getAllBackendsByAllCluster().values().asList();
            for (int j = 0; j < localHostNum; ++j) {
                do {
                    localHost = backends.get(random.nextInt(backends.size())).getHost();
                } while (!localHosts.add(localHost));
                totalLocalHosts.add(localHost);
            }
            long localSplitLength = random.nextInt(115343360 - 94371840) + 94371840;
            FileSplit split = new FileSplit(new LocationPath(
                    "hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()),
                    0, localSplitLength, localSplitLength, 0, localHosts.toArray(new String[0]),
                    Collections.emptyList());
            localSplits.add(split);
        }

        Multimap<Backend, Split> result = null;
        // Run 3 times to ensure the same results
        for (int i = 0; i < 3; ++i) {
            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            policy.init();
            Assertions.assertEquals(policy.numBackends(), aliveBackendNum);
            int totalSplitNum = 0;
            List<Split> totalSplits = new ArrayList<>();
            totalSplits.addAll(remoteSplits);
            totalSplits.addAll(localSplits);
            sortSplits(totalSplits);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(totalSplits);
            if (i == 0) {
                result = ArrayListMultimap.create(assignment);
            } else {
                Assertions.assertEquals(result, assignment);
            }
            int maxAssignedSplitNum = Integer.MIN_VALUE;
            int minAssignedSplitNum = Integer.MAX_VALUE;
            for (Backend backend : assignment.keySet()) {
                Collection<Split> assignedSplits = assignment.get(backend);
                if (assignedSplits.size() <= minAssignedSplitNum) {
                    minAssignedSplitNum = assignedSplits.size();
                }
                if (assignedSplits.size() >= maxAssignedSplitNum) {
                    maxAssignedSplitNum = assignedSplits.size();
                }

                long scanBytes = 0L;
                for (Split split : assignedSplits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    ++totalSplitNum;
                    if (fileSplit.getHosts() != null && fileSplit.getHosts().length > 0) {
                        for (String host : fileSplit.getHosts()) {
                            Assert.assertTrue(totalLocalHosts.contains(host));
                        }
                    }
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
            }
            Assert.assertEquals(totalSplits.size(), totalSplitNum);

            Assert.assertTrue(Math.abs(maxAssignedSplitNum - minAssignedSplitNum) <= Config.split_assigner_max_split_num_variance);
        }
    }

    private static class TestSplitHashKey {
        private String path;
        private long start;
        private long length;

        public TestSplitHashKey(String path, long start, long length) {
            this.path = path;
            this.start = start;
            this.length = length;
        }

        public String getPath() {
            return path;
        }

        public long getStart() {
            return start;
        }

        public long getLength() {
            return length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestSplitHashKey that = (TestSplitHashKey) o;
            return start == that.start && length == that.length && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, start, length);
        }
    }

    @Test
    public void testConsistentHashWhenNodeChanged() throws UserException {
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
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(new LocationPath(
                "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"),
                0, 105664025, 105664025, 0, null, Collections.emptyList()));

        Map<TestSplitHashKey, Backend> originSplitAssignedBackends = new HashMap<>();
            {
            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            policy.init();
            // Set these options to ensure that the consistent hash algorithm is consistent.
            policy.setEnableSplitsRedistribution(false);
            Config.split_assigner_min_consistent_hash_candidate_num = 1;
            int backendNum = 3;
            Assertions.assertEquals(policy.numBackends(), backendNum);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

            for (Backend backend : assignment.keySet()) {
                Collection<Split> assignedSplits = assignment.get(backend);
                long scanBytes = 0L;
                for (Split split : assignedSplits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    originSplitAssignedBackends.put(
                            new TestSplitHashKey(split.getPathString(), split.getStart(), split.getLength()), backend);
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
            }
            Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
            for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
                System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
            }
            }

            // remove a node
            {
            service.dropBackend(backend3.getId());
            int changed = 0;

            FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
            policy.init();
            int backendNum = 2;
            Assertions.assertEquals(policy.numBackends(), backendNum);
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

            for (Backend backend : assignment.keySet()) {
                Collection<Split> assignedSplits = assignment.get(backend);
                long scanBytes = 0L;
                for (Split split : assignedSplits) {
                    FileSplit fileSplit = (FileSplit) split;
                    scanBytes += fileSplit.getLength();
                    Backend origin = originSplitAssignedBackends.get(
                            new TestSplitHashKey(split.getPathString(), split.getStart(), split.getLength()));
                    if (!backend.equals(origin)) {
                        changed += 1;
                    }
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
            }

            Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
            for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
                System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
            }

            float moveRatio = changed * 1.0f / assignment.values().size();
            System.out.printf("Remove a node: move ratio = %.2f\n", moveRatio);
            Assertions.assertEquals(0.375, moveRatio);
            }

            // add a node
            {
            Backend backend4 = new Backend(10004L, "172.30.0.128", 9050);
            backend4.setAlive(true);
            service.addBackend(backend4);
            int changed = 0;

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
                    Backend origin = originSplitAssignedBackends.get(
                            new TestSplitHashKey(split.getPathString(), split.getStart(), split.getLength()));
                    if (!backend.equals(origin)) {
                        changed += 1;
                    }
                }
                System.out.printf("%s -> %d splits, %d bytes\n", backend, assignedSplits.size(), scanBytes);
            }
            Map<Backend, Long> stats = policy.getAssignedWeightPerBackend();
            for (Map.Entry<Backend, Long> entry : stats.entrySet()) {
                System.out.printf("weight: %s -> %d\n", entry.getKey(), entry.getValue());
            }

            float moveRatio = changed * 1.0f / assignment.values().size();
            System.out.printf("Add a node, move ratio = %.2f\n", moveRatio);
            Assertions.assertEquals(0.25, moveRatio);
            }
    }

    private static <K, V> boolean areMultimapsEqualIgnoringOrder(
            Multimap<K, V> multimap1, Multimap<K, V> multimap2) {
        Collection<Map.Entry<K, V>> entries1 = multimap1.entries();
        Collection<Map.Entry<K, V>> entries2 = multimap2.entries();

        return entries1.containsAll(entries2) && entries2.containsAll(entries1);
    }
}
