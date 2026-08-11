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
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.doris.source.RemoteDorisSplit;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.datasource.scan.NodeSelectionStrategy;
import org.apache.doris.datasource.split.FileSplit;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class FederationBackendPolicyTest {
    private Env env = Mockito.mock(Env.class);
    private MockedStatic<Env> mockedEnvStatic;

    @Before
    public void setUp() {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(org.apache.doris.persist.EditLog.class));
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

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

        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 105664025, 105664025, 0, null, Collections.emptyList()));

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
    public void testSplitsOfSameFileStayOnOneBackendAcrossBatches() throws UserException {
        SystemInfoService service = new SystemInfoService();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(20000L + i, "172.30.0." + (100 + i), 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        ComputeGroupMgr computeGroupMgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);

        String path = "hdfs://namenode/warehouse/table/data.parquet";
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();

        Set<Long> assignedBackendIds = new HashSet<>();
        int assignedSplitCount = 0;
        for (int batch = 0; batch < 2; batch++) {
            List<Split> splits = new ArrayList<>();
            for (int splitIndex = 0; splitIndex < 3; splitIndex++) {
                long start = (batch * 3L + splitIndex) * 128;
                splits.add(fileSplit(path, start, 128, 768));
            }
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
            assignedSplitCount += assignment.size();
            assignment.keySet().forEach(backend -> assignedBackendIds.add(backend.getId()));
        }

        Assert.assertEquals(1, assignedBackendIds.size());
        Assert.assertEquals(6, assignedSplitCount);
    }

    @Test
    public void testFileAffinityDoesNotDependOnBatchOrder() throws UserException {
        SystemInfoService service = new SystemInfoService();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(20500L + i, "172.30.1." + (100 + i), 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        ComputeGroupMgr computeGroupMgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);

        String pathA = "s3://bucket/table/a.parquet";
        String pathB = "s3://bucket/table/b.parquet";
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();

        List<List<Split>> batches = new ArrayList<>();
        batches.add(Collections.singletonList(fileSplit(pathA, 0, 128, 256)));
        batches.add(Collections.singletonList(fileSplit(pathB, 0, 128, 256)));
        batches.add(Collections.singletonList(fileSplit(pathA, 128, 128, 256)));

        Set<Long> pathABackends = new HashSet<>();
        for (List<Split> batch : batches) {
            Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(batch);
            Assert.assertEquals(1, assignment.size());
            for (Map.Entry<Backend, Split> entry : assignment.entries()) {
                if (pathA.equals(entry.getValue().getPathString())) {
                    pathABackends.add(entry.getKey().getId());
                }
            }
        }

        Assert.assertEquals(1, pathABackends.size());
    }

    @Test
    public void testNewFileBatchDoesNotRedistributeFromPreviousBatch() throws UserException {
        SystemInfoService service = new SystemInfoService();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(20700L + i, "172.30.2." + (100 + i), 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        ComputeGroupMgr computeGroupMgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);

        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        List<Split> firstBatch = new ArrayList<>();
        for (int i = 0; i < Config.split_assigner_max_split_num_variance + 3; i++) {
            firstBatch.add(fileSplit("s3://bucket/table/a.parquet", i * 128L, 128, 4096));
        }
        Assert.assertEquals(firstBatch.size(), policy.computeScanRangeAssignment(firstBatch).size());

        long firstOwner = ownerId(policy.computeScanRangeAssignment(
                Collections.singletonList(fileSplit("s3://bucket/table/a.parquet", 0, 128, 4096))));
        String secondPath = null;
        FederationBackendPolicy probePolicy = new FederationBackendPolicy();
        probePolicy.init();
        for (int fileIndex = 0; fileIndex < 100; fileIndex++) {
            String candidate = "s3://bucket/table/b-" + fileIndex + ".parquet";
            long candidateOwner = ownerId(probePolicy.computeScanRangeAssignment(
                    Collections.singletonList(fileSplit(candidate, 0, 128, 256))));
            if (candidateOwner != firstOwner) {
                secondPath = candidate;
                break;
            }
        }
        Assert.assertNotNull(secondPath);

        Multimap<Backend, Split> secondAssignment = policy.computeScanRangeAssignment(
                Collections.singletonList(fileSplit(secondPath, 0, 128, 256)));
        Assert.assertEquals(1, secondAssignment.size());
        Assert.assertNotEquals(firstOwner, ownerId(secondAssignment));
    }

    @Test
    public void testDifferentFilesRemainDistributedAcrossBackends() throws UserException {
        SystemInfoService service = new SystemInfoService();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(21000L + i, "172.31.0." + (100 + i), 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        ComputeGroupMgr computeGroupMgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);

        List<Split> splits = new ArrayList<>();
        for (int fileIndex = 0; fileIndex < 6; fileIndex++) {
            String path = "s3://bucket/table/file-" + fileIndex + ".parquet";
            splits.add(fileSplit(path, 0, 128, 256));
            splits.add(fileSplit(path, 128, 128, 256));
        }

        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        Assert.assertEquals(12, assignment.size());
        Assert.assertTrue(assignment.keySet().size() > 1);
        Map<String, Set<Long>> backendsPerFile = new HashMap<>();
        for (Map.Entry<Backend, Split> entry : assignment.entries()) {
            backendsPerFile.computeIfAbsent(entry.getValue().getPathString(), key -> new HashSet<>())
                    .add(entry.getKey().getId());
        }
        backendsPerFile.values().forEach(backendIds -> Assert.assertEquals(1, backendIds.size()));
    }

    @Test
    public void testVirtualSplitsWithDummyPathRemainDistributed() throws UserException {
        SystemInfoService service = new SystemInfoService();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(22000L + i, "172.32.0." + (100 + i), 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        ComputeGroupMgr computeGroupMgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);

        List<Split> splits = new ArrayList<>();
        for (int splitIndex = 0; splitIndex < 6; splitIndex++) {
            splits.add(new RemoteDorisSplit("remote-" + splitIndex, ByteBuffer.allocate(0)));
        }

        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        Assert.assertEquals(6, assignment.size());
        Assert.assertTrue(assignment.keySet().size() > 1);
    }

    @Test
    public void testPluginVirtualPathDoesNotEnableFileAffinity() {
        PluginDrivenSplit split = pluginSplit("/byte_size", 0, false);
        Assert.assertFalse(split.getFileAffinityKey().isPresent());
    }

    @Test
    public void testPluginPhysicalFileEnablesFileAffinity() {
        PluginDrivenSplit split = pluginSplit("s3a://bucket/table/data.parquet", 0, true);
        split.setFileAffinitySupported(true);
        Assert.assertEquals(split.getPathString(), split.getFileAffinityKey().get());
    }

    @Test
    public void testPluginPhysicalFileRequiresFileAffinityEnablement() {
        PluginDrivenSplit split = pluginSplit("s3a://bucket/table/data.parquet", 0, true);
        Assert.assertFalse(split.getFileAffinityKey().isPresent());
    }

    @Test
    public void testFileAffinityDoesNotOverrideBlockLocality() {
        FileSplit first = new FileSplit(LocationPath.of("hdfs://namenode/table/data.parquet"),
                0, 128, 256, 0, new String[] {"host-a"}, Collections.emptyList());
        FileSplit second = new FileSplit(LocationPath.of("hdfs://namenode/table/data.parquet"),
                128, 128, 256, 0, new String[] {"host-b"}, Collections.emptyList());
        SplitAssignment.enableFileAffinity(Arrays.asList(first, second), true);
        Assert.assertFalse(first.getFileAffinityKey().isPresent());
        Assert.assertFalse(second.getFileAffinityKey().isPresent());
    }

    @Test
    public void testFileAffinityRequiresSupportedFormat() {
        FileSplit split = new FileSplit(LocationPath.of("s3://bucket/table/data-without-extension"),
                0, 128, 256, 0, null, Collections.emptyList());
        Assert.assertFalse(split.getFileAffinityKey().isPresent());
        SplitAssignment.enableFileAffinity(Collections.singletonList(split), true);
        Assert.assertTrue(split.getFileAffinityKey().isPresent());
        SplitAssignment.enableFileAffinity(Collections.singletonList(split), false);
        Assert.assertFalse(split.getFileAffinityKey().isPresent());
    }

    @Test
    public void testVirtualNodeNumberMustBePositiveAndCanRecover() throws UserException {
        SystemInfoService service = new SystemInfoService();
        Backend backend = new Backend(22400L, "172.32.1.100", 9050);
        backend.setAlive(true);
        service.addBackend(backend);
        mockEnv(service);

        int originalVirtualNodeNumber = Config.split_assigner_virtual_node_number;
        try {
            Config.split_assigner_virtual_node_number = 0;
            UserException zeroException = Assert.assertThrows(UserException.class,
                    () -> new FederationBackendPolicy().init());
            Assert.assertTrue(zeroException.getMessage().contains("split_assigner_virtual_node_number"));

            Config.split_assigner_virtual_node_number = -1;
            UserException negativeException = Assert.assertThrows(UserException.class,
                    () -> new FederationBackendPolicy().init());
            Assert.assertTrue(negativeException.getMessage().contains("must be positive"));

            Config.split_assigner_virtual_node_number = 1;
            FederationBackendPolicy recoveredPolicy = new FederationBackendPolicy();
            recoveredPolicy.init();
            Assert.assertEquals(1, recoveredPolicy.computeScanRangeAssignment(Collections.singletonList(
                    fileSplit("s3://bucket/table/recovered.parquet", 0, 128, 256))).size());
        } finally {
            Config.split_assigner_virtual_node_number = originalVirtualNodeNumber;
        }
    }

    @Test
    public void testConsistentHashCacheIncludesVirtualNodeNumber() throws UserException {
        SystemInfoService service = new SystemInfoService();
        Backend backend = new Backend(22450L, "172.32.2.100", 9050);
        backend.setAlive(true);
        service.addBackend(backend);
        mockEnv(service);

        int originalVirtualNodeNumber = Config.split_assigner_virtual_node_number;
        try {
            Config.split_assigner_virtual_node_number = 1;
            TestFederationBackendPolicy first = new TestFederationBackendPolicy();
            first.init();

            Config.split_assigner_virtual_node_number = 2;
            TestFederationBackendPolicy second = new TestFederationBackendPolicy();
            second.init();
            TestFederationBackendPolicy third = new TestFederationBackendPolicy();
            third.init();

            Assert.assertNotSame(first.getConsistentHash(), second.getConsistentHash());
            Assert.assertSame(second.getConsistentHash(), third.getConsistentHash());
        } finally {
            Config.split_assigner_virtual_node_number = originalVirtualNodeNumber;
        }
    }

    @Test
    public void testRedistributionKeepsFileAffinitySplitsFixed() throws UserException {
        SystemInfoService service = new SystemInfoService();
        for (int i = 0; i < 3; i++) {
            Backend backend = new Backend(22500L + i, "172.33.0." + (100 + i), 9050);
            backend.setAlive(true);
            service.addBackend(backend);
        }

        ComputeGroupMgr computeGroupMgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(computeGroupMgr);

        String path = "s3://bucket/table/large.parquet";
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            splits.add(fileSplit(path, i * 128L, 128, 1152));
        }
        for (int i = 0; i < 6; i++) {
            splits.add(new RemoteDorisSplit("remote-" + i, ByteBuffer.allocate(0)));
        }

        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        Set<Long> fileBackendIds = new HashSet<>();
        Set<Long> movableBackendIds = new HashSet<>();
        Set<Split> assignedSplits = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Backend, Long> finalWeights = new HashMap<>();
        for (Map.Entry<Backend, Split> entry : assignment.entries()) {
            if (path.equals(entry.getValue().getPathString())) {
                fileBackendIds.add(entry.getKey().getId());
            } else {
                movableBackendIds.add(entry.getKey().getId());
            }
            Assert.assertTrue(assignedSplits.add(entry.getValue()));
            finalWeights.merge(entry.getKey(), entry.getValue().getSplitWeight().getRawValue(), Long::sum);
        }
        Assert.assertEquals(1, fileBackendIds.size());
        Assert.assertTrue(movableBackendIds.size() > 1);
        Assert.assertEquals(splits.size(), assignedSplits.size());
        Assert.assertTrue(assignedSplits.containsAll(splits));
        for (Backend backend : policy.getBackends()) {
            Assert.assertEquals(finalWeights.getOrDefault(backend, 0L),
                    policy.getAssignedWeightPerBackend().get(backend));
        }
        Assert.assertEquals(15, assignment.size());
    }

    private static long ownerId(Multimap<Backend, Split> assignment) {
        Assert.assertEquals(1, assignment.keySet().size());
        return assignment.keySet().iterator().next().getId();
    }

    private void mockEnv(SystemInfoService service) {
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(new ComputeGroupMgr(service));
    }

    private static class TestFederationBackendPolicy extends FederationBackendPolicy {
        private Object getConsistentHash() {
            return consistentHash;
        }
    }

    private static FileSplit fileSplit(String path, long start, long length, long fileLength) {
        FileSplit split = new FileSplit(LocationPath.of(path), start, length, fileLength,
                0, null, Collections.emptyList());
        split.setFileAffinitySupported(true);
        return split;
    }

    private static PluginDrivenSplit pluginSplit(String path, long start, boolean physicalFile) {
        ConnectorScanRange range = new ConnectorScanRange() {
            @Override
            public Optional<String> getPath() {
                return Optional.of(path);
            }

            @Override
            public boolean isNativeReadRange() {
                return physicalFile;
            }

            @Override
            public String getFileFormat() {
                return physicalFile ? "parquet" : "jni";
            }

            @Override
            public long getStart() {
                return start;
            }

            @Override
            public long getLength() {
                return 128;
            }

            @Override
            public long getFileSize() {
                return 256;
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }
        };
        return new PluginDrivenSplit(range);
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

        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 112140970, 112140970, 0, new String[]{"172.30.0.100"}, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 95795997, 95795997, 0, new String[]{"172.30.0.106"}, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 105664025, 105664025, 0, null, Collections.emptyList()));

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
                if (fileSplit.getPath().getNormalizedLocation().equals("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc")) {
                    Assert.assertEquals("172.30.0.100", backend.getHost());
                    checkedLocalSplit.add(true);
                } else if (fileSplit.getPath().getNormalizedLocation().equals("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc")) {
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

        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 105664025, 105664025, 0, null, Collections.emptyList()));

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
        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

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
            FileSplit split = new FileSplit(LocationPath.of("hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()), 0, splitLength, splitLength, 0, null, Collections.emptyList());
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
            FileSplit split = new FileSplit(LocationPath.of("hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()), 0, localSplitLength, localSplitLength, 0, localHosts.toArray(new String[0]), Collections.emptyList());
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
        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

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
            FileSplit split = new FileSplit(LocationPath.of("hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()), 0, splitLength, splitLength, 0, null, Collections.emptyList());
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
            FileSplit split = new FileSplit(LocationPath.of("hdfs://HDFS00001/usr/hive/warehouse/test.db/test_table/" + UUID.randomUUID()), 0, localSplitLength, localSplitLength, 0, localHosts.toArray(new String[0]), Collections.emptyList());
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

        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

        List<Split> splits = new ArrayList<>();
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 112140970, 112140970, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 120839661, 120839661, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 108897409, 108897409, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 95795997, 95795997, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 104600402, 104600402, 0, null, Collections.emptyList()));
        splits.add(new FileSplit(LocationPath.of("hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc"), 0, 105664025, 105664025, 0, null, Collections.emptyList()));

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
                    originSplitAssignedBackends.put(new TestSplitHashKey(split.getPathString(), split.getStart(), split.getLength()), backend);
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
                    Backend origin = originSplitAssignedBackends.get(new TestSplitHashKey(split.getPathString(), split.getStart(), split.getLength()));
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
                    Backend origin = originSplitAssignedBackends.get(new TestSplitHashKey(split.getPathString(), split.getStart(), split.getLength()));
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

    private static <K, V> boolean areMultimapsEqualIgnoringOrder(Multimap<K, V> multimap1, Multimap<K, V> multimap2) {
        Collection<Map.Entry<K, V>> entries1 = multimap1.entries();
        Collection<Map.Entry<K, V>> entries2 = multimap2.entries();
        return entries1.containsAll(entries2) && entries2.containsAll(entries1);
    }

    @Test
    public void testSplitWeight() {
        FileSplit fileSplit = new FileSplit(LocationPath.of("s1"), 0, 1000, 1000, 0, null, Collections.emptyList());
        fileSplit.setSelfSplitWeight(1000L);

        fileSplit.setTargetSplitSize(10L);
        Assert.assertEquals(100L, fileSplit.getSplitWeight().getRawValue(), 100L);

        fileSplit.setTargetSplitSize(10000000L);
        Assert.assertEquals(1L, fileSplit.getSplitWeight().getRawValue());

        fileSplit.setTargetSplitSize(2000L);
        Assert.assertEquals(50, fileSplit.getSplitWeight().getRawValue());
    }

    // Regression for the NPE in testGenerateRandomly: FileSplit is Lombok @Data, whose generated
    // equals()/hashCode() invoke getSelfSplitWeight(). A split that never sets a size-based weight
    // leaves selfSplitWeight null, so the getter must surface the "-1 = not provided" sentinel
    // instead of unboxing null (which threw NPE during the multimap comparison).
    @Test
    public void testFileSplitEqualsHashCodeWithUnsetWeight() {
        LocationPath path = LocationPath.of("s1");
        // Two distinct instances that share the same LocationPath are field-equal, so equals()
        // proceeds past the identity short-circuit and exercises getSelfSplitWeight().
        FileSplit a = new FileSplit(path, 0, 1000, 1000, 0, null, Collections.emptyList());
        FileSplit b = new FileSplit(path, 0, 1000, 1000, 0, null, Collections.emptyList());
        Assert.assertEquals(-1L, a.getSelfSplitWeight());
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testBiggerSplit() throws UserException {
        SystemInfoService service = new SystemInfoService();

        Backend backend1 = new Backend(1L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(2L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(3L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        ComputeGroupMgr cgmgr = new ComputeGroupMgr(service);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(service);
        Mockito.when(env.getComputeGroupMgr()).thenReturn(cgmgr);

        List<Split> splits = new ArrayList<>();
        splits.add(genFileSplit("s1", 1000000L, 1000L)); // belong 2
        splits.add(genFileSplit("s2", 100000L, 1000L));  // belong 2
        splits.add(genFileSplit("s3", 200000L, 1000L));  // belong 2
        splits.add(genFileSplit("s4", 300000L, 1000L));  // belong 2
        splits.add(genFileSplit("s5", 800000L, 1000L));  // belong 1

        FederationBackendPolicy policy = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        // Set these options to ensure that the consistent hash algorithm is consistent.
        policy.setEnableSplitsRedistribution(false);
        Config.split_assigner_min_consistent_hash_candidate_num = 1;
        policy.init();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);
        Map<Backend, List<Split>> backendListMap = mergeAssignment(assignment);
        backendListMap.forEach((k, v) -> {
            if (k.getId() == 1) {
                Assert.assertEquals(800000, v.stream().mapToLong(Split::getLength).sum());
            } else if (k.getId() == 2) {
                Assert.assertEquals(1600000, v.stream().mapToLong(Split::getLength).sum());
            }
        });

        Config.split_assigner_min_consistent_hash_candidate_num = 1;
        FederationBackendPolicy policy2 = new FederationBackendPolicy(NodeSelectionStrategy.CONSISTENT_HASHING);
        policy2.init();
        Multimap<Backend, Split> assignment2 = policy2.computeScanRangeAssignment(splits);
        Map<Backend, List<Split>> backendListMap2 = mergeAssignment(assignment2);
        backendListMap2.forEach((k, v) -> {
            if (k.getId() == 1) {
                Assert.assertEquals(1000000L, v.stream().mapToLong(Split::getLength).sum());
            } else if (k.getId() == 2) {
                Assert.assertEquals(400000L, v.stream().mapToLong(Split::getLength).sum());
            } else if (k.getId() == 3) {
                Assert.assertEquals(1000000L, v.stream().mapToLong(Split::getLength).sum());
            }
        });
    }

    private Map<Backend, List<Split>> mergeAssignment(Multimap<Backend, Split> ass) {
        HashMap<Backend, List<Split>> map = new HashMap<>();
        ass.forEach((k, v) -> {
            if (map.containsKey(k)) {
                map.get(k).add(v);
            } else {
                ArrayList<Split> splits = new ArrayList<>();
                splits.add(v);
                map.put(k, splits);
            }
        });
        return map;
    }

    private FileSplit genFileSplit(String path, long length, long targetSplit) {
        FileSplit s = new FileSplit(LocationPath.of(path), 0, length, length, 0, null, Collections.emptyList());
        s.setSelfSplitWeight(length);
        s.setTargetSplitSize(targetSplit);
        return s;
    }
}
