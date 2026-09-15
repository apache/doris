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

package org.apache.doris.load;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTabletLocation;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class StreamLoadHandlerTest {
    @Test
    public void testSelectBackendSkipsDecommissioningBackend() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        Backend decommissioningBackend = createBackend(10001L, "127.0.0.1");
        decommissioningBackend.setDecommissioning(true);
        Backend selectedBackend = createBackend(10002L, "127.0.0.2");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(decommissioningBackend, selectedBackend));

        try {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);

            Assertions.assertEquals(selectedBackend.getId(), StreamLoadHandler.selectBackend("cluster0").getId());
        } finally {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testSetCloudClusterUsesBackendComputeGroup() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend backend = createBackend(10001L, "127.0.0.1");
        backend.setCloudClusterName("backend_compute_group");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(backend));
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("");
        request.setBackendId(backend.getId());
        request.setCloudCluster("header_compute_group");

        try {
            Config.cloud_unique_id = "test_cloud_unique_id";
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            ConnectContext.remove();

            StreamLoadHandler handler = new StreamLoadHandler(
                    request, null, new TStreamLoadPutResult(), "127.0.0.1");
            handler.setCloudCluster();

            Assertions.assertEquals("backend_compute_group",
                    ConnectContext.get().getSessionVariable().getCloudCluster());
            Assertions.assertEquals("backend_compute_group", request.getCloudCluster());
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testGroupCommitValidatesBackendComputeGroupPrivilege() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend backend = createBackend(10001L, "127.0.0.1");
        backend.setCloudClusterName("backend_compute_group");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(backend));
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(cloudEnv.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(cloudEnv.getAuth()).thenReturn(auth);
        Mockito.doAnswer(invocation -> {
            List<UserIdentity> currentUser = invocation.getArgument(3);
            currentUser.add(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            return null;
        }).when(auth).checkPlainPassword(Mockito.eq("test_user"), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyList());
        Mockito.doThrow(new DdlException("USAGE denied"))
                .when(cloudEnv).changeCloudCluster(
                        Mockito.eq("backend_compute_group"), Mockito.any(ConnectContext.class));

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("test_user");
        request.setUserIp("127.0.0.1");
        request.setPasswd("test_password");
        request.setBackendId(backend.getId());
        request.setCloudCluster("header_compute_group");
        request.setGroupCommitMode("sync_mode");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test_cloud_unique_id";
            mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            ConnectContext.remove();

            StreamLoadHandler handler = new StreamLoadHandler(
                    request, null, new TStreamLoadPutResult(), "127.0.0.1");
            try {
                handler.setCloudCluster();
                Assertions.fail("group commit should validate compute group privilege");
            } catch (DdlException e) {
                Assertions.assertTrue(e.getMessage().contains("USAGE denied"));
            }

            Mockito.verify(cloudEnv).changeCloudCluster(
                    Mockito.eq("backend_compute_group"), Mockito.any(ConnectContext.class));
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testAssignAdaptiveRandomBucketRoutesPartitionToTabletOwner() {
        // The load runs on backend 20, but the only tablet of the partition lives on backend 10,
        // so the sink has to route the partition to backend 10 instead of assuming self ownership.
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(20L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult(), "127.0.0.1");

        TPipelineFragmentParams params = buildStreamLoadParams(createSingleBucketSink());
        handler.assignAdaptiveRandomBucket(params);

        TOlapTablePartition partition = getSinglePartition(params);
        Assertions.assertEquals(10L, partition.getBucketBeId());
        Assertions.assertEquals(0L, partition.getLoadTabletIdx());
        Assertions.assertEquals(Lists.newArrayList(0), partition.getLocalBucketSeqs());
        Assertions.assertEquals(10L, partition.getIndexes().get(0).getBucketBeId());
        Assertions.assertEquals(Lists.newArrayList(0), partition.getIndexes().get(0).getLocalBucketSeqs());
    }

    @Test
    public void testAssignAdaptiveRandomBucketKeepsLocalBucketWhenSinkOwnsTablet() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(10L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult(), "127.0.0.1");

        TPipelineFragmentParams params = buildStreamLoadParams(createSingleBucketSink());
        handler.assignAdaptiveRandomBucket(params);

        TOlapTablePartition partition = getSinglePartition(params);
        Assertions.assertEquals(10L, partition.getBucketBeId());
        Assertions.assertEquals(Lists.newArrayList(0), partition.getLocalBucketSeqs());
    }

    @Test
    public void testAssignAdaptiveRandomBucketFallsBackWithoutBackendId() {
        // An old client does not report the executing backend, so no assignment consistent with the
        // receiver side can be computed. Adaptive mode must be turned off to keep the legacy routing.
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setDb("test_db");
        request.setTbl("test_tbl");
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult(), "127.0.0.1");

        TPipelineFragmentParams params = buildStreamLoadParams(createSingleBucketSink());
        handler.assignAdaptiveRandomBucket(params);

        TOlapTableSink sink = params.getFragment().getOutputSink().getOlapTableSink();
        Assertions.assertFalse(sink.isSetEnableAdaptiveRandomBucket());
        TOlapTablePartition partition = getSinglePartition(params);
        Assertions.assertFalse(partition.isSetBucketBeId());
        Assertions.assertFalse(partition.getIndexes().get(0).isSetBucketBeId());
    }

    @Test
    public void testAssignAdaptiveRandomBucketIgnoresNonOlapTableSink() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setBackendId(20L);
        StreamLoadHandler handler = new StreamLoadHandler(
                request, null, new TStreamLoadPutResult(), "127.0.0.1");

        TPipelineFragmentParams params = new TPipelineFragmentParams();
        TPlanFragment fragment = new TPlanFragment();
        fragment.setOutputSink(new TDataSink(TDataSinkType.DATA_STREAM_SINK));
        params.setFragment(fragment);

        handler.assignAdaptiveRandomBucket(params);
        Assertions.assertNull(params.getFragment().getOutputSink().getOlapTableSink());
    }

    /**
     * Creates a random distribution sink with one bucket, whose only tablet is located on backend 10.
     */
    private static TOlapTableSink createSingleBucketSink() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1000L);
        partition.setNumBuckets(1);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Lists.newArrayList(100L)));
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.addToPartitions(partition);

        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        locationParam.addToTablets(new TTabletLocation(100L, Lists.newArrayList(10L)));

        TOlapTableSink sink = new TOlapTableSink();
        sink.setEnableAdaptiveRandomBucket(true);
        sink.setLoadToSingleTablet(false);
        sink.setPartition(partitionParam);
        sink.setLocation(locationParam);
        return sink;
    }

    private static TPipelineFragmentParams buildStreamLoadParams(TOlapTableSink sink) {
        TPipelineFragmentParams params = new TPipelineFragmentParams();
        TPlanFragment fragment = new TPlanFragment();
        TDataSink tDataSink = new TDataSink(TDataSinkType.OLAP_TABLE_SINK);
        tDataSink.setOlapTableSink(sink);
        fragment.setOutputSink(tDataSink);
        params.setFragment(fragment);
        return params;
    }

    private static TOlapTablePartition getSinglePartition(TPipelineFragmentParams params) {
        return params.getFragment().getOutputSink().getOlapTableSink()
                .getPartition().getPartitions().get(0);
    }

    private Backend createBackend(long id, String host) {
        Backend backend = new Backend(id, host, 9050);
        backend.setAlive(true);
        return backend;
    }

    private static class TestCloudSystemInfoService extends CloudSystemInfoService {
        private final List<Backend> backends;

        private TestCloudSystemInfoService(List<Backend> backends) {
            this.backends = backends;
        }

        @Override
        public List<Backend> getBackendsByClusterName(final String clusterName) {
            return backends;
        }

        @Override
        public Backend getBackend(long backendId) {
            return backends.stream().filter(backend -> backend.getId() == backendId).findFirst().orElse(null);
        }
    }
}
