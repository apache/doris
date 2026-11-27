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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class CloudMetadataSynchronizerTest {
    private CloudEnv env;
    private CloudSystemInfoService service;
    private CloudClusterChecker clusterChecker;
    private CloudInstanceStatusChecker instanceChecker;
    private CloudMetadataSynchronizer synchronizer;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    void setUp() {
        env = Mockito.mock(CloudEnv.class);
        service = Mockito.mock(CloudSystemInfoService.class);
        clusterChecker = Mockito.mock(CloudClusterChecker.class);
        instanceChecker = Mockito.mock(CloudInstanceStatusChecker.class);
        synchronizer = new CloudMetadataSynchronizer(service, clusterChecker, instanceChecker);
        mockedEnv = mockEnv();
    }

    private MockedStatic<Env> mockEnv() {
        MockedStatic<Env> mock = Mockito.mockStatic(Env.class);
        mock.when(Env::getCurrentEnv).thenReturn(env);
        return mock;
    }

    @AfterEach
    void tearDown() {
        mockedEnv.close();
    }

    @Test
    void testMasterAppliesOneSnapshotInDependencyOrder() {
        Mockito.when(env.isMaster()).thenReturn(true);
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenReturn(response(instance));

        synchronizer.runAfterCatalogReady();

        InOrder order = Mockito.inOrder(service, clusterChecker, instanceChecker);
        order.verify(service).getCloudInstance();
        order.verify(clusterChecker).check(Mockito.same(instance));
        order.verify(instanceChecker).check(Mockito.same(instance));
        Mockito.verifyNoMoreInteractions(service, clusterChecker, instanceChecker);
    }

    @Test
    void testNonMasterOnlyRefreshesLocalState() {
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenReturn(response(instance));

        synchronizer.runAfterCatalogReady();

        Mockito.verify(instanceChecker).check(Mockito.same(instance));
        Mockito.verifyNoInteractions(clusterChecker);
        Mockito.verify(service).getCloudInstance();
        Mockito.verifyNoMoreInteractions(service);
    }

    @Test
    void testRoleIsCheckedOnEveryCycle() {
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenReturn(response(instance));
        Mockito.when(env.isMaster()).thenReturn(false, true, false);

        synchronizer.runAfterCatalogReady();
        synchronizer.runAfterCatalogReady();
        synchronizer.runAfterCatalogReady();

        Mockito.verify(clusterChecker).check(instance);
        Mockito.verify(instanceChecker, Mockito.times(3)).check(instance);
    }

    @Test
    void testIncompleteOrFailedResponseDoesNotRemoveMetadata() {
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(service.getCloudInstance()).thenReturn(null,
                Cloud.GetInstanceResponse.getDefaultInstance(),
                Cloud.GetInstanceResponse.newBuilder().setStatus(
                        Cloud.MetaServiceResponseStatus.getDefaultInstance()).build(),
                Cloud.GetInstanceResponse.newBuilder().setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.CLUSTER_NOT_FOUND)).build(),
                Cloud.GetInstanceResponse.newBuilder().setStatus(ok()).build());

        for (int i = 0; i < 5; i++) {
            synchronizer.runAfterCatalogReady();
        }

        Mockito.verifyNoInteractions(clusterChecker, instanceChecker);
    }

    @Test
    void testFetchFailureRetriesOnNextCycle() {
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenThrow(new RuntimeException("MS unavailable"))
                .thenReturn(response(instance));

        synchronizer.runAfterCatalogReady();
        Mockito.verifyNoInteractions(clusterChecker, instanceChecker);
        synchronizer.runAfterCatalogReady();

        Mockito.verify(instanceChecker).check(instance);
    }

    @Test
    void testNodeFailureDoesNotBlockInstanceRefresh() {
        Mockito.when(env.isMaster()).thenReturn(true);
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenReturn(response(instance));
        Mockito.doThrow(new RuntimeException("node reconciliation failed")).when(clusterChecker).check(instance);

        synchronizer.runAfterCatalogReady();

        Mockito.verify(instanceChecker).check(instance);
    }

    @Test
    void testInstanceRefreshFailureRetriesOnNextCycle() {
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenReturn(response(instance));
        Mockito.doThrow(new RuntimeException("instance refresh failed")).doNothing()
                .when(instanceChecker).check(instance);

        synchronizer.runAfterCatalogReady();
        synchronizer.runAfterCatalogReady();

        Mockito.verify(instanceChecker, Mockito.times(2)).check(instance);
    }

    @Test
    void testCheckNowUsesSameRefreshPath() {
        Mockito.when(env.isMaster()).thenReturn(true);
        Cloud.InstanceInfoPB instance = instance("cg");
        Mockito.when(service.getCloudInstance()).thenReturn(response(instance));

        synchronizer.checkNow();

        InOrder order = Mockito.inOrder(service, clusterChecker, instanceChecker);
        order.verify(service).getCloudInstance();
        order.verify(clusterChecker).check(instance);
        order.verify(instanceChecker).check(instance);
    }

    @Test
    void testCheckNowDoesNothingOnNonMaster() {
        synchronizer.checkNow();

        Mockito.verifyNoInteractions(service, clusterChecker, instanceChecker);
    }

    @Test
    void testManualRefreshCannotOvertakeFetchedSnapshot() throws Exception {
        Mockito.when(env.isMaster()).thenReturn(true);
        Cloud.InstanceInfoPB first = instance("old");
        Cloud.InstanceInfoPB second = instance("new");
        CountDownLatch fetched = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch manualStarted = new CountDownLatch(1);
        CountDownLatch fetchedAgain = new CountDownLatch(1);
        Mockito.when(service.getCloudInstance()).thenAnswer(invocation -> {
            fetched.countDown();
            Assertions.assertTrue(release.await(30, TimeUnit.SECONDS));
            return response(first);
        }).thenAnswer(invocation -> {
            fetchedAgain.countDown();
            return response(second);
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> periodic = executor.submit(() -> {
                try (MockedStatic<Env> ignored = mockEnv()) {
                    synchronizer.runAfterCatalogReady();
                }
            });
            Assertions.assertTrue(fetched.await(30, TimeUnit.SECONDS));
            Future<?> manual = executor.submit(() -> {
                try (MockedStatic<Env> ignored = mockEnv()) {
                    manualStarted.countDown();
                    synchronizer.checkNow();
                }
            });
            Assertions.assertTrue(manualStarted.await(30, TimeUnit.SECONDS));
            Assertions.assertFalse(fetchedAgain.await(100, TimeUnit.MILLISECONDS));
            release.countDown();
            periodic.get(30, TimeUnit.SECONDS);
            manual.get(30, TimeUnit.SECONDS);

            InOrder order = Mockito.inOrder(service, clusterChecker, instanceChecker);
            order.verify(service).getCloudInstance();
            order.verify(clusterChecker).check(first);
            order.verify(instanceChecker).check(first);
            order.verify(service).getCloudInstance();
            order.verify(clusterChecker).check(second);
            order.verify(instanceChecker).check(second);
            Mockito.verifyNoMoreInteractions(service, clusterChecker, instanceChecker);
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private Cloud.InstanceInfoPB instance(String clusterId) {
        return Cloud.InstanceInfoPB.newBuilder().setStatus(Cloud.InstanceInfoPB.Status.NORMAL)
                .addClusters(Cloud.ClusterPB.newBuilder().setClusterId(clusterId)).build();
    }

    private Cloud.GetInstanceResponse response(Cloud.InstanceInfoPB instance) {
        return Cloud.GetInstanceResponse.newBuilder().setStatus(ok()).setInstance(instance).build();
    }

    private Cloud.MetaServiceResponseStatus ok() {
        return Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK).build();
    }
}
