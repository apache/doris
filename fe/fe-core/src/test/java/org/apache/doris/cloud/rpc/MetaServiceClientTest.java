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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.MetaServiceGrpc;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

public class MetaServiceClientTest {
    private String originCloudUniqueId;
    private MetaServiceClient client;
    private MetaServiceGrpc.MetaServiceBlockingStub blockingStub;

    @BeforeEach
    public void setUp() {
        originCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "1:instance_id:fe_node";

        blockingStub = Mockito.mock(MetaServiceGrpc.MetaServiceBlockingStub.class);
        Mockito.when(blockingStub.withDeadlineAfter(Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenReturn(blockingStub);
        client = Mockito.mock(MetaServiceClient.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(client, "blockingStub", blockingStub);
    }

    @AfterEach
    public void tearDown() {
        Config.cloud_unique_id = originCloudUniqueId;
    }

    private Cloud.AlterClusterRequest sentAlterCluster(Cloud.AlterClusterRequest request) {
        ArgumentCaptor<Cloud.AlterClusterRequest> sent = ArgumentCaptor.forClass(Cloud.AlterClusterRequest.class);
        Mockito.when(blockingStub.alterCluster(sent.capture()))
                .thenReturn(Cloud.AlterClusterResponse.getDefaultInstance());
        client.alterCluster(request);
        return sent.getValue();
    }

    private Cloud.AlterInstanceRequest sentAlterInstance(Cloud.AlterInstanceRequest request) {
        ArgumentCaptor<Cloud.AlterInstanceRequest> sent = ArgumentCaptor.forClass(Cloud.AlterInstanceRequest.class);
        Mockito.when(blockingStub.alterInstance(sent.capture()))
                .thenReturn(Cloud.AlterInstanceResponse.getDefaultInstance());
        client.alterInstance(request);
        return sent.getValue();
    }

    @Test
    public void testFillCloudUniqueIdForInstanceIdRequest() {
        Cloud.AlterClusterRequest alterCluster = Cloud.AlterClusterRequest.newBuilder()
                .setInstanceId("instance_id").setOp(Cloud.AlterClusterRequest.Operation.ADD_NODE).build();
        Cloud.AlterClusterRequest sentCluster = sentAlterCluster(alterCluster);
        Assertions.assertEquals("instance_id", sentCluster.getInstanceId());
        Assertions.assertEquals("1:instance_id:fe_node", sentCluster.getCloudUniqueId());

        Cloud.AlterInstanceRequest alterInstance = Cloud.AlterInstanceRequest.newBuilder()
                .setInstanceId("instance_id")
                .setOp(Cloud.AlterInstanceRequest.Operation.SET_SNAPSHOT_PROPERTY).build();
        Cloud.AlterInstanceRequest sentInstance = sentAlterInstance(alterInstance);
        Assertions.assertEquals("instance_id", sentInstance.getInstanceId());
        Assertions.assertEquals("1:instance_id:fe_node", sentInstance.getCloudUniqueId());
    }

    @Test
    public void testKeepGivenCloudUniqueId() {
        Cloud.AlterClusterRequest request = Cloud.AlterClusterRequest.newBuilder()
                .setCloudUniqueId("1:instance_id:other_node").build();
        Assertions.assertEquals("1:instance_id:other_node", sentAlterCluster(request).getCloudUniqueId());
    }

    @Test
    public void testFillCloudUniqueIdDerivedFromClusterId() {
        Config.cloud_unique_id = "1:12345678:fe";
        Cloud.AlterClusterRequest alterCluster = Cloud.AlterClusterRequest.newBuilder()
                .setInstanceId("12345678").setOp(Cloud.AlterClusterRequest.Operation.ADD_NODE).build();
        Cloud.AlterInstanceRequest alterInstance = Cloud.AlterInstanceRequest.newBuilder()
                .setInstanceId("12345678").setOp(Cloud.AlterInstanceRequest.Operation.SET_SNAPSHOT_PROPERTY).build();

        Assertions.assertEquals(Config.cloud_unique_id, sentAlterCluster(alterCluster).getCloudUniqueId());
        Assertions.assertEquals(Config.cloud_unique_id, sentAlterInstance(alterInstance).getCloudUniqueId());
    }
}
