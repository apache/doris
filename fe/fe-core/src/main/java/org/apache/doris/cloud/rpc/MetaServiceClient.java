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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.NameResolverRegistry;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MetaServiceClient {
    public static final Logger LOG = LogManager.getLogger(MetaServiceClient.class);

    private final String address;
    private final MetaServiceGrpc.MetaServiceFutureStub stub;
    private final MetaServiceGrpc.MetaServiceBlockingStub blockingStub;
    private final ManagedChannel channel;
    private final long expiredAt;
    private final boolean isMetaServiceEndpointList;

    static {
        NameResolverRegistry.getDefaultRegistry().register(new MetaServiceListResolverProvider());
    }

    public MetaServiceClient(String address) {
        this.address = address;

        isMetaServiceEndpointList = address.contains(",");

        String target = address;
        if (isMetaServiceEndpointList) {
            target = MetaServiceListResolverProvider.MS_LIST_SCHEME_PREFIX + address;
        }
        channel = NettyChannelBuilder.forTarget(target)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes)
                .defaultServiceConfig(getRetryingServiceConfig())
                .defaultLoadBalancingPolicy("round_robin")
                .enableRetry()
                .usePlaintext().build();
        stub = MetaServiceGrpc.newFutureStub(channel);
        blockingStub = MetaServiceGrpc.newBlockingStub(channel);
        expiredAt = connectionAgeExpiredAt();
    }

    private long connectionAgeExpiredAt() {
        long connectionAgeBase = Config.meta_service_connection_age_base_minutes;
        // Disable connection age if the endpoint is a list.
        if (!isMetaServiceEndpointList && connectionAgeBase > 1) {
            long base = TimeUnit.MINUTES.toMillis(connectionAgeBase);
            return base + System.currentTimeMillis() % base;
        }
        return Long.MAX_VALUE;
    }

    protected Map<String, ?> getRetryingServiceConfig() {
        // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy-capabilities
        Map<String, ?> serviceConfig = new Gson().fromJson(new JsonReader(new InputStreamReader(
                MetaServiceClient.class.getResourceAsStream("/retrying_service_config.json"),
                StandardCharsets.UTF_8)), Map.class);
        LOG.info("serviceConfig:{}", serviceConfig);
        return serviceConfig;
    }

    // Is the connection age has expired?
    public boolean isConnectionAgeExpired() {
        return Config.meta_service_connection_age_base_minutes > 0
                && expiredAt < System.currentTimeMillis();
    }

    // Is the underlying channel in a normal state? (That means the RPC call will
    // not fail immediately)
    public boolean isNormalState() {
        ConnectivityState state = channel.getState(false);
        return state == ConnectivityState.CONNECTING
                || state == ConnectivityState.IDLE
                || state == ConnectivityState.READY;
    }

    public void shutdown(boolean debugLog) {
        channel.shutdown();
        if (debugLog) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("shut down meta service client: {}", address);
            }
        } else {
            LOG.warn("shut down meta service client: {}", address);
        }
    }

    public Future<Cloud.GetVersionResponse> getVisibleVersionAsync(Cloud.GetVersionRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetVersionRequest.Builder builder = Cloud.GetVersionRequest.newBuilder();
            builder.mergeFrom(request);
            return stub.getVersion(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return stub.getVersion(request);
    }

    public Cloud.GetVersionResponse getVersion(Cloud.GetVersionRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetVersionRequest.Builder builder = Cloud.GetVersionRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getVersion(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getVersion(request);
    }

    public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.CreateTabletsRequest.Builder builder = Cloud.CreateTabletsRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.createTablets(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.createTablets(request);
    }

    public Cloud.UpdateTabletResponse updateTablet(Cloud.UpdateTabletRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.UpdateTabletRequest.Builder builder = Cloud.UpdateTabletRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.updateTablet(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.updateTablet(request);
    }

    public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.BeginTxnRequest.Builder builder = Cloud.BeginTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.beginTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.beginTxn(request);
    }

    public Cloud.PrecommitTxnResponse precommitTxn(Cloud.PrecommitTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.PrecommitTxnRequest.Builder builder = Cloud.PrecommitTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.precommitTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.precommitTxn(request);
    }

    public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.CommitTxnRequest.Builder builder = Cloud.CommitTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.commitTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.commitTxn(request);
    }

    public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AbortTxnRequest.Builder builder = Cloud.AbortTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.abortTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.abortTxn(request);
    }

    public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request) {
        return blockingStub.getTxn(request);
    }

    public Cloud.GetTxnIdResponse getTxnId(Cloud.GetTxnIdRequest request) {
        return blockingStub.getTxnId(request);
    }

    public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request) {
        return blockingStub.getCurrentMaxTxnId(request);
    }

    public Cloud.BeginSubTxnResponse beginSubTxn(Cloud.BeginSubTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.BeginSubTxnRequest.Builder builder = Cloud.BeginSubTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.beginSubTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.beginSubTxn(request);
    }

    public Cloud.AbortSubTxnResponse abortSubTxn(Cloud.AbortSubTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AbortSubTxnRequest.Builder builder = Cloud.AbortSubTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.abortSubTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.abortSubTxn(request);
    }

    public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request) {
        return blockingStub.checkTxnConflict(request);
    }

    public Cloud.CleanTxnLabelResponse cleanTxnLabel(Cloud.CleanTxnLabelRequest request) {
        return blockingStub.cleanTxnLabel(request);
    }

    public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetClusterRequest.Builder builder = Cloud.GetClusterRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getCluster(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getCluster(request);
    }

    public Cloud.IndexResponse prepareIndex(Cloud.IndexRequest request) {
        return blockingStub.prepareIndex(request);
    }

    public Cloud.IndexResponse commitIndex(Cloud.IndexRequest request) {
        return blockingStub.commitIndex(request);
    }

    public Cloud.IndexResponse dropIndex(Cloud.IndexRequest request) {
        return blockingStub.dropIndex(request);
    }

    public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request) {
        return blockingStub.preparePartition(request);
    }

    public Cloud.PartitionResponse commitPartition(Cloud.PartitionRequest request) {
        return blockingStub.commitPartition(request);
    }

    public Cloud.CheckKVResponse checkKv(Cloud.CheckKVRequest request) {
        return blockingStub.checkKv(request);
    }

    public Cloud.PartitionResponse dropPartition(Cloud.PartitionRequest request) {
        return blockingStub.dropPartition(request);
    }

    public Cloud.GetTabletStatsResponse getTabletStats(Cloud.GetTabletStatsRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetTabletStatsRequest.Builder builder = Cloud.GetTabletStatsRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getTabletStats(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getTabletStats(request);
    }

    public Cloud.CreateStageResponse createStage(Cloud.CreateStageRequest request) {
        return blockingStub.createStage(request);
    }

    public Cloud.GetStageResponse getStage(Cloud.GetStageRequest request) {
        return blockingStub.getStage(request);
    }

    public Cloud.DropStageResponse dropStage(Cloud.DropStageRequest request) {
        return blockingStub.dropStage(request);
    }

    public Cloud.GetIamResponse getIam(Cloud.GetIamRequest request) {
        return blockingStub.getIam(request);
    }

    public Cloud.BeginCopyResponse beginCopy(Cloud.BeginCopyRequest request) {
        return blockingStub.beginCopy(request);
    }

    public Cloud.FinishCopyResponse finishCopy(Cloud.FinishCopyRequest request) {
        return blockingStub.finishCopy(request);
    }

    public Cloud.GetCopyJobResponse getCopyJob(Cloud.GetCopyJobRequest request) {
        return blockingStub.getCopyJob(request);
    }

    public Cloud.GetCopyFilesResponse getCopyFiles(Cloud.GetCopyFilesRequest request) {
        return blockingStub.getCopyFiles(request);
    }

    public Cloud.FilterCopyFilesResponse filterCopyFiles(Cloud.FilterCopyFilesRequest request) {
        return blockingStub.filterCopyFiles(request);
    }

    public Cloud.AlterClusterResponse alterCluster(Cloud.AlterClusterRequest request) {
        return blockingStub.alterCluster(request);
    }

    public Cloud.AlterObjStoreInfoResponse alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.alterObjStoreInfo(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.alterObjStoreInfo(request);
    }

    public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.alterStorageVault(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.alterStorageVault(request);
    }

    public Cloud.GetDeleteBitmapUpdateLockResponse getDeleteBitmapUpdateLock(
            Cloud.GetDeleteBitmapUpdateLockRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetDeleteBitmapUpdateLockRequest.Builder builder = Cloud.GetDeleteBitmapUpdateLockRequest
                    .newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getDeleteBitmapUpdateLock(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getDeleteBitmapUpdateLock(request);
    }

    public Cloud.GetInstanceResponse getInstance(Cloud.GetInstanceRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetInstanceRequest.Builder builder = Cloud.GetInstanceRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getInstance(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getInstance(request);
    }

    public Cloud.GetRLTaskCommitAttachResponse
            getRLTaskCommitAttach(Cloud.GetRLTaskCommitAttachRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetRLTaskCommitAttachRequest.Builder builder =
                    Cloud.GetRLTaskCommitAttachRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getRlTaskCommitAttach(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getRlTaskCommitAttach(request);
    }

    public Cloud. ResetRLProgressResponse resetRLProgress(Cloud. ResetRLProgressRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud. ResetRLProgressRequest.Builder builder =
                    Cloud. ResetRLProgressRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.resetRlProgress(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.resetRlProgress(request);
    }

    public Cloud.GetObjStoreInfoResponse
            getObjStoreInfo(Cloud.GetObjStoreInfoRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetObjStoreInfoRequest.Builder builder =
                    Cloud.GetObjStoreInfoRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.getObjStoreInfo(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.getObjStoreInfo(request);
    }

    public Cloud.AbortTxnWithCoordinatorResponse
            abortTxnWithCoordinator(Cloud.AbortTxnWithCoordinatorRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AbortTxnWithCoordinatorRequest.Builder builder =
                    Cloud.AbortTxnWithCoordinatorRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.abortTxnWithCoordinator(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.abortTxnWithCoordinator(request);
    }

    public Cloud.FinishTabletJobResponse
            finishTabletJob(Cloud.FinishTabletJobRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.FinishTabletJobRequest.Builder builder =
                    Cloud.FinishTabletJobRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.finishTabletJob(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.finishTabletJob(request);
    }

    public Cloud.CreateInstanceResponse
            createInstance(Cloud.CreateInstanceRequest request) {
        return blockingStub.createInstance(request);
    }
}
