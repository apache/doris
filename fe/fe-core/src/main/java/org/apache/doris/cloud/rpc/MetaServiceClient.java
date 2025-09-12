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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.NameResolverRegistry;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MetaServiceClient {
    public static final Logger LOG = LogManager.getLogger(MetaServiceClient.class);
    private static final Map<String, ?> serviceConfig;

    private final String address;
    private final MetaServiceGrpc.MetaServiceFutureStub stub;
    private final MetaServiceGrpc.MetaServiceBlockingStub blockingStub;
    private final ManagedChannel channel;
    private final long expiredAt;
    private Random random = new Random();

    static {
        NameResolverRegistry.getDefaultRegistry().register(new MetaServiceListResolverProvider());

        // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy-capabilities
        serviceConfig = new Gson().fromJson(new JsonReader(new InputStreamReader(
                MetaServiceClient.class.getResourceAsStream("/retrying_service_config.json"),
                StandardCharsets.UTF_8)), Map.class);
        LOG.info("serviceConfig:{}", serviceConfig);
    }

    public MetaServiceClient(String address) {
        this.address = address;

        String target = address;
        if (address.contains(",")) {
            target = MetaServiceListResolverProvider.MS_LIST_SCHEME_PREFIX + address;
        }

        Preconditions.checkNotNull(serviceConfig, "serviceConfig is null");
        channel = NettyChannelBuilder.forTarget(target)
                .flowControlWindow(Config.grpc_max_message_size_bytes)
                .maxInboundMessageSize(Config.grpc_max_message_size_bytes)
                .defaultServiceConfig(serviceConfig)
                .defaultLoadBalancingPolicy("round_robin")
                .enableRetry()
                .usePlaintext()
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, Config.meta_service_brpc_connect_timeout_ms).build();
        stub = MetaServiceGrpc.newFutureStub(channel);
        blockingStub = MetaServiceGrpc.newBlockingStub(channel);
        expiredAt = connectionAgeExpiredAt();
    }

    private long connectionAgeExpiredAt() {
        long connectionAgeBase = Config.meta_service_connection_age_base_minutes;
        if (connectionAgeBase > 0) {
            long base = TimeUnit.MINUTES.toMillis(connectionAgeBase);
            long now = System.currentTimeMillis();
            long rand = random.nextLong() % base;
            return now + base + rand;
        }
        return Long.MAX_VALUE;
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
            return stub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .getVersion(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return stub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS).getVersion(request);
    }

    public Cloud.GetVersionResponse getVersion(Cloud.GetVersionRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetVersionRequest.Builder builder = Cloud.GetVersionRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .getVersion(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getVersion(request);
    }

    public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.CreateTabletsRequest.Builder builder = Cloud.CreateTabletsRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .createTablets(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .createTablets(request);
    }

    public Cloud.UpdateTabletResponse updateTablet(Cloud.UpdateTabletRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.UpdateTabletRequest.Builder builder = Cloud.UpdateTabletRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .updateTablet(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .updateTablet(request);
    }

    public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.BeginTxnRequest.Builder builder = Cloud.BeginTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .beginTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .beginTxn(request);
    }

    public Cloud.PrecommitTxnResponse precommitTxn(Cloud.PrecommitTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.PrecommitTxnRequest.Builder builder = Cloud.PrecommitTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .precommitTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .precommitTxn(request);
    }

    public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.CommitTxnRequest.Builder builder = Cloud.CommitTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .commitTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .commitTxn(request);
    }

    public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AbortTxnRequest.Builder builder = Cloud.AbortTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .abortTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .abortTxn(request);
    }

    public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getTxn(request);
    }

    public Cloud.GetTxnIdResponse getTxnId(Cloud.GetTxnIdRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getTxnId(request);
    }

    public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getCurrentMaxTxnId(request);
    }

    public Cloud.BeginSubTxnResponse beginSubTxn(Cloud.BeginSubTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.BeginSubTxnRequest.Builder builder = Cloud.BeginSubTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .beginSubTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .beginSubTxn(request);
    }

    public Cloud.AbortSubTxnResponse abortSubTxn(Cloud.AbortSubTxnRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AbortSubTxnRequest.Builder builder = Cloud.AbortSubTxnRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .abortSubTxn(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .abortSubTxn(request);
    }

    public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .checkTxnConflict(request);
    }

    public Cloud.CleanTxnLabelResponse cleanTxnLabel(Cloud.CleanTxnLabelRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .cleanTxnLabel(request);
    }

    public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetClusterRequest.Builder builder = Cloud.GetClusterRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .getCluster(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getCluster(request);
    }

    public Cloud.IndexResponse prepareIndex(Cloud.IndexRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .prepareIndex(request);
    }

    public Cloud.IndexResponse commitIndex(Cloud.IndexRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .commitIndex(request);
    }

    public Cloud.IndexResponse dropIndex(Cloud.IndexRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .dropIndex(request);
    }

    public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .preparePartition(request);
    }

    public Cloud.PartitionResponse commitPartition(Cloud.PartitionRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .commitPartition(request);
    }

    public Cloud.CheckKVResponse checkKv(Cloud.CheckKVRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .checkKv(request);
    }

    public Cloud.PartitionResponse dropPartition(Cloud.PartitionRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .dropPartition(request);
    }

    public Cloud.GetTabletStatsResponse getTabletStats(Cloud.GetTabletStatsRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetTabletStatsRequest.Builder builder = Cloud.GetTabletStatsRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getTabletStats(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getTabletStats(request);
    }

    public Cloud.CreateStageResponse createStage(Cloud.CreateStageRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .createStage(request);
    }

    public Cloud.GetStageResponse getStage(Cloud.GetStageRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getStage(request);
    }

    public Cloud.DropStageResponse dropStage(Cloud.DropStageRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .dropStage(request);
    }

    public Cloud.GetIamResponse getIam(Cloud.GetIamRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getIam(request);
    }

    public Cloud.BeginCopyResponse beginCopy(Cloud.BeginCopyRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .beginCopy(request);
    }

    public Cloud.FinishCopyResponse finishCopy(Cloud.FinishCopyRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .finishCopy(request);
    }

    public Cloud.GetCopyJobResponse getCopyJob(Cloud.GetCopyJobRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getCopyJob(request);
    }

    public Cloud.GetCopyFilesResponse getCopyFiles(Cloud.GetCopyFilesRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getCopyFiles(request);
    }

    public Cloud.FilterCopyFilesResponse filterCopyFiles(Cloud.FilterCopyFilesRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .filterCopyFiles(request);
    }

    public Cloud.AlterClusterResponse alterCluster(Cloud.AlterClusterRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .alterCluster(request);
    }

    /**
     * This method is deprecated, there is no code to call it.
     */
    @Deprecated
    public Cloud.AlterObjStoreInfoResponse alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .alterObjStoreInfo(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .alterObjStoreInfo(request);
    }

    public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AlterObjStoreInfoRequest.Builder builder = Cloud.AlterObjStoreInfoRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .alterStorageVault(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .alterStorageVault(request);
    }

    public Cloud.GetDeleteBitmapUpdateLockResponse getDeleteBitmapUpdateLock(
            Cloud.GetDeleteBitmapUpdateLockRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetDeleteBitmapUpdateLockRequest.Builder builder = Cloud.GetDeleteBitmapUpdateLockRequest
                    .newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getDeleteBitmapUpdateLock(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getDeleteBitmapUpdateLock(request);
    }

    public Cloud.RemoveDeleteBitmapUpdateLockResponse removeDeleteBitmapUpdateLock(
            Cloud.RemoveDeleteBitmapUpdateLockRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.RemoveDeleteBitmapUpdateLockRequest.Builder builder = Cloud.RemoveDeleteBitmapUpdateLockRequest
                    .newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .removeDeleteBitmapUpdateLock(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .removeDeleteBitmapUpdateLock(request);
    }

    public Cloud.GetInstanceResponse getInstance(Cloud.GetInstanceRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetInstanceRequest.Builder builder = Cloud.GetInstanceRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getInstance(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getInstance(request);
    }

    public Cloud.GetRLTaskCommitAttachResponse
            getRLTaskCommitAttach(Cloud.GetRLTaskCommitAttachRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetRLTaskCommitAttachRequest.Builder builder =
                    Cloud.GetRLTaskCommitAttachRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .getRlTaskCommitAttach(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getRlTaskCommitAttach(request);
    }

    public Cloud. ResetRLProgressResponse resetRLProgress(Cloud. ResetRLProgressRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud. ResetRLProgressRequest.Builder builder =
                    Cloud. ResetRLProgressRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .resetRlProgress(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .resetRlProgress(request);
    }

    public Cloud.GetObjStoreInfoResponse
            getObjStoreInfo(Cloud.GetObjStoreInfoRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.GetObjStoreInfoRequest.Builder builder =
                    Cloud.GetObjStoreInfoRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .getObjStoreInfo(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getObjStoreInfo(request);
    }

    public Cloud.AbortTxnWithCoordinatorResponse
            abortTxnWithCoordinator(Cloud.AbortTxnWithCoordinatorRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.AbortTxnWithCoordinatorRequest.Builder builder =
                    Cloud.AbortTxnWithCoordinatorRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .abortTxnWithCoordinator(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .abortTxnWithCoordinator(request);
    }

    public Cloud.FinishTabletJobResponse
            finishTabletJob(Cloud.FinishTabletJobRequest request) {
        if (!request.hasCloudUniqueId()) {
            Cloud.FinishTabletJobRequest.Builder builder =
                    Cloud.FinishTabletJobRequest.newBuilder();
            builder.mergeFrom(request);
            return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                    .finishTabletJob(builder.setCloudUniqueId(Config.cloud_unique_id).build());
        }
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .finishTabletJob(request);
    }

    public Cloud.CreateInstanceResponse
            createInstance(Cloud.CreateInstanceRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .createInstance(request);
    }

    public Cloud.GetStreamingTaskCommitAttachResponse
            getStreamingTaskCommitAttach(Cloud.GetStreamingTaskCommitAttachRequest request) {
        return blockingStub.withDeadlineAfter(Config.meta_service_brpc_timeout_ms, TimeUnit.MILLISECONDS)
                .getStreamingTaskCommitAttach(request);
    }
}
