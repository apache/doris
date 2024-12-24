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

package org.apache.doris.utframe;

import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.proto.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.proto.Types;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.FrontendService.Client;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TAgentPublishRequest;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.TCancelPlanFragmentParams;
import org.apache.doris.thrift.TCancelPlanFragmentResult;
import org.apache.doris.thrift.TCheckStorageFormatResult;
import org.apache.doris.thrift.TCloneReq;
import org.apache.doris.thrift.TCreateTabletReq;
import org.apache.doris.thrift.TDiskTrashInfo;
import org.apache.doris.thrift.TDropTabletReq;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExecPlanFragmentResult;
import org.apache.doris.thrift.TExportState;
import org.apache.doris.thrift.TExportStatusResult;
import org.apache.doris.thrift.TExportTaskRequest;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.THeartbeatResult;
import org.apache.doris.thrift.TIngestBinlogRequest;
import org.apache.doris.thrift.TIngestBinlogResult;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPublishTopicRequest;
import org.apache.doris.thrift.TPublishTopicResult;
import org.apache.doris.thrift.TQueryIngestBinlogRequest;
import org.apache.doris.thrift.TQueryIngestBinlogResult;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TScanBatchResult;
import org.apache.doris.thrift.TScanCloseParams;
import org.apache.doris.thrift.TScanCloseResult;
import org.apache.doris.thrift.TScanNextBatchParams;
import org.apache.doris.thrift.TScanOpenParams;
import org.apache.doris.thrift.TScanOpenResult;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMediumMigrateReq;
import org.apache.doris.thrift.TStreamLoadRecordResult;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTabletStatResult;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TTransmitDataParams;
import org.apache.doris.thrift.TTransmitDataResult;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.grpc.stub.StreamObserver;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/*
 * This class is used to create mock backends.
 * Usage can be found in Demon.java's beforeClass()
 *
 *
 */
public class MockedBackendFactory {

    public static final String BE_DEFAULT_IP = "127.0.0.1";
    public static final int BE_DEFAULT_HEARTBEAT_PORT = 9050;
    public static final int BE_DEFAULT_THRIFT_PORT = 9060;
    public static final int BE_DEFAULT_BRPC_PORT = 8060;
    public static final int BE_DEFAULT_HTTP_PORT = 8040;
    public static final int BE_DEFAULT_ARROW_FLIGHT_SQL_PORT = 8070;

    // create a mocked backend with customize parameters
    public static MockedBackend createBackend(String host, int heartbeatPort, int thriftPort, int brpcPort,
                                              int httpPort, int arrowFlightSqlPort,
                                              HeartbeatService.Iface hbService, BeThriftService beThriftService,
                                              PBackendServiceGrpc.PBackendServiceImplBase pBackendService)
            throws IOException {
        MockedBackend backend = new MockedBackend(host, heartbeatPort, thriftPort, brpcPort, httpPort, hbService,
                beThriftService, pBackendService);
        return backend;
    }

    // the default hearbeat service.
    // User can implement HeartbeatService.Iface to create other custom heartbeat service.
    public static class DefaultHeartbeatServiceImpl implements HeartbeatService.Iface {
        private int beThriftPort;
        private int beHttpPort;
        private int beBrpcPort;

        private int beArrowFlightSqlPort;

        public DefaultHeartbeatServiceImpl(int beThriftPort, int beHttpPort, int beBrpcPort, int beArrowFlightSqlPort) {
            this.beThriftPort = beThriftPort;
            this.beHttpPort = beHttpPort;
            this.beBrpcPort = beBrpcPort;
            this.beArrowFlightSqlPort = beArrowFlightSqlPort;
        }

        @Override
        public THeartbeatResult heartbeat(TMasterInfo masterInfo) throws TException {
            TBackendInfo backendInfo = new TBackendInfo(beThriftPort, beHttpPort);
            backendInfo.setBrpcPort(beBrpcPort);
            backendInfo.setArrowFlightSqlPort(beArrowFlightSqlPort);
            THeartbeatResult result = new THeartbeatResult(new TStatus(TStatusCode.OK), backendInfo);
            return result;
        }
    }

    // abstract BeThriftService.
    // User can extends this abstract class to create other custom be thrift service
    public abstract static class BeThriftService implements BackendService.Iface {
        protected MockedBackend backend;
        protected Backend backendInFe;

        public void setBackend(MockedBackend backend) {
            this.backend = backend;
        }

        public void setBackendInFe(Backend backendInFe) {
            this.backendInFe = backendInFe;
        }

        public abstract void init();
    }

    // the default be thrift service extends from BeThriftService
    public static class DefaultBeThriftServiceImpl extends BeThriftService {
        // task queue to save all agent tasks coming from Frontend
        private BlockingQueue<TAgentTaskRequest> taskQueue = Queues.newLinkedBlockingQueue();
        private TBackend tBackend;
        private long reportVersion = 0;

        public DefaultBeThriftServiceImpl() {
        }

        @Override
        public void init() {
            tBackend = new TBackend(backend.getHost(), backend.getBeThriftPort(), backend.getHttpPort());
            // start a thread to handle all agent tasks in taskQueue.
            // Only return information that the task was successfully executed.
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        boolean ok = false;
                        Client client = null;
                        TNetworkAddress address = null;
                        try {
                            // ATTR: backend.getFeAddress must after taskQueue.take, because fe addr thread race
                            TAgentTaskRequest request = taskQueue.take();
                            address = backend.getFeAddress();
                            if (address == null) {
                                System.out.println("fe addr thread race, please check it");
                            }
                            System.out.println(backend.getHost() + ":" + backend.getHeartbeatPort() + " "
                                    + "get agent task request. type: " + request.getTaskType() + ", signature: "
                                    + request.getSignature() + ", fe addr: " + address);
                            TFinishTaskRequest finishTaskRequest = new TFinishTaskRequest(tBackend,
                                    request.getTaskType(), request.getSignature(), new TStatus(TStatusCode.OK));
                            TTaskType taskType = request.getTaskType();
                            switch (taskType) {
                                case CREATE:
                                    ++reportVersion;
                                    handleCreateTablet(request, finishTaskRequest);
                                    break;
                                case ALTER:
                                    ++reportVersion;
                                    break;
                                case DROP:
                                    handleDropTablet(request, finishTaskRequest);
                                    break;
                                case CLONE:
                                    ++reportVersion;
                                    handleCloneTablet(request, finishTaskRequest);
                                    break;
                                case STORAGE_MEDIUM_MIGRATE:
                                    handleStorageMediumMigrate(request, finishTaskRequest);
                                    break;
                                default:
                                    break;
                            }
                            finishTaskRequest.setReportVersion(reportVersion);

                            client = ClientPool.frontendPool.borrowObject(address, 2000);
                            client.finishTask(finishTaskRequest);
                            ok = true;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }  finally {
                            if (ok) {
                                ClientPool.frontendPool.returnObject(address, client);
                            } else {
                                ClientPool.frontendPool.invalidateObject(address, client);
                            }
                        }
                    }
                }

                private void handleCreateTablet(TAgentTaskRequest request, TFinishTaskRequest finishTaskRequest) {
                    TCreateTabletReq req = request.getCreateTabletReq();
                    List<DiskInfo> candDisks = backendInFe.getDisks().values().stream()
                            .filter(disk -> req.storage_medium == disk.getStorageMedium() && disk.isAlive())
                            .collect(Collectors.toList());
                    if (candDisks.isEmpty()) {
                        candDisks = backendInFe.getDisks().values().stream()
                                .filter(DiskInfo::isAlive)
                                .collect(Collectors.toList());
                    }
                    DiskInfo choseDisk = candDisks.isEmpty() ? null
                            : candDisks.get(new Random().nextInt(candDisks.size()));

                    List<TTabletInfo> tabletInfos = Lists.newArrayList();
                    TTabletInfo tabletInfo = new TTabletInfo();
                    tabletInfo.setTabletId(req.tablet_id);
                    tabletInfo.setVersion(req.version);
                    tabletInfo.setPathHash(choseDisk == null ? -1L : choseDisk.getPathHash());
                    tabletInfo.setReplicaId(req.replica_id);
                    tabletInfo.setUsed(true);
                    tabletInfos.add(tabletInfo);
                    finishTaskRequest.setFinishTabletInfos(tabletInfos);
                }

                private void handleDropTablet(TAgentTaskRequest request, TFinishTaskRequest finishTaskRequest) {
                    TDropTabletReq req = request.getDropTabletReq();
                    long dataSize = Math.max(1, CatalogTestUtil.getTabletDataSize(req.tablet_id));
                    DiskInfo diskInfo = getDisk(-1);
                    if (diskInfo != null) {
                        diskInfo.setDataUsedCapacityB(Math.max(0L,
                                    diskInfo.getDataUsedCapacityB() - dataSize));
                        diskInfo.setAvailableCapacityB(Math.min(diskInfo.getTotalCapacityB(),
                                    diskInfo.getAvailableCapacityB() + dataSize));
                    }
                }

                private void handleCloneTablet(TAgentTaskRequest request, TFinishTaskRequest finishTaskRequest) {
                    while (DebugPointUtil.isEnable("MockedBackendFactory.handleCloneTablet.block")) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    TCloneReq req = request.getCloneReq();
                    long dataSize = Math.max(1, CatalogTestUtil.getTabletDataSize(req.tablet_id));
                    long pathHash = req.dest_path_hash;
                    DiskInfo diskInfo = getDisk(pathHash);
                    if (diskInfo != null) {
                        pathHash = diskInfo.getPathHash();
                        diskInfo.setDataUsedCapacityB(Math.min(diskInfo.getTotalCapacityB(),
                                    diskInfo.getDataUsedCapacityB() + dataSize));
                        diskInfo.setAvailableCapacityB(Math.max(0L,
                                    diskInfo.getAvailableCapacityB() - dataSize));
                    }

                    List<TTabletInfo> tabletInfos = Lists.newArrayList();
                    TTabletInfo tabletInfo = new TTabletInfo(req.tablet_id, req.schema_hash, req.version,
                            0L, 1, dataSize);
                    tabletInfo.setStorageMedium(req.storage_medium);
                    tabletInfo.setPathHash(pathHash);
                    tabletInfo.setUsed(true);
                    tabletInfos.add(tabletInfo);
                    if (DebugPointUtil.isEnable("MockedBackendFactory.handleCloneTablet.failed")) {
                        finishTaskRequest.setTaskStatus(new TStatus(TStatusCode.CANCELLED));
                        finishTaskRequest.getTaskStatus().setErrorMsgs(Collections.singletonList("debug point set"));
                    }
                    finishTaskRequest.setFinishTabletInfos(tabletInfos);
                }

                private void handleStorageMediumMigrate(TAgentTaskRequest request, TFinishTaskRequest finishTaskRequest) {
                    TStorageMediumMigrateReq req = request.getStorageMediumMigrateReq();
                    long dataSize = Math.max(1, CatalogTestUtil.getTabletDataSize(req.tablet_id));

                    long srcDataPath = CatalogTestUtil.getReplicaPathHash(req.tablet_id, backendInFe.getId());
                    DiskInfo srcDiskInfo = getDisk(srcDataPath);
                    if (srcDiskInfo != null) {
                        srcDiskInfo.setDataUsedCapacityB(Math.min(srcDiskInfo.getTotalCapacityB(),
                                srcDiskInfo.getDataUsedCapacityB() - dataSize));
                        srcDiskInfo.setAvailableCapacityB(Math.max(0L,
                                srcDiskInfo.getAvailableCapacityB() + dataSize));
                        srcDiskInfo.setState(DiskState.ONLINE);
                    }

                    DiskInfo destDiskInfo = getDisk(req.data_dir);
                    if (destDiskInfo != null) {
                        destDiskInfo.setDataUsedCapacityB(Math.min(destDiskInfo.getTotalCapacityB(),
                                destDiskInfo.getDataUsedCapacityB() + dataSize));
                        destDiskInfo.setAvailableCapacityB(Math.max(0L,
                                destDiskInfo.getAvailableCapacityB() - dataSize));
                        destDiskInfo.setState(DiskState.ONLINE);
                    }
                }

                private DiskInfo getDisk(String dataDir) {
                    return backendInFe.getDisks().get(dataDir);
                }

                private DiskInfo getDisk(long pathHash) {
                    DiskInfo diskInfo = null;
                    for (DiskInfo tmpDiskInfo : backendInFe.getDisks().values()) {
                        diskInfo = tmpDiskInfo;
                        if (diskInfo.getPathHash() == pathHash
                                || pathHash == -1L || pathHash == 0) {
                            break;
                        }
                    }

                    return diskInfo;
                }
            }).start();
        }

        @Override
        public TExecPlanFragmentResult execPlanFragment(TExecPlanFragmentParams params) throws TException {
            return null;
        }

        @Override
        public TCancelPlanFragmentResult cancelPlanFragment(TCancelPlanFragmentParams params) throws TException {
            return null;
        }

        @Override
        public TTransmitDataResult transmitData(TTransmitDataParams params) throws TException {
            return null;
        }

        @Override
        public TAgentResult submitTasks(List<TAgentTaskRequest> tasks) throws TException {
            for (TAgentTaskRequest request : tasks) {
                taskQueue.add(request);
                System.out.println(backend.getHost() + ":" + backend.getHeartbeatPort() + " "
                        + "receive agent task request. type: " + request.getTaskType() + ", signature: "
                        + request.getSignature());
            }
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult makeSnapshot(TSnapshotRequest snapshotRequest) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult releaseSnapshot(String snapshotPath) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult publishClusterState(TAgentPublishRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TPublishTopicResult publishTopicInfo(TPublishTopicRequest request) throws TException {
            return new TPublishTopicResult(new TStatus(TStatusCode.OK));
        }


        @Override
        public TStatus submitExportTask(TExportTaskRequest request) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TExportStatusResult getExportStatus(TUniqueId taskId) throws TException {
            return new TExportStatusResult(new TStatus(TStatusCode.OK), TExportState.FINISHED);
        }

        @Override
        public TStatus eraseExportTask(TUniqueId taskId) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public long getTrashUsedCapacity() throws TException {
            return 0L;
        }

        @Override
        public List<TDiskTrashInfo> getDiskTrashUsedCapacity() throws TException {
            return null;
        }

        @Override
        public TTabletStatResult getTabletStat() throws TException {
            return new TTabletStatResult(Maps.newHashMap());
        }

        @Override
        public TStatus submitRoutineLoadTask(List<TRoutineLoadTask> tasks) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TScanOpenResult openScanner(TScanOpenParams params) throws TException {
            return null;
        }

        @Override
        public TScanBatchResult getNext(TScanNextBatchParams params) throws TException {
            return null;
        }

        @Override
        public TScanCloseResult closeScanner(TScanCloseParams params) throws TException {
            return null;
        }

        @Override
        public TStreamLoadRecordResult getStreamLoadRecord(long lastStreamRecordTime) throws TException {
            return new TStreamLoadRecordResult(Maps.newHashMap());
        }

        @Override
        public TCheckStorageFormatResult checkStorageFormat() throws TException {
            return new TCheckStorageFormatResult();
        }

        @Override
        public TIngestBinlogResult ingestBinlog(TIngestBinlogRequest ingestBinlogRequest) throws TException {
            return null;
        }

        @Override
        public TQueryIngestBinlogResult queryIngestBinlog(TQueryIngestBinlogRequest queryIngestBinlogRequest)
                throws TException {
            return null;
        }
    }

    // The default Brpc service.
    public static class DefaultPBackendServiceImpl extends PBackendServiceGrpc.PBackendServiceImplBase {
        @Override
        public void transmitData(InternalService.PTransmitDataParams request,
                                 StreamObserver<InternalService.PTransmitDataResult> responseObserver) {
            responseObserver.onNext(InternalService.PTransmitDataResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void execPlanFragment(InternalService.PExecPlanFragmentRequest request,
                                     StreamObserver<InternalService.PExecPlanFragmentResult> responseObserver) {
            System.out.println("get exec_plan_fragment request");
            responseObserver.onNext(InternalService.PExecPlanFragmentResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void execPlanFragmentPrepare(InternalService.PExecPlanFragmentRequest request,
                                            StreamObserver<InternalService.PExecPlanFragmentResult> responseObserver) {
            System.out.println("get exec_plan_fragment_prepare request");
            responseObserver.onNext(InternalService.PExecPlanFragmentResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void execPlanFragmentStart(InternalService.PExecPlanFragmentStartRequest request,
                                          StreamObserver<InternalService.PExecPlanFragmentResult> responseObserver) {
            System.out.println("get exec_plan_fragment_start request");
            responseObserver.onNext(InternalService.PExecPlanFragmentResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void cancelPlanFragment(InternalService.PCancelPlanFragmentRequest request,
                                       StreamObserver<InternalService.PCancelPlanFragmentResult> responseObserver) {
            System.out.println("get cancel_plan_fragment request");
            responseObserver.onNext(InternalService.PCancelPlanFragmentResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void fetchData(InternalService.PFetchDataRequest request,
                              StreamObserver<InternalService.PFetchDataResult> responseObserver) {
            System.out.println("get fetch_data request");
            responseObserver.onNext(InternalService.PFetchDataResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0))
                    .setQueryStatistics(Data.PQueryStatistics.newBuilder()
                            .setScanRows(0L)
                            .setScanBytes(0L))
                    .setEos(true)
                    .setPacketSeq(0L)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void tabletWriterOpen(InternalService.PTabletWriterOpenRequest request,
                                     StreamObserver<InternalService.PTabletWriterOpenResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void tabletWriterCancel(InternalService.PTabletWriterCancelRequest request,
                                       StreamObserver<InternalService.PTabletWriterCancelResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void getInfo(InternalService.PProxyRequest request,
                            StreamObserver<InternalService.PProxyResult> responseObserver) {
            System.out.println("get get_info request");
            responseObserver.onNext(InternalService.PProxyResult.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateCache(InternalService.PUpdateCacheRequest request,
                                StreamObserver<InternalService.PCacheResponse> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void fetchCache(InternalService.PFetchCacheRequest request,
                               StreamObserver<InternalService.PFetchCacheResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void clearCache(InternalService.PClearCacheRequest request,
                               StreamObserver<InternalService.PCacheResponse> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }
    }
}
