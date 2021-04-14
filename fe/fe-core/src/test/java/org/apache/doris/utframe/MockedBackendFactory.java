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

import org.apache.doris.common.ClientPool;
import org.apache.doris.proto.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.proto.Status;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TAgentPublishRequest;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.TCancelPlanFragmentParams;
import org.apache.doris.thrift.TCancelPlanFragmentResult;
import org.apache.doris.thrift.TDeleteEtlFilesRequest;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExecPlanFragmentResult;
import org.apache.doris.thrift.TExportState;
import org.apache.doris.thrift.TExportStatusResult;
import org.apache.doris.thrift.TExportTaskRequest;
import org.apache.doris.thrift.TFetchDataParams;
import org.apache.doris.thrift.TFetchDataResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.THeartbeatResult;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TMiniLoadEtlStatusRequest;
import org.apache.doris.thrift.TMiniLoadEtlStatusResult;
import org.apache.doris.thrift.TMiniLoadEtlTaskRequest;
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
import org.apache.doris.thrift.TTabletStatResult;
import org.apache.doris.thrift.TTransmitDataParams;
import org.apache.doris.thrift.TTransmitDataResult;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import io.grpc.stub.StreamObserver;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

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

    // create a mocked backend with customize parameters
    public static MockedBackend createBackend(String host, int heartbeatPort, int thriftPort, int brpcPort, int httpPort,
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

        public DefaultHeartbeatServiceImpl(int beThriftPort, int beHttpPort, int beBrpcPort) {
            this.beThriftPort = beThriftPort;
            this.beHttpPort = beHttpPort;
            this.beBrpcPort = beBrpcPort;
        }

        @Override
        public THeartbeatResult heartbeat(TMasterInfo master_info) throws TException {
            TBackendInfo backendInfo = new TBackendInfo(beThriftPort, beHttpPort);
            backendInfo.setBrpcPort(beBrpcPort);
            THeartbeatResult result = new THeartbeatResult(new TStatus(TStatusCode.OK), backendInfo);
            return result;
        }
    }
    
    // abstract BeThriftService.
    // User can extends this abstract class to create other custom be thrift service
    public static abstract class BeThriftService implements BackendService.Iface {
        protected MockedBackend backend;

        public void setBackend(MockedBackend backend) {
            this.backend = backend;
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
                        try {
                            TAgentTaskRequest request = taskQueue.take();
                            System.out.println("get agent task request. type: " + request.getTaskType()
                                    + ", signature: " + request.getSignature() + ", fe addr: " + backend.getFeAddress());
                            TFinishTaskRequest finishTaskRequest = new TFinishTaskRequest(tBackend,
                                    request.getTaskType(), request.getSignature(), new TStatus(TStatusCode.OK));
                            finishTaskRequest.setReportVersion(++reportVersion);

                            FrontendService.Client client = ClientPool.frontendPool.borrowObject(backend.getFeAddress(), 2000);
                            System.out.println("get fe " + backend.getFeAddress() + " client: " + client);
                            client.finishTask(finishTaskRequest);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
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
        public TFetchDataResult fetchData(TFetchDataParams params) throws TException {
            return null;
        }

        @Override
        public TAgentResult submitTasks(List<TAgentTaskRequest> tasks) throws TException {
            for (TAgentTaskRequest request : tasks) {
                taskQueue.add(request);
                System.out.println("receive agent task request. type: " + request.getTaskType() + ", signature: "
                        + request.getSignature());
            }
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult makeSnapshot(TSnapshotRequest snapshot_request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult releaseSnapshot(String snapshot_path) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult publishClusterState(TAgentPublishRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult submitEtlTask(TMiniLoadEtlTaskRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TMiniLoadEtlStatusResult getEtlStatus(TMiniLoadEtlStatusRequest request) throws TException {
            return new TMiniLoadEtlStatusResult(new TStatus(TStatusCode.OK), TEtlState.FINISHED);
        }

        @Override
        public TAgentResult deleteEtlFiles(TDeleteEtlFilesRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TStatus submitExportTask(TExportTaskRequest request) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TExportStatusResult getExportStatus(TUniqueId task_id) throws TException {
            return new TExportStatusResult(new TStatus(TStatusCode.OK), TExportState.FINISHED);
        }

        @Override
        public TStatus eraseExportTask(TUniqueId task_id) throws TException {
            return new TStatus(TStatusCode.OK);
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
    }

    // The default Brpc service.
    public static class DefaultPBackendServiceImpl extends PBackendServiceGrpc.PBackendServiceImplBase {
       @Override
        public void transmitData(InternalService.PTransmitDataParams request, StreamObserver<InternalService.PTransmitDataResult> responseObserver) {
           responseObserver.onNext(InternalService.PTransmitDataResult.newBuilder()
                   .setStatus(Status.PStatus.newBuilder().setStatusCode(0)).build());
           responseObserver.onCompleted();
        }

        @Override
        public void execPlanFragment(InternalService.PExecPlanFragmentRequest request, StreamObserver<InternalService.PExecPlanFragmentResult> responseObserver) {
            System.out.println("get exec_plan_fragment request");
            responseObserver.onNext(InternalService.PExecPlanFragmentResult.newBuilder()
                    .setStatus(Status.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void cancelPlanFragment(InternalService.PCancelPlanFragmentRequest request, StreamObserver<InternalService.PCancelPlanFragmentResult> responseObserver) {
            System.out.println("get cancel_plan_fragment request");
            responseObserver.onNext(InternalService.PCancelPlanFragmentResult.newBuilder()
                    .setStatus(Status.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void fetchData(InternalService.PFetchDataRequest request, StreamObserver<InternalService.PFetchDataResult> responseObserver) {
            System.out.println("get fetch_data request");
            responseObserver.onNext(InternalService.PFetchDataResult.newBuilder()
                    .setStatus(Status.PStatus.newBuilder().setStatusCode(0))
                    .setQueryStatistics(Data.PQueryStatistics.newBuilder()
                            .setScanRows(0L)
                            .setScanBytes(0L))
                    .setEos(true)
                    .setPacketSeq(0L)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void tabletWriterOpen(InternalService.PTabletWriterOpenRequest request, StreamObserver<InternalService.PTabletWriterOpenResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void tabletWriterAddBatch(InternalService.PTabletWriterAddBatchRequest request, StreamObserver<InternalService.PTabletWriterAddBatchResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void tabletWriterCancel(InternalService.PTabletWriterCancelRequest request, StreamObserver<InternalService.PTabletWriterCancelResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void triggerProfileReport(InternalService.PTriggerProfileReportRequest request, StreamObserver<InternalService.PTriggerProfileReportResult> responseObserver) {
            System.out.println("get triggerProfileReport request");
            responseObserver.onNext(InternalService.PTriggerProfileReportResult.newBuilder()
                    .setStatus(Status.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void getInfo(InternalService.PProxyRequest request, StreamObserver<InternalService.PProxyResult> responseObserver) {
            System.out.println("get get_info request");
            responseObserver.onNext(InternalService.PProxyResult.newBuilder()
                    .setStatus(Status.PStatus.newBuilder().setStatusCode(0)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateCache(InternalService.PUpdateCacheRequest request, StreamObserver<InternalService.PCacheResponse> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void fetchCache(InternalService.PFetchCacheRequest request, StreamObserver<InternalService.PFetchCacheResult> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        @Override
        public void clearCache(InternalService.PClearCacheRequest request, StreamObserver<InternalService.PCacheResponse> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }
    }
}
