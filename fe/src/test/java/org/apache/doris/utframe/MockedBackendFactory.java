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

import org.apache.doris.common.ThriftServer;
import org.apache.doris.proto.PCancelPlanFragmentRequest;
import org.apache.doris.proto.PCancelPlanFragmentResult;
import org.apache.doris.proto.PExecPlanFragmentResult;
import org.apache.doris.proto.PFetchDataResult;
import org.apache.doris.proto.PProxyRequest;
import org.apache.doris.proto.PProxyResult;
import org.apache.doris.proto.PQueryStatistics;
import org.apache.doris.proto.PStatus;
import org.apache.doris.proto.PTriggerProfileReportResult;
import org.apache.doris.rpc.PExecPlanFragmentRequest;
import org.apache.doris.rpc.PFetchDataRequest;
import org.apache.doris.rpc.PTriggerProfileReportRequest;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TAgentPublishRequest;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentTaskRequest;
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

import com.baidu.jprotobuf.pbrpc.ProtobufRPCService;
import com.baidu.jprotobuf.pbrpc.transport.RpcServer;
import com.google.common.collect.Maps;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.List;

/*
 * This class is used to create mock backends.
 * Usage:
 *      // create a default mocked backend
 *      MockedBackendFactory.createDefaultBackend();
 *      List<Pair<String, Integer>> bes = Lists.newArrayList();
 *      bes.add(Pair.create("127.0.0.1", MockedBackend.BE_DEFAULT_HEARTBEAT_PORT));
 *      Catalog.getCurrentSystemInfo().addBackends(bes, false, "default_cluster");
 *      Thread.sleep(5000); // sleep at least 5 seconds so that the first heartbeat can be done.
 *      
 *      // create a customized mocked backend
 *      MockedBackendFactory.createBackend(hbPort, thriftPort, brpcPort, hbServiceImpl, backendServiceImpl, pBackendServiceImpl);
 *      List<Pair<String, Integer>> bes = Lists.newArrayList();
 *      bes.add(Pair.create("127.0.0.1", beHeartbeatPort));
 *      Catalog.getCurrentSystemInfo().addBackends(bes, false, "default_cluster");
 *      Thread.sleep(5000); // sleep at least 5 seconds so that the first heartbeat can be done.
 *      
 * A mocked backend has 4 ports:
 *      heartbeat port
 *      be thrift port
 *      brpc port
 *      http port
 *      
 *      The first 3 ports must be specified to start corresponding mocking service.
 *      the http port should be specified in response of HeartbeatService.heartbeat().
 *      
 * A default mocked backend offers 3 default services.
 * 
 */
public class MockedBackendFactory {

    public static final int BE_DEFAULT_HEARTBEAT_PORT = 9050;
    public static final int BE_DEFAULT_THRIFT_PORT = 9060;
    public static final int BE_DEFAULT_BRPC_PORT = 8060;
    public static final int BE_DEFAULT_HTTP_PORT = 8040;

    public static void createDefaultBackend() throws IOException {
        createBackend(BE_DEFAULT_HEARTBEAT_PORT, BE_DEFAULT_THRIFT_PORT, BE_DEFAULT_BRPC_PORT,
                new DefaultHeartbeatServiceImpl(), new DefaultBeThriftServiceImpl(), new DefaultPBackendServiceImpl());
    }

    public static void createBackend(int heartbeatPort, int thriftPort, int brpcPort, 
            HeartbeatService.Iface hbServiceImpl,
            BackendService.Iface backendServiceImpl,
            Object pBackendServiceImpl) throws IOException {
        createHeartbeatService(heartbeatPort, hbServiceImpl);
        createBeThriftService(thriftPort, backendServiceImpl);
        createBrpcService(brpcPort, pBackendServiceImpl);
    }

    private static void createHeartbeatService(int heartbeatPort, HeartbeatService.Iface serviceImpl) throws IOException {
        TProcessor tprocessor = new HeartbeatService.Processor<HeartbeatService.Iface>(serviceImpl);
        ThriftServer heartbeatServer = new ThriftServer(heartbeatPort, tprocessor);
        heartbeatServer.start();
        System.out.println("Be heartbeat service is started with port: " + heartbeatPort);
    }

    private static void createBeThriftService(int beThriftPort, BackendService.Iface serviceImpl) throws IOException {
        TProcessor tprocessor = new BackendService.Processor<BackendService.Iface>(serviceImpl);
        ThriftServer beThriftServer = new ThriftServer(beThriftPort, tprocessor);
        beThriftServer.start();
        System.out.println("Be thrift service is started with port: " + beThriftPort);
    }

    private static void createBrpcService(int brpcPort, Object pBackendServiceImpl) {
        RpcServer rpcServer = new RpcServer();
        rpcServer.registerService(pBackendServiceImpl);
        rpcServer.start(brpcPort);
        System.out.println("Be brpc service is started with port: " + brpcPort);
    }

    public static class DefaultHeartbeatServiceImpl implements HeartbeatService.Iface {
        @Override
        public THeartbeatResult heartbeat(TMasterInfo master_info) throws TException {
            TBackendInfo backendInfo = new TBackendInfo(BE_DEFAULT_THRIFT_PORT, BE_DEFAULT_HTTP_PORT);
            backendInfo.setBrpc_port(BE_DEFAULT_BRPC_PORT);
            THeartbeatResult result = new THeartbeatResult(new TStatus(TStatusCode.OK), backendInfo);
            return result;
        }
    }
    
    public static class DefaultBeThriftServiceImpl implements BackendService.Iface {
        @Override
        public TExecPlanFragmentResult exec_plan_fragment(TExecPlanFragmentParams params) throws TException {
            return null;
        }

        @Override
        public TCancelPlanFragmentResult cancel_plan_fragment(TCancelPlanFragmentParams params) throws TException {
            return null;
        }

        @Override
        public TTransmitDataResult transmit_data(TTransmitDataParams params) throws TException {
            return null;
        }

        @Override
        public TFetchDataResult fetch_data(TFetchDataParams params) throws TException {
            return null;
        }

        @Override
        public TAgentResult submit_tasks(List<TAgentTaskRequest> tasks) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult make_snapshot(TSnapshotRequest snapshot_request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult release_snapshot(String snapshot_path) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult publish_cluster_state(TAgentPublishRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TAgentResult submit_etl_task(TMiniLoadEtlTaskRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TMiniLoadEtlStatusResult get_etl_status(TMiniLoadEtlStatusRequest request) throws TException {
            return new TMiniLoadEtlStatusResult(new TStatus(TStatusCode.OK), TEtlState.FINISHED);
        }

        @Override
        public TAgentResult delete_etl_files(TDeleteEtlFilesRequest request) throws TException {
            return new TAgentResult(new TStatus(TStatusCode.OK));
        }

        @Override
        public TStatus submit_export_task(TExportTaskRequest request) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TExportStatusResult get_export_status(TUniqueId task_id) throws TException {
            return new TExportStatusResult(new TStatus(TStatusCode.OK), TExportState.FINISHED);
        }

        @Override
        public TStatus erase_export_task(TUniqueId task_id) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TTabletStatResult get_tablet_stat() throws TException {
            return new TTabletStatResult(Maps.newHashMap());
        }

        @Override
        public TStatus submit_routine_load_task(List<TRoutineLoadTask> tasks) throws TException {
            return new TStatus(TStatusCode.OK);
        }

        @Override
        public TScanOpenResult open_scanner(TScanOpenParams params) throws TException {
            return null;
        }

        @Override
        public TScanBatchResult get_next(TScanNextBatchParams params) throws TException {
            return null;
        }

        @Override
        public TScanCloseResult close_scanner(TScanCloseParams params) throws TException {
            return null;
        }
    }

    public static class DefaultPBackendServiceImpl {

        @ProtobufRPCService(serviceName = "PBackendService", methodName = "exec_plan_fragment")
        public PExecPlanFragmentResult exec_plan_fragment(PExecPlanFragmentRequest request) {
            System.out.println("get exec_plan_fragment");
            PExecPlanFragmentResult result = new PExecPlanFragmentResult();
            PStatus pStatus = new PStatus();
            pStatus.status_code = 0;
            result.status = pStatus;
            return result;
        }

        @ProtobufRPCService(serviceName = "PBackendService", methodName = "cancel_plan_fragment")
        public PCancelPlanFragmentResult cancel_plan_fragment(PCancelPlanFragmentRequest request) {
            System.out.println("get cancel_plan_fragment");
            PCancelPlanFragmentResult result = new PCancelPlanFragmentResult();
            PStatus pStatus = new PStatus();
            pStatus.status_code = 0;
            result.status = pStatus;
            return result;
        }

        @ProtobufRPCService(serviceName = "PBackendService", methodName = "fetch_data")
        public PFetchDataResult fetchDataAsync(PFetchDataRequest request) {
            System.out.println("get fetch_data");
            PFetchDataResult result = new PFetchDataResult();
            PStatus pStatus = new PStatus();
            pStatus.status_code = 0;

            PQueryStatistics pQueryStatistics = new PQueryStatistics();
            pQueryStatistics.scan_rows = 0L;
            pQueryStatistics.scan_bytes = 0L;

            result.status = pStatus;
            result.packet_seq = 0L;
            result.query_statistics = pQueryStatistics;
            result.eos = true;
            return result;
        }

        @ProtobufRPCService(serviceName = "PBackendService", methodName = "trigger_profile_report")
        public PTriggerProfileReportResult triggerProfileReport(PTriggerProfileReportRequest request) {
            return null;
        }

        @ProtobufRPCService(serviceName = "PBackendService", methodName = "get_info")
        public PProxyResult getInfo(PProxyRequest request) {
            return null;
        }
    }
}
