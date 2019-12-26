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

package org.apache.doris.common;

import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TAgentPublishRequest;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TCancelPlanFragmentParams;
import org.apache.doris.thrift.TCancelPlanFragmentResult;
import org.apache.doris.thrift.TDeleteEtlFilesRequest;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExecPlanFragmentResult;
import org.apache.doris.thrift.TExportStatusResult;
import org.apache.doris.thrift.TExportTaskRequest;
import org.apache.doris.thrift.TFetchDataParams;
import org.apache.doris.thrift.TFetchDataResult;
import org.apache.doris.thrift.TMiniLoadEtlStatusRequest;
import org.apache.doris.thrift.TMiniLoadEtlStatusResult;
import org.apache.doris.thrift.TMiniLoadEtlTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TScanBatchResult;
import org.apache.doris.thrift.TScanCloseParams;
import org.apache.doris.thrift.TScanCloseResult;
import org.apache.doris.thrift.TScanNextBatchParams;
import org.apache.doris.thrift.TScanOpenParams;
import org.apache.doris.thrift.TScanOpenResult;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TTabletStatResult;
import org.apache.doris.thrift.TTransmitDataParams;
import org.apache.doris.thrift.TTransmitDataResult;
import org.apache.doris.thrift.TUniqueId;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenericPoolTest {
    static GenericPool<BackendService.Client> backendService;
    static ThriftServer service;
    static String ip = "127.0.0.1";
    static int port;

    static {
        Random r = new Random(System.currentTimeMillis());
        port = 20000 + r.nextInt(10000);
    }

    static void close() {
        if (service != null) {
            service.stop();
        }
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        try {
            GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
            config.setLifo(true); // set Last In First Out strategy
            config.setMaxIdlePerKey(2); // (default 8)
            config.setMinIdlePerKey(0); // (default 0)
            config.setMaxTotalPerKey(2); // (default 8)
            config.setMaxTotal(3); // (default -1)
            config.setMaxWaitMillis(500);
            // new ClientPool
            backendService = new GenericPool("BackendService", config, 0);
            // new ThriftService
            TProcessor tprocessor = new BackendService.Processor<BackendService.Iface>(
                    new InternalProcessor());
            service = new ThriftServer(port, tprocessor);
            service.start();
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    @AfterClass
    public static void afterClass() throws IOException {
        close();
    }

    private static class InternalProcessor implements BackendService.Iface {
        public InternalProcessor() {
            //
        }

        @Override
        public TExecPlanFragmentResult exec_plan_fragment(TExecPlanFragmentParams params) {
            return new TExecPlanFragmentResult();
        }

        @Override
        public TCancelPlanFragmentResult cancel_plan_fragment(TCancelPlanFragmentParams params) {
            return new TCancelPlanFragmentResult();
        }

        @Override
        public TTransmitDataResult transmit_data(TTransmitDataParams params) {
            return new TTransmitDataResult();
        }

        @Override
        public TFetchDataResult fetch_data(TFetchDataParams params) {
            TFetchDataResult result = new TFetchDataResult();
            result.setPacket_num(123);
            result.setResult_batch(new TResultBatch(new ArrayList<ByteBuffer>(), false, 0));
            result.setEos(true);
            return result;
        }

        @Override
        public TAgentResult submit_tasks(List<TAgentTaskRequest> tasks) throws TException {
            return null;
        }

        @Override
        public TAgentResult release_snapshot(String snapshot_path) throws TException {
            return null;
        }

        @Override
        public TAgentResult publish_cluster_state(TAgentPublishRequest request) throws TException {
            return null;
        }

        @Override
        public TAgentResult submit_etl_task(TMiniLoadEtlTaskRequest request) throws TException {
            return null;
        }

        @Override
        public TMiniLoadEtlStatusResult get_etl_status(TMiniLoadEtlStatusRequest request) throws TException {
            return null;
        }

        @Override
        public TAgentResult delete_etl_files(TDeleteEtlFilesRequest request) throws TException {
            return null;
        }

        @Override
        public TAgentResult make_snapshot(TSnapshotRequest snapshot_request) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TStatus submit_export_task(TExportTaskRequest request) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TExportStatusResult get_export_status(TUniqueId task_id) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TStatus erase_export_task(TUniqueId task_id) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TTabletStatResult get_tablet_stat() throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TStatus submit_routine_load_task(List<TRoutineLoadTask> tasks) throws TException {
            // TODO Auto-generated method stub
            return null;
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

    @Test
    public void testNormal() throws Exception {
        TNetworkAddress address = new TNetworkAddress(ip, port);
        BackendService.Client object = backendService.borrowObject(address);

        TFetchDataResult result = object.fetch_data(new TFetchDataParams(
                PaloInternalServiceVersion.V1, new TUniqueId()));
        Assert.assertEquals(result.getPacket_num(), 123);

        backendService.returnObject(address, object);
    }

    @Test
    public void testSetMaxPerKey() throws Exception {
        TNetworkAddress address = new TNetworkAddress(ip, port);
        BackendService.Client object1;
        BackendService.Client object2;
        BackendService.Client object3;

        // first success
        object1 = backendService.borrowObject(address);

        // second success
        object2 = backendService.borrowObject(address);

        // third fail, because the max connection is 2
        boolean flag = false;
        try {
            object3 = backendService.borrowObject(address);
        } catch (java.util.NoSuchElementException e) {
            flag = true;
            // pass
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertTrue(flag);

        // fouth success, beacuse we drop the object1
        backendService.returnObject(address, object1);
        object3 = null;
        object3 = backendService.borrowObject(address);
        Assert.assertTrue(object3 != null);

        backendService.returnObject(address, object2);
        backendService.returnObject(address, object3);
    }

    @Test
    public void testException() throws Exception {
        TNetworkAddress address = new TNetworkAddress(ip, port);
        BackendService.Client object;
        // borrow null
        boolean flag = false;
        try {
            object = backendService.borrowObject(null);
        } catch (NullPointerException e) {
            flag = true;
        }
        Assert.assertTrue(flag);
        flag = false;
        // return twice
        object = backendService.borrowObject(address);
        backendService.returnObject(address, object);
        try {
            backendService.returnObject(address, object);
        } catch (java.lang.IllegalStateException e) {
            flag = true;
        }
        Assert.assertTrue(flag);
    }
}
