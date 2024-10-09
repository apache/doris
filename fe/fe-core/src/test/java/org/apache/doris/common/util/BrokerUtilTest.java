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

package org.apache.doris.common.util;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerCloseWriterRequest;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOpenWriterRequest;
import org.apache.doris.thrift.TBrokerOpenWriterResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TBrokerPWriteRequest;
import org.apache.doris.thrift.TBrokerReadResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

public class BrokerUtilTest {

    @Test
    public void parseColumnsFromPath() {
        String path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assert.assertEquals(1, columns.size());
            Assert.assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k1/xxx.csv";
        try {
            BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k2"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assert.assertEquals(1, columns.size());
            Assert.assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2=v2/a/k1=v1/xxx.csv";
        try {
            BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2", "k3"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k2=v2//k1=v1//xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1//xxx.csv";
        try {
            List<String> columns = BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals(Lists.newArrayList("v1", "=v2="), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1/";
        try {
            BrokerUtil.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=2/a/xxx.csv";
        try {
            BrokerUtil.parseColumnsFromPath(path, Collections.singletonList("k1"));
        } catch (UserException ignored) {
            Assert.fail();
        }

    }

    @Test
    public void testReadFile(@Mocked TPaloBrokerService.Client client, @Mocked Env env,
                             @Injectable BrokerMgr brokerMgr)
            throws TException, UserException, UnsupportedEncodingException {
        // list response
        TBrokerListResponse listResponse = new TBrokerListResponse();
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        listResponse.opStatus = status;
        List<TBrokerFileStatus> files = Lists.newArrayList();
        String filePath = "hdfs://127.0.0.1:10000/doris/jobs/1/label6/9/dpp_result.json";
        files.add(new TBrokerFileStatus(filePath, false, 10, false));
        listResponse.files = files;

        // open reader response
        TBrokerOpenReaderResponse openReaderResponse = new TBrokerOpenReaderResponse();
        openReaderResponse.opStatus = status;
        openReaderResponse.fd = new TBrokerFD(1, 2);

        // read response
        String dppResultStr = "{'normal_rows': 10, 'abnormal_rows': 0, 'failed_reason': 'etl job failed'}";
        TBrokerReadResponse readResponse = new TBrokerReadResponse();
        readResponse.opStatus = status;
        readResponse.setData(dppResultStr.getBytes("UTF-8"));

        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TPaloBrokerService.Client>>() {
            @Mock
            public TPaloBrokerService.Client borrowObject(TNetworkAddress address) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                env.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
                client.listPath((TBrokerListPathRequest) any);
                result = listResponse;
                client.openReader((TBrokerOpenReaderRequest) any);
                result = openReaderResponse;
                client.pread((TBrokerPReadRequest) any);
                result = readResponse;
                times = 1;
                client.closeReader((TBrokerCloseReaderRequest) any);
                result = status;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        byte[] data = BrokerUtil.readFile(filePath, brokerDesc, 0);
        String readStr = new String(data, "UTF-8");
        Assert.assertEquals(dppResultStr, readStr);
    }

    @Test
    public void testWriteFile(@Mocked TPaloBrokerService.Client client, @Mocked Env env,
                              @Injectable BrokerMgr brokerMgr)
            throws TException, UserException, UnsupportedEncodingException {
        // open writer response
        TBrokerOpenWriterResponse openWriterResponse = new TBrokerOpenWriterResponse();
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        openWriterResponse.opStatus = status;
        openWriterResponse.fd = new TBrokerFD(1, 2);
        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TPaloBrokerService.Client>>() {
            @Mock
            public TPaloBrokerService.Client borrowObject(TNetworkAddress address) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                env.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
                client.openWriter((TBrokerOpenWriterRequest) any);
                result = openWriterResponse;
                client.pwrite((TBrokerPWriteRequest) any);
                result = status;
                times = 1;
                client.closeWriter((TBrokerCloseWriterRequest) any);
                result = status;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        byte[] configs = "{'label': 'label0'}".getBytes("UTF-8");
        String destFilePath = "hdfs://127.0.0.1:10000/doris/jobs/1/label6/9/configs/jobconfig.json";
        try {
            BrokerUtil.writeFile(configs, destFilePath, brokerDesc);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDeletePath(@Mocked TPaloBrokerService.Client client, @Mocked Env env,
                               @Injectable BrokerMgr brokerMgr) throws AnalysisException, TException {
        // delete response
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TPaloBrokerService.Client>>() {
            @Mock
            public TPaloBrokerService.Client borrowObject(TNetworkAddress address) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                env.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
                client.deletePath((TBrokerDeletePathRequest) any);
                result = status;
                times = 1;
            }
        };

        try {
            BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
            BrokerUtil.deletePathWithBroker("hdfs://127.0.0.1:10000/doris/jobs/1/label6/9", brokerDesc);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
