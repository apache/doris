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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.Mocked;
import mockit.MockUp;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SparkEtlJobHandlerTest {
    private long loadJobId;
    private String label;
    private String resourceName;
    private String broker;
    private long pendingTaskId;
    private String appId;
    private String etlOutputPath;
    private String trackingUrl;

    @Before
    public void setUp() {
        loadJobId = 0L;
        label = "label0";
        resourceName = "spark0";
        broker = "broker0";
        pendingTaskId = 3L;
        appId = "application_15888888888_0088";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/doris/100/label/101";
        trackingUrl = "http://127.0.0.1:8080/proxy/application_1586619723848_0088/";
    }

    @Test
    public void testSubmitEtlJob(@Mocked BrokerUtil brokerUtil, @Mocked SparkLauncher launcher,
                                 @Injectable SparkAppHandle handle) throws IOException, LoadException {
        new Expectations() {
            {
                launcher.startApplication((SparkAppHandle.Listener) any);
                result = handle;
                handle.getAppId();
                returns(null, null, appId);
                handle.getState();
                returns(State.CONNECTED, State.SUBMITTED, State.RUNNING);
            }
        };

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        handler.submitEtlJob(loadJobId, label, etlJobConfig, resource, brokerDesc, attachment);

        // check submit etl job success
        Assert.assertEquals(appId, attachment.getAppId());
        Assert.assertEquals(handle, attachment.getHandle());
    }

    @Test(expected = LoadException.class)
    public void testSubmitEtlJobFailed(@Mocked BrokerUtil brokerUtil, @Mocked SparkLauncher launcher,
                                       @Injectable SparkAppHandle handle) throws IOException, LoadException {
        new Expectations() {
            {
                launcher.startApplication((SparkAppHandle.Listener) any);
                result = handle;
                handle.getAppId();
                result = null;
                handle.getState();
                returns(State.CONNECTED, State.SUBMITTED, State.FAILED);
            }
        };

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        handler.submitEtlJob(loadJobId, label, etlJobConfig, resource, brokerDesc, attachment);
    }

    @Test
    public void testGetEtlJobStatus(@Mocked BrokerUtil brokerUtil, @Mocked YarnClient client,
                                    @Injectable ApplicationReport report)
            throws IOException, YarnException, UserException {
        new Expectations() {
            {
                YarnClient.createYarnClient();
                result = client;
                client.getApplicationReport((ApplicationId) any);
                result = report;
                report.getYarnApplicationState();
                returns(YarnApplicationState.RUNNING, YarnApplicationState.FINISHED, YarnApplicationState.FINISHED);
                report.getFinalApplicationStatus();
                returns(FinalApplicationStatus.UNDEFINED, FinalApplicationStatus.FAILED, FinalApplicationStatus.SUCCEEDED);
                report.getTrackingUrl();
                result = trackingUrl;
                report.getProgress();
                returns(0.5f, 1f, 1f);
                BrokerUtil.readFile(anyString, (BrokerDesc) any);
                result = "{'normal_rows': 10, 'abnormal_rows': 0, 'failed_reason': 'etl job failed'}";
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkEtlJobHandler handler = new SparkEtlJobHandler();

        // running
        EtlStatus status = handler.getEtlJobStatus(null, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.RUNNING, status.getState());
        Assert.assertEquals(50, status.getProgress());

        // yarn finished and spark failed
        status = handler.getEtlJobStatus(null, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.CANCELLED, status.getState());
        Assert.assertEquals(100, status.getProgress());
        Assert.assertEquals("etl job failed", status.getDppResult().failedReason);

        // finished
        status = handler.getEtlJobStatus(null, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.FINISHED, status.getState());
        Assert.assertEquals(100, status.getProgress());
        Assert.assertEquals(trackingUrl, status.getTrackingUrl());
        Assert.assertEquals(10, status.getDppResult().normalRows);
        Assert.assertEquals(0, status.getDppResult().abnormalRows);
    }

    @Test
    public void testKillEtlJob(@Mocked YarnClient client) throws IOException, YarnException {
        new Expectations() {
            {
                YarnClient.createYarnClient();
                result = client;
                client.killApplication((ApplicationId) any);
                times = 1;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        try {
            handler.killEtlJob(null, appId, loadJobId, resource);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetEtlFilePaths(@Mocked TPaloBrokerService.Client client, @Mocked Catalog catalog,
                                    @Injectable BrokerMgr brokerMgr) throws Exception {
        // list response
        TBrokerListResponse response = new TBrokerListResponse();
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.statusCode = TBrokerOperationStatusCode.OK;
        response.opStatus = status;
        List<TBrokerFileStatus> files = Lists.newArrayList();
        String filePath = "hdfs://127.0.0.1:10000/doris/jobs/1/label6/9/label6.10.11.12.0.666666.parquet";
        files.add(new TBrokerFileStatus(filePath, false, 10, false));
        response.files = files;

        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        new MockUp<GenericPool<TPaloBrokerService.Client>>() {
            @Mock
            public TPaloBrokerService.Client borrowObject(TNetworkAddress address) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TPaloBrokerService.Client object) {
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TPaloBrokerService.Client object) {
            }
        };

        new Expectations() {
            {
                client.listPath((TBrokerListPathRequest) any);
                result = response;
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.getBroker(anyString, anyString);
                result = fsBroker;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        Map<String, Long> filePathToSize = handler.getEtlFilePaths(etlOutputPath, brokerDesc);
        Assert.assertTrue(filePathToSize.containsKey(filePath));
        Assert.assertEquals(10, (long) filePathToSize.get(filePath));
    }

    @Test
    public void testDeleteEtlOutputPath(@Mocked BrokerUtil brokerUtil) throws UserException {
        new Expectations() {
            {
                BrokerUtil.deletePath(etlOutputPath, (BrokerDesc) any);
                times = 1;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        try {
            handler.deleteEtlOutputPath(etlOutputPath, brokerDesc);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}