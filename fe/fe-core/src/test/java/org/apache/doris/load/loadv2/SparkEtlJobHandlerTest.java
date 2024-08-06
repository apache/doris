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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.CommandResult;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.sparkdpp.EtlJobConfig;
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
import mockit.MockUp;
import mockit.Mocked;
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
    private String dppVersion;
    private String remoteArchivePath;
    private SparkRepository.SparkArchive archive;

    private final String runningReport = "Application Report :\n"
            + "Application-Id : application_15888888888_0088\n"
            + "Application-Name : label0\n"
            + "Application-Type : SPARK-2.4.1\n"
            + "User : test\n"
            + "Queue : test-queue\n"
            + "Start-Time : 1597654469958\n"
            + "Finish-Time : 0\n"
            + "Progress : 50%\n"
            + "State : RUNNING\n"
            + "Final-State : UNDEFINED\n"
            + "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n"
            + "RPC Port : 40236\n"
            + "AM Host : host-name";

    private final String failedReport = "Application Report :\n"
            + "Application-Id : application_15888888888_0088\n"
            + "Application-Name : label0\n"
            + "Application-Type : SPARK-2.4.1\n"
            + "User : test\n"
            + "Queue : test-queue\n"
            + "Start-Time : 1597654469958\n"
            + "Finish-Time : 1597654801939\n"
            + "Progress : 100%\n"
            + "State : FINISHED\n"
            + "Final-State : FAILED\n"
            + "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n"
            + "RPC Port : 40236\n"
            + "AM Host : host-name";

    private final String finishReport = "Application Report :\n"
            + "Application-Id : application_15888888888_0088\n"
            + "Application-Name : label0\n"
            + "Application-Type : SPARK-2.4.1\n"
            + "User : test\n"
            + "Queue : test-queue\n"
            + "Start-Time : 1597654469958\n"
            + "Finish-Time : 1597654801939\n"
            + "Progress : 100%\n"
            + "State : FINISHED\n"
            + "Final-State : SUCCEEDED\n"
            + "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n"
            + "RPC Port : 40236\n"
            + "AM Host : host-name";


    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        loadJobId = 0L;
        label = "label0";
        resourceName = "spark0";
        broker = "broker0";
        pendingTaskId = 3L;
        appId = "application_15888888888_0088";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/doris/100/label/101";
        trackingUrl = "http://127.0.0.1:8080/proxy/application_1586619723848_0088/";
        dppVersion = Config.spark_dpp_version;
        remoteArchivePath = etlOutputPath + "/__repository__/__archive_" + dppVersion;
        archive = new SparkRepository.SparkArchive(remoteArchivePath, dppVersion);
        archive.libraries.add(new SparkRepository
                .SparkLibrary("", "", SparkRepository.SparkLibrary.LibType.DPP, 0L));
        archive.libraries.add(new SparkRepository
                .SparkLibrary("", "", SparkRepository.SparkLibrary.LibType.SPARK2X, 0L));
    }

    @Test
    public void testSubmitEtlJob(
            @Mocked BrokerUtil brokerUtil,
            @Mocked SparkLauncher launcher,
            @Injectable Process process,
            @Mocked SparkLoadAppHandle handle) throws IOException, LoadException {
        new Expectations() {
            {
                launcher.launch();
                result = process;
                handle.getAppId();
                result = appId;
                handle.getState();
                result = SparkLoadAppHandle.State.RUNNING;
            }
        };

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.prepareArchive();
                result = archive;
            }
        };

        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        handler.submitEtlJob(loadJobId, label, etlJobConfig, resource, brokerDesc, handle, attachment);

        // check submit etl job success
        Assert.assertEquals(appId, attachment.getAppId());
    }

    @Test(expected = LoadException.class)
    public void testSubmitEtlJobFailed(@Mocked BrokerUtil brokerUtil, @Mocked SparkLauncher launcher, @Injectable Process process,
                                       @Mocked SparkLoadAppHandle handle) throws IOException, LoadException {
        new Expectations() {
            {
                launcher.launch();
                result = process;
                handle.getAppId();
                result = appId;
                handle.getState();
                result = SparkLoadAppHandle.State.FAILED;
            }
        };

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.prepareArchive();
                result = archive;
            }
        };

        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        handler.submitEtlJob(loadJobId, label, etlJobConfig, resource, brokerDesc, handle, attachment);
    }

    @Test
    public void testGetEtlJobStatus(@Mocked BrokerUtil brokerUtil, @Mocked Util util, @Mocked CommandResult commandResult,
                                    @Mocked SparkYarnConfigFiles sparkYarnConfigFiles, @Mocked SparkLoadAppHandle handle)
            throws IOException, UserException {

        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";

                commandResult.getReturnCode();
                result = 0;
                commandResult.getStdout();
                returns(runningReport, runningReport, failedReport, failedReport, finishReport, finishReport);

                handle.getUrl();
                result = trackingUrl;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any);
                minTimes = 0;
                result = commandResult;

                BrokerUtil.readFile(anyString, (BrokerDesc) any, anyLong);
                result = "{'normal_rows': 10, 'abnormal_rows': 0, 'failed_reason': 'etl job failed'}";
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkEtlJobHandler handler = new SparkEtlJobHandler();

        // running
        EtlStatus status = handler.getEtlJobStatus(handle, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.RUNNING, status.getState());
        Assert.assertEquals(50, status.getProgress());
        Assert.assertEquals(trackingUrl, status.getTrackingUrl());

        // yarn finished and spark failed
        status = handler.getEtlJobStatus(handle, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.CANCELLED, status.getState());
        Assert.assertEquals(100, status.getProgress());
        Assert.assertEquals("etl job failed", status.getDppResult().failedReason);

        // finished
        status = handler.getEtlJobStatus(handle, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.FINISHED, status.getState());
        Assert.assertEquals(100, status.getProgress());
        Assert.assertEquals(trackingUrl, status.getTrackingUrl());
        Assert.assertEquals(10, status.getDppResult().normalRows);
        Assert.assertEquals(0, status.getDppResult().abnormalRows);
    }

    @Test
    public void testGetEtlJobStatusFailed(@Mocked Util util, @Mocked CommandResult commandResult,
                                          @Mocked SparkYarnConfigFiles sparkYarnConfigFiles, @Mocked SparkLoadAppHandle handle)
            throws IOException, UserException {

        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";

                commandResult.getReturnCode();
                result = -1;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any);
                minTimes = 0;
                result = commandResult;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkEtlJobHandler handler = new SparkEtlJobHandler();

        // yarn finished and spark failed
        EtlStatus status = handler.getEtlJobStatus(null, appId, loadJobId, etlOutputPath, resource, brokerDesc);
        Assert.assertEquals(TEtlState.CANCELLED, status.getState());
    }

    @Test
    public void testKillEtlJob(@Mocked Util util, @Mocked CommandResult commandResult,
                               @Mocked SparkYarnConfigFiles sparkYarnConfigFiles) throws IOException, UserException {
        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";
            }
        };

        new Expectations() {
            {
                commandResult.getReturnCode();
                result = 0;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any);
                minTimes = 0;
                result = commandResult;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        try {
            handler.killEtlJob(null, appId, loadJobId, resource);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetEtlFilePaths(@Mocked TPaloBrokerService.Client client, @Mocked Env env,
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
                env.getBrokerMgr();
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
                BrokerUtil.deletePathWithBroker(etlOutputPath, (BrokerDesc) any);
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
