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

import org.apache.doris.PaloFe;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TEtlState;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

public class SparkEtlJobHandler {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJobHandler.class);

    private static final String JOB_CONFIG_DIR = PaloFe.DORIS_HOME_DIR + "/temp/job_conf";
    private static final String APP_RESOURCE = PaloFe.DORIS_HOME_DIR + "/lib/palo-fe.jar";
    private static final String MAIN_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";
    private static final String ETL_JOB_NAME = "doris__%s";
    // http://host:port/api/v1/applications/appid/jobs
    private static final String STATUS_URL = "%s/api/v1/applications/%s/jobs";

    public static final String NUM_TASKS = "numTasks";
    public static final String NUM_COMPLETED_TASKS = "numCompletedTasks";

    class SparkAppListener implements Listener {
        @Override
        public void stateChanged(SparkAppHandle sparkAppHandle) {}

        @Override
        public void infoChanged(SparkAppHandle sparkAppHandle) {}
    }

    public SparkAppHandle submitEtlJob(long loadJobId, String loadLabel, String sparkMaster,
                                       Map<String, String> sparkConfigs, String jobJsonConfig) throws LoadException {
        // check outputPath exist

        // create job config file
        String configDirPath = JOB_CONFIG_DIR + "/" + loadJobId;
        String configFilePath = configDirPath + "/" + EtlJobConfig.JOB_CONFIG_FILE_NAME;
        try {
            createJobConfigFile(configDirPath, configFilePath, loadJobId, jobJsonConfig);
        } catch (LoadException e) {
            return null;
        }

        // spark cluster config
        SparkLauncher launcher = new SparkLauncher();
        launcher = launcher.setMaster(sparkMaster)
                .setAppResource(APP_RESOURCE)
                .setMainClass(MAIN_CLASS)
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .addFile(configFilePath);
                //.setDeployMode("cluster")
                //.addSparkArg("--jars", "")
                //addSparkArg("--files", "")
        for (Map.Entry<String, String> entry : sparkConfigs.entrySet()) {
            launcher = launcher.setConf(entry.getKey(), entry.getValue());
        }

        // start app
        SparkAppHandle handle = null;
        try {
            handle = launcher.startApplication(new SparkAppListener());
        } catch (IOException e) {
            String errMsg = "start spark app fail, error: " + e.toString();
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        } finally {
            // delete config file
            // Util.deleteDirectory(new File(configDirPath));
        }

        return handle;
    }

    private void createJobConfigFile(String configDirPath, String configFilePath,
                                     long loadJobId, String jsonConfig) throws LoadException {
        // check config dir
        File configDir = new File(configDirPath);
        if (!Util.deleteDirectory(configDir)) {
            String errMsg = "delete config dir error. job: " + loadJobId;
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        }
        if (!configDir.mkdirs()) {
            String errMsg = "create config dir error. job: " + loadJobId;
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        }

        // write file
        File configFile = new File(configFilePath);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFile),
                                                           "UTF-8"));
            bw.write(jsonConfig);
            bw.flush();
        } catch (IOException e) {
            Util.deleteDirectory(configDir);
            String errMsg = "create config file error. job: " + loadJobId;
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    LOG.warn("close buffered writer error", e);
                }
            }
        }
    }


    public EtlStatus getEtlJobStatus(SparkAppHandle handle, long loadJobId, String statusServer) {
        EtlStatus status = new EtlStatus();

        // state
        State etlJobState = handle.getState();
        if (etlJobState == State.FINISHED) {
            status.setState(TEtlState.FINISHED);
        } else if (etlJobState == State.FAILED || etlJobState == State.KILLED || etlJobState == State.LOST) {
            status.setState(TEtlState.CANCELLED);
        } else {
            // UNKNOWN CONNECTED SUBMITTED RUNNING
            status.setState(TEtlState.RUNNING);
        }

        // get spark appid
        String appId = handle.getAppId();
        if (appId == null) {
            LOG.info("spark app handle get null appid, and check in next round. load job id: {}, state: {}",
                     loadJobId, etlJobState);
            return status;
        }

        // stats
        String statusUrl = String.format(STATUS_URL, statusServer, appId);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(statusUrl);
        CloseableHttpResponse httpResponse = null;
        String responseJson = null;
        try {
            httpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();
            responseJson = EntityUtils.toString(httpEntity);
            LOG.info("get spark app status success. load job id: {}, response: {}", loadJobId, responseJson);
        } catch (IOException e) {
            LOG.warn("get spark app status fail. load job id: {}, error: {}", loadJobId, e);
            return status;
        } finally {
            try {
                if (httpResponse != null) {
                    httpResponse.close();
                }
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException e) {
                LOG.warn("close http response or client fail. load job id: {}, error: {}", loadJobId, e);
            }
        }

        // [{ "jobId" : 0, "name" : "foreachPartition at Dpp.java:248",
        //    "submissionTime" : "2020-02-18T09:09:46.398GMT", "stageIds" : [ 0, 1, 2 ], "status" : "RUNNING",
        //    "numTasks" : 12, "numActiveTasks" : 3, "numCompletedTasks" : 9, "numSkippedTasks" : 0,
        //    "numFailedTasks" : 0, "numKilledTasks" : 0, "numCompletedIndices" : 9, "numActiveStages" : 1,
        //    "numCompletedStages" : 2, "numSkippedStages" : 0, "numFailedStages" : 0, "killedTasksSummary" : { }
        //  }]
        List<Map<String, Object>> jobInfos = new Gson().fromJson(responseJson, List.class);
        Map<String, String> stats = Maps.newHashMap();
        int numTasks = 0;
        int numCompletedTasks = 0;
        for (Map<String, Object> jobInfo : jobInfos) {
            if (jobInfo.containsKey(NUM_TASKS)) {
                numTasks += new Double((double) jobInfo.get(NUM_TASKS)).intValue();
            }
            if (jobInfo.containsKey(NUM_COMPLETED_TASKS)) {
                numCompletedTasks += new Double((double) jobInfo.get(NUM_COMPLETED_TASKS)).intValue();
            }
        }

        stats.put(NUM_TASKS, String.valueOf(numTasks));
        stats.put(NUM_COMPLETED_TASKS, String.valueOf(numCompletedTasks));
        status.setStats(stats);

        // counters

        return status;
    }

    public void killEtlJob(SparkAppHandle handle) {
        handle.stop();
    }

    public Map<String, Long> getEtlFilePaths(String outputPath, BrokerDesc brokerDesc) throws Exception {
        Map<String, Long> filePathToSize = Maps.newHashMap();

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        try {
            BrokerUtil.parseBrokerFile(outputPath, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new Exception(e);
        }

        for (TBrokerFileStatus fstatus : fileStatuses) {
            if (fstatus.isDir) {
                continue;
            }
            filePathToSize.put(fstatus.getPath(), fstatus.getSize());
        }
        LOG.debug("get spark etl file paths. files map: {}", filePathToSize);

        return filePathToSize;
    }
}
