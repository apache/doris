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
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.thrift.TEtlState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * SparkEtlJobHandler is responsible for
 * 1. submit spark etl job
 * 2. get spark etl job status
 * 3. kill spark etl job
 * 4. get spark etl file paths
 * 5. delete etl output path
 */
public class SparkEtlJobHandler {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJobHandler.class);

    private static final String APP_RESOURCE_NAME = "palo-fe.jar";
    private static final String CONFIG_FILE_NAME = "jobconfig.json";
    private static final String APP_RESOURCE_LOCAL_PATH = PaloFe.DORIS_HOME_DIR + "/lib/" + APP_RESOURCE_NAME;
    private static final String JOB_CONFIG_DIR = "configs";
    private static final String MAIN_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";
    private static final String ETL_JOB_NAME = "doris__%s";
    // 5min
    private static final int GET_APPID_MAX_RETRY_TIMES = 300;
    private static final int GET_APPID_SLEEP_MS = 1000;

    class SparkAppListener implements Listener {
        @Override
        public void stateChanged(SparkAppHandle sparkAppHandle) {}

        @Override
        public void infoChanged(SparkAppHandle sparkAppHandle) {}
    }

    public void submitEtlJob(long loadJobId, String loadLabel, EtlJobConfig etlJobConfig, SparkResource resource,
                             BrokerDesc brokerDesc, SparkPendingTaskAttachment attachment) throws LoadException {
        // delete outputPath
        deleteEtlOutputPath(etlJobConfig.outputPath, brokerDesc);

        // upload app resource and jobconfig to hdfs
        String configsHdfsDir = etlJobConfig.outputPath + "/" + JOB_CONFIG_DIR + "/";
        String appResourceHdfsPath = configsHdfsDir + APP_RESOURCE_NAME;
        String jobConfigHdfsPath = configsHdfsDir + CONFIG_FILE_NAME;
        try {
            BrokerUtil.writeFile(APP_RESOURCE_LOCAL_PATH, appResourceHdfsPath, brokerDesc);
            byte[] configData = etlJobConfig.configToJson().getBytes("UTF-8");
            BrokerUtil.writeFile(configData, jobConfigHdfsPath, brokerDesc);
        } catch (UserException | UnsupportedEncodingException e) {
            throw new LoadException(e.getMessage());
        }

        SparkLauncher launcher = new SparkLauncher();
        // master      |  deployMode
        // ------------|-------------
        // yarn        |  cluster
        // spark://xx  |  client
        launcher.setMaster(resource.getMaster())
                .setDeployMode(resource.getDeployMode().name().toLowerCase())
                .setAppResource(appResourceHdfsPath)
				// TODO(wyb): spark-load
                // replace with getCanonicalName later
                //.setMainClass(SparkEtlJob.class.getCanonicalName())
                .setMainClass(MAIN_CLASS)
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .addAppArgs(jobConfigHdfsPath);
        // spark configs
        for (Map.Entry<String, String> entry : resource.getSparkConfigs().entrySet()) {
            launcher.setConf(entry.getKey(), entry.getValue());
        }

        // start app
        SparkAppHandle handle = null;
        State state = null;
        String appId = null;
        int retry = 0;
        String errMsg = "start spark app failed. error: ";
        try {
            handle = launcher.startApplication(new SparkAppListener());
        } catch (IOException e) {
            LOG.warn(errMsg, e);
            throw new LoadException(errMsg + e.getMessage());
        }

        while (retry++ < GET_APPID_MAX_RETRY_TIMES) {
            appId = handle.getAppId();
            if (appId != null) {
                break;
            }

            // check state and retry
            state = handle.getState();
            if (fromSparkState(state) == TEtlState.CANCELLED) {
                throw new LoadException(errMsg + "spark app state: " + state.toString());
            }
            if (retry >= GET_APPID_MAX_RETRY_TIMES) {
                throw new LoadException(errMsg + "wait too much time for getting appid. spark app state: "
                                                + state.toString());
            }

            // log
            if (retry % 10 == 0) {
                LOG.info("spark appid that handle get is null. load job id: {}, state: {}, retry times: {}",
                         loadJobId, state.toString(), retry);
            }
            try {
                Thread.sleep(GET_APPID_SLEEP_MS);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }

        // success
        attachment.setAppId(appId);
        attachment.setHandle(handle);
    }

    public void deleteEtlOutputPath(String outputPath, BrokerDesc brokerDesc) {
        try {
            BrokerUtil.deletePath(outputPath, brokerDesc);
            LOG.info("delete path success. path: {}", outputPath);
        } catch (UserException e) {
            LOG.warn("delete path failed. path: {}", outputPath, e);
        }
    }

    private TEtlState fromSparkState(State state) {
        switch (state) {
            case FINISHED:
                return TEtlState.FINISHED;
            case FAILED:
            case KILLED:
            case LOST:
                return TEtlState.CANCELLED;
            default:
                // UNKNOWN CONNECTED SUBMITTED RUNNING
                return TEtlState.RUNNING;
        }
    }
}
