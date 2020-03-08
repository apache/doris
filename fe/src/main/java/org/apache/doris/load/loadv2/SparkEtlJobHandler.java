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
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.EtlStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.collect.Maps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

public class SparkEtlJobHandler {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJobHandler.class);

    private static final String JOB_CONFIG_DIR = PaloFe.DORIS_HOME_DIR + "/temp/job_conf";
    private static final String JOB_CONFIG_FILE = "jobconfig.json";
    private static final String APP_RESOURCE = PaloFe.DORIS_HOME_DIR + "/lib/spark-etl-1.0.0.jar";
    private static final String MAIN_CLASS = "org.apache.doris.dpp.Dpp";
    private static final String ETL_JOB_NAME = "doris__%s";
    // hdfs://host:port/outputPath/dbId/loadLabel/PendingTaskSignature
    private static final String ETL_OUTPUT_PATH = "%s/%s/%d/%s/%d";

    class SparkAppListener implements Listener {
        @Override
        public void stateChanged(SparkAppHandle sparkAppHandle) {
            sparkAppHandle.getAppId();
            sparkAppHandle.getState();
        }

        @Override
        public void infoChanged(SparkAppHandle sparkAppHandle) {}
    }

    public SparkAppHandle submitEtlJob(long loadJobId, String loadLabel, String sparkMaster,
                                       Map<String, String> sparkConfigs, String jobJsonConfig) throws LoadException {
        // create job config file
        String configDirPath = JOB_CONFIG_DIR + "/" + loadJobId;
        String configFilePath = configDirPath + "/" + JOB_CONFIG_FILE;
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
                .addAppArgs(configFilePath);
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
            Util.deleteDirectory(new File(configDirPath));
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


    public EtlStatus getEtlJobStatus(String etlJobId) {
        return new EtlStatus();
    }

    public void killEtlJob(String etlJobId) {

    }

    public Map<String, Long> getEtlFilePaths(String outputPath) {
        return Maps.newHashMap();
    }

    public static String getOutputPath(String fsDefaultName, String outputPath, long dbId,
                                       String loadLabel, long taskSignature) {
        return String.format(ETL_OUTPUT_PATH, fsDefaultName, outputPath, dbId, loadLabel, taskSignature);
    }
}
