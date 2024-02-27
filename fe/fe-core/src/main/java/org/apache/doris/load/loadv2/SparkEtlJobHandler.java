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
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.CommandResult;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.SparkLoadAppHandle.State;
import org.apache.doris.sparkdpp.DppResult;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TEtlState;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
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

    private static final String CONFIG_FILE_NAME = "jobconfig.json";
    private static final String JOB_CONFIG_DIR = "configs";
    private static final String ETL_JOB_NAME = "doris__%s";
    private static final String LAUNCHER_LOG = "spark_launcher_%s_%s.log";
    // 5min
    private static final long GET_APPID_TIMEOUT_MS = 300000L;
    // 30s
    private static final long EXEC_CMD_TIMEOUT_MS = 30000L;
    // yarn command
    private static final String YARN_STATUS_CMD = "%s --config %s application -status %s";
    private static final String YARN_KILL_CMD = "%s --config %s application -kill %s";

    private static final String SPARK_ETL_JOB_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";

    public void submitEtlJob(long loadJobId, String loadLabel, EtlJobConfig etlJobConfig, SparkResource resource,
            BrokerDesc brokerDesc, SparkLoadAppHandle handle, SparkPendingTaskAttachment attachment)
            throws LoadException {
        // delete outputPath
        deleteEtlOutputPath(etlJobConfig.outputPath, brokerDesc);

        // init local dir
        if (!FeConstants.runningUnitTest) {
            initLocalDir();
        }

        // prepare dpp archive
        SparkRepository.SparkArchive archive = resource.prepareArchive();
        SparkRepository.SparkLibrary dppLibrary = archive.getDppLibrary();
        SparkRepository.SparkLibrary spark2xLibrary = archive.getSpark2xLibrary();

        // spark home
        String sparkHome = Config.spark_home_default_dir;
        // etl config path
        String configsHdfsDir = etlJobConfig.outputPath + "/" + JOB_CONFIG_DIR + "/";
        // etl config json path
        String jobConfigHdfsPath = configsHdfsDir + CONFIG_FILE_NAME;
        // spark submit app resource path
        String appResourceHdfsPath = dppLibrary.remotePath;
        // spark yarn archive path
        String jobArchiveHdfsPath = spark2xLibrary.remotePath;
        // spark yarn stage dir
        String jobStageHdfsPath = resource.getWorkingDir();
        // spark launcher log path
        String logFilePath = Config.spark_launcher_log_dir + "/" + String.format(LAUNCHER_LOG, loadJobId, loadLabel);

        // update archive and stage configs here
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        if (Strings.isNullOrEmpty(sparkConfigs.get("spark.yarn.archive"))) {
            sparkConfigs.put("spark.yarn.archive", jobArchiveHdfsPath);
        }
        if (Strings.isNullOrEmpty(sparkConfigs.get("spark.yarn.stage.dir"))) {
            sparkConfigs.put("spark.yarn.stage.dir", jobStageHdfsPath);
        }

        LOG.info("submit etl spark job, sparkConfigs:{}", sparkConfigs);

        try {
            byte[] configData = etlJobConfig.configToJson().getBytes("UTF-8");
            BrokerUtil.writeFile(configData, jobConfigHdfsPath, brokerDesc);
        } catch (UserException | UnsupportedEncodingException e) {
            throw new LoadException(e.getMessage());
        }

        Map<String, String> envParams = resource.getEnvConfigsWithoutPrefix();
        LOG.info("submit etl job,env:{}", envParams);

        SparkLauncher launcher = new SparkLauncher(envParams);
        // master      |  deployMode
        // ------------|-------------
        // yarn        |  cluster
        // spark://xx  |  client
        launcher.setMaster(resource.getMaster())
                .setDeployMode(resource.getDeployMode().name().toLowerCase())
                .setAppResource(appResourceHdfsPath)
                .setMainClass(SPARK_ETL_JOB_CLASS)
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .setSparkHome(sparkHome)
                .addAppArgs(jobConfigHdfsPath)
                .redirectError();

        // spark configs
        for (Map.Entry<String, String> entry : resource.getSparkConfigs().entrySet()) {
            launcher.setConf(entry.getKey(), entry.getValue());
        }

        // start app
        State state = null;
        String appId = null;
        String errMsg = "start spark app failed. error: ";
        try {
            Process process = launcher.launch();
            handle.setProcess(process);
            if (!FeConstants.runningUnitTest) {
                SparkLauncherMonitor.LogMonitor logMonitor =
                        SparkLauncherMonitor.createLogMonitor(handle, sparkConfigs);
                logMonitor.setSubmitTimeoutMs(GET_APPID_TIMEOUT_MS);
                logMonitor.setRedirectLogPath(logFilePath);
                logMonitor.start();
                try {
                    logMonitor.join();
                } catch (InterruptedException e) {
                    logMonitor.interrupt();
                    throw new LoadException(errMsg + e.getMessage());
                }
            }
            appId = handle.getAppId();
            state = handle.getState();
        } catch (IOException e) {
            LOG.warn(errMsg, e);
            throw new LoadException(errMsg + e.getMessage());
        }

        if (fromSparkState(state) == TEtlState.CANCELLED) {
            throw new LoadException(errMsg + "spark app state: " + state.toString() + ", loadJobId:" + loadJobId);
        }

        if (appId == null) {
            throw new LoadException(errMsg + "Waiting too much time to get appId from handle. spark app state: "
                    + state.toString() + ", loadJobId:" + loadJobId);
        }

        // success
        attachment.setAppId(appId);
        attachment.setHandle(handle);
    }

    public EtlStatus getEtlJobStatus(SparkLoadAppHandle handle, String appId, long loadJobId, String etlOutputPath,
                                     SparkResource resource, BrokerDesc brokerDesc) throws LoadException {
        EtlStatus status = new EtlStatus();

        Preconditions.checkState(appId != null && !appId.isEmpty());
        if (resource.isYarnMaster()) {
            // prepare yarn config
            String configDir = resource.prepareYarnConfig();
            // yarn client path
            String yarnClient = resource.getYarnClientPath();
            // command: yarn --config configDir application -status appId
            String yarnStatusCmd = String.format(YARN_STATUS_CMD, yarnClient, configDir, appId);
            LOG.info(yarnStatusCmd);

            Map<String, String> envParams = resource.getEnvConfigsWithoutPrefix();
            int envNums = envParams.size() + 1;
            String[] envp = new String[envNums];
            int idx = 0;
            envp[idx++] = "LC_ALL=" + Config.locale;
            if (envParams.size() > 0) {
                for (Map.Entry<String, String> entry : envParams.entrySet()) {
                    String envItem = entry.getKey() + "=" + entry.getValue();
                    envp[idx++] = envItem;
                }
            }
            LOG.info("getEtlJobStatus,appId:{}, loadJobId:{}, env:{},resource:{}", appId, loadJobId, envp, resource);
            CommandResult result = Util.executeCommand(yarnStatusCmd, envp, EXEC_CMD_TIMEOUT_MS);
            if (result.getReturnCode() != 0) {
                String stderr = result.getStderr();
                if (stderr != null) {
                    // case application not exists
                    if (stderr.contains("doesn't exist in RM")) {
                        LOG.warn("spark app not found. spark app id: {}, load job id: {}", appId, loadJobId);
                        status.setState(TEtlState.CANCELLED);
                        status.setFailMsg(stderr);
                    }
                }
                LOG.warn("yarn application status failed. spark app id: {}, load job id: {}, timeout: {}, msg: {}",
                            appId, loadJobId, EXEC_CMD_TIMEOUT_MS, stderr);
                status.setState(TEtlState.CANCELLED);
                status.setFailMsg(stderr);
                return status;
            }
            ApplicationReport report = new YarnApplicationReport(result.getStdout()).getReport();
            LOG.info("yarn application -status {}. load job id: {}, output: {}, report: {}",
                    appId, loadJobId, result.getStdout(), report);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus faStatus = report.getFinalApplicationStatus();
            status.setState(fromYarnState(state, faStatus));
            if (status.getState() == TEtlState.CANCELLED) {
                if (state == YarnApplicationState.FINISHED) {
                    status.setFailMsg("spark app state: " + faStatus.toString());
                } else {
                    status.setFailMsg("yarn app state: " + state.toString());
                }
            }
            status.setTrackingUrl(handle.getUrl() != null ? handle.getUrl() : report.getTrackingUrl());
            status.setProgress((int) (report.getProgress() * 100));
        } else {
            // state from handle
            if (handle == null) {
                status.setFailMsg("spark app handle is null");
                status.setState(TEtlState.CANCELLED);
                return status;
            }

            State state = handle.getState();
            status.setState(fromSparkState(state));
            if (status.getState() == TEtlState.CANCELLED) {
                status.setFailMsg("spark app state: " + state.toString());
            }
            LOG.info("spark app id: {}, load job id: {}, app state: {}", appId, loadJobId, state);
        }

        if (status.getState() == TEtlState.FINISHED || status.getState() == TEtlState.CANCELLED) {
            // get dpp result
            String dppResultFilePath = EtlJobConfig.getDppResultFilePath(etlOutputPath);
            try {
                byte[] data = BrokerUtil.readFile(dppResultFilePath, brokerDesc, 0);
                String dppResultStr = new String(data, "UTF-8");
                DppResult dppResult = new Gson().fromJson(dppResultStr, DppResult.class);
                if (dppResult != null) {
                    status.setDppResult(dppResult);
                    if (status.getState() == TEtlState.CANCELLED && !Strings.isNullOrEmpty(dppResult.failedReason)) {
                        status.setFailMsg(dppResult.failedReason);
                    }
                }
            } catch (UserException | JsonSyntaxException | UnsupportedEncodingException e) {
                LOG.warn("read broker file failed. path: {}", dppResultFilePath, e);
            }
        }

        return status;
    }

    public void killEtlJob(SparkLoadAppHandle handle, String appId,
            long loadJobId, SparkResource resource) throws LoadException {
        if (resource.isYarnMaster()) {
            // The appId may be empty when the load job is in PENDING phase. This is because the appId is
            // parsed from the spark launcher process's output (spark launcher process submit job and then
            // return appId). In this case, the spark job has still not been submitted, we only need to kill
            // the spark launcher process.
            if (Strings.isNullOrEmpty(appId)) {
                appId = handle.getAppId();
                if (Strings.isNullOrEmpty(appId)) {
                    handle.kill();
                    return;
                }
            }
            // prepare yarn config
            String configDir = resource.prepareYarnConfig();
            // yarn client path
            String yarnClient = resource.getYarnClientPath();
            // command: yarn --config configDir application -kill appId
            String yarnKillCmd = String.format(YARN_KILL_CMD, yarnClient, configDir, appId);
            LOG.info(yarnKillCmd);
            Map<String, String> envParams = resource.getEnvConfigsWithoutPrefix();
            int envNums = envParams.size() + 1;
            String[] envp = new String[envNums];
            int idx = 0;
            envp[idx++] = "LC_ALL=" + Config.locale;
            if (envParams.size() > 0) {
                for (Map.Entry<String, String> entry : envParams.entrySet()) {
                    String envItem = entry.getKey() + "=" + entry.getValue();
                    envp[idx++] = envItem;
                }
            }
            LOG.info("killEtlJob, env:{}", envp);
            CommandResult result = Util.executeCommand(yarnKillCmd, envp, EXEC_CMD_TIMEOUT_MS);
            LOG.info("yarn application -kill {}, output: {}", appId, result.getStdout());
            if (result.getReturnCode() != 0) {
                String stderr = result.getStderr();
                LOG.warn("yarn application kill failed. app id: {}, load job id: {}, msg: {}",
                        appId, loadJobId, stderr);
            }
        } else {
            if (handle != null) {
                handle.stop();
            }
        }
    }

    public Map<String, Long> getEtlFilePaths(String outputPath, BrokerDesc brokerDesc) throws Exception {
        Map<String, Long> filePathToSize = Maps.newHashMap();

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        String etlFilePaths = outputPath + "/*";
        try {
            BrokerUtil.parseFile(etlFilePaths, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new Exception(e);
        }

        for (TBrokerFileStatus fstatus : fileStatuses) {
            if (fstatus.isDir) {
                continue;
            }
            filePathToSize.put(fstatus.getPath(), fstatus.getSize());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("get spark etl file paths. files map: {}", filePathToSize);
        }

        return filePathToSize;
    }

    public static synchronized void initLocalDir() {
        String logDir = Config.spark_launcher_log_dir;
        File file = new File(logDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void deleteEtlOutputPath(String outputPath, BrokerDesc brokerDesc) {
        try {
            BrokerUtil.deletePath(outputPath, brokerDesc);
            LOG.info("delete path success. path: {}", outputPath);
        } catch (UserException e) {
            LOG.warn("delete path failed. path: {}", outputPath, e);
        }
    }

    private TEtlState fromYarnState(YarnApplicationState state, FinalApplicationStatus faStatus) {
        switch (state) {
            case FINISHED:
                if (faStatus == FinalApplicationStatus.SUCCEEDED) {
                    // finish and success
                    return TEtlState.FINISHED;
                } else {
                    // finish but fail
                    return TEtlState.CANCELLED;
                }
            case FAILED:
            case KILLED:
                // not finish
                return TEtlState.CANCELLED;
            default:
                // ACCEPTED NEW NEW_SAVING RUNNING SUBMITTED
                return TEtlState.RUNNING;
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
