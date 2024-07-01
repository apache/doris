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

package org.apache.doris.load;

import org.apache.doris.DorisFE;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.CommandResult;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class DppScheduler {
    private static final Logger LOG = LogManager.getLogger(DppScheduler.class);

    private static final String HADOOP_CLIENT = DorisFE.DORIS_HOME_DIR + Config.dpp_hadoop_client_path;
    private static final String DPP_OUTPUT_DIR = "export";
    private static final String JOB_CONFIG_DIR = DorisFE.DORIS_HOME_DIR + "/temp/job_conf";
    private static final String JOB_CONFIG_FILE = "jobconfig.json";
    private static final String LOCAL_DPP_DIR = DorisFE.DORIS_HOME_DIR + "/lib/dpp/" + FeConstants.dpp_version;
    private static final int DEFAULT_REDUCE_NUM = 1000;
    private static final long GB = 1024 * 1024 * 1024L;

    // hdfs://host:port/outputPath/dbId/loadLabel/etlOutputDir
    private static final String ETL_OUTPUT_PATH = "%s%s/%d/%s/%s";
    private static final String ETL_JOB_NAME = "palo2__%s__%s";

    // hadoop command
    private static final String HADOOP_BISTREAMING_CMD = "%s bistreaming %s -D mapred.job.name=\"%s\" "
            + "-input %s -output %s "
            + "-mapper \"sh mapred/mapper.sh\" "
            + "-reducer \"sh mapred/reducer.sh '\\\"%s\\\"'\" "
            + "-partitioner com.baidu.sos.mapred.lib.MapIntPartitioner "
            + "-cacheArchive %s/dpp/x86_64-scm-linux-gnu.tar.gz#tc "
            + "-cacheArchive %s/dpp/pypy.tar.gz#pypy "
            + "-cacheArchive %s/dpp/palo_dpp_mr.tar.gz#mapred "
            + "-numReduceTasks %d -file \"%s\" ";
    private static final String HADOOP_STATUS_CMD = "%s job %s -status %s";
    private static final String HADOOP_KILL_CMD = "%s job %s -kill %s";
    private static final String HADOOP_LS_CMD = "%s fs %s -ls %s";
    private static final String HADOOP_COUNT_CMD = "%s fs %s -count %s";
    private static final String HADOOP_TEST_CMD = "%s fs %s -test %s %s";
    private static final String HADOOP_MKDIR_CMD = "%s fs %s -mkdir %s";
    private static final String HADOOP_RMR_CMD = "%s fs %s -rmr %s";
    private static final String HADOOP_PUT_CMD = "%s fs %s -put %s %s";
    private static final long HADOOP_SPEED_LIMIT_KB = 10240L; // 10M

    private static final ConcurrentMap<String, Object> DPP_LOCK_MAP = Maps.newConcurrentMap();

    private String hadoopConfig;
    private String applicationsPath;

    public DppScheduler(DppConfig dppConfig) {
        hadoopConfig = getHadoopConfigsStr(dppConfig.getHadoopConfigs());
        applicationsPath = dppConfig.getFsDefaultName() + dppConfig.getApplicationsPath();
    }

    private String getHadoopConfigsStr(Map<String, String> hadoopConfigs) {
        List<String> configs = Lists.newArrayList();
        for (Map.Entry<String, String> entry : hadoopConfigs.entrySet()) {
            configs.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }
        return String.format("-D %s", StringUtils.join(configs, " -D "));
    }

    public EtlSubmitResult submitEtlJob(long jobId, String loadLabel, String clusterName,
                                        String dbName, Map<String, Object> jobConf, int retry) {
        String etlJobId = null;
        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        List<String> failMsgs = Lists.newArrayList();
        status.setErrorMsgs(failMsgs);

        // check dpp lock map
        if (retry > 0) {
            // failed once, try check dpp application
            LOG.warn("submit etl retry[{}] > 0. check dpp application", retry);
            // prepare dpp applications
            DPP_LOCK_MAP.putIfAbsent(clusterName, new Object());
            Preconditions.checkState(DPP_LOCK_MAP.containsKey(clusterName));
            synchronized (DPP_LOCK_MAP.get(clusterName)) {
                try {
                    prepareDppApplications();
                } catch (LoadException e) {
                    status.setStatusCode(TStatusCode.CANCELLED);
                    failMsgs.add(e.getMessage());
                    return new EtlSubmitResult(status, null);
                }
            }
        }

        // create job config file
        String configDirPath = JOB_CONFIG_DIR + "/" + jobId;
        File configDir = new File(configDirPath);
        if (!Util.deleteDirectory(configDir)) {
            String errMsg = "delete config dir error. job: " + jobId;
            LOG.warn(errMsg + ", path: {}", configDirPath);
            status.setStatusCode(TStatusCode.CANCELLED);
            failMsgs.add(errMsg);
            return new EtlSubmitResult(status, null);
        }
        if (!configDir.mkdirs()) {
            String errMsg = "create config file dir error. job: " + jobId;
            LOG.warn(errMsg + ", path: {}", configDirPath);
            status.setStatusCode(TStatusCode.CANCELLED);
            failMsgs.add(errMsg);
            return new EtlSubmitResult(status, null);
        }
        File configFile = new File(configDirPath + "/" + JOB_CONFIG_FILE);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFile), "UTF-8"));
            Gson gson = new Gson();
            bw.write(gson.toJson(jobConf));
            bw.flush();
        } catch (IOException e) {
            Util.deleteDirectory(configDir);

            String errMsg = "create config file error. job: " + jobId;
            LOG.warn(errMsg + ", file: {}", configDirPath + "/" + JOB_CONFIG_FILE);
            status.setStatusCode(TStatusCode.CANCELLED);
            failMsgs.add(errMsg);
            return new EtlSubmitResult(status, null);
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    LOG.warn("close buffered writer error", e);
                    status.setStatusCode(TStatusCode.CANCELLED);
                    failMsgs.add(e.getMessage());
                    return new EtlSubmitResult(status, null);
                }
            }
        }

        // create input path
        Set<String> inputPaths = getInputPaths(jobConf);
        String inputPath = StringUtils.join(inputPaths, " -input ");

        // reduce num
        int reduceNumByInputSize = 0;
        try {
            reduceNumByInputSize = calcReduceNumByInputSize(inputPaths);
        } catch (InputSizeInvalidException e) {
            status.setStatusCode(TStatusCode.CANCELLED);
            failMsgs.add(e.getMessage());
            return new EtlSubmitResult(status, null);
        }
        int reduceNumByTablet = calcReduceNumByTablet(jobConf);
        int reduceNum = Math.min(reduceNumByInputSize, reduceNumByTablet);
        LOG.info("calculate reduce num. reduceNum: {}, reduceNumByInputSize: {}, reduceNumByTablet: {}",
                 reduceNum, reduceNumByInputSize, reduceNumByTablet);

        // rm path
        String outputPath = (String) jobConf.get("output_path");
        deleteEtlOutputPath(outputPath);

        // submit etl job
        String etlJobName = String.format(ETL_JOB_NAME, dbName, loadLabel);
        String hadoopRunCmd = String.format(HADOOP_BISTREAMING_CMD, HADOOP_CLIENT, hadoopConfig, etlJobName, inputPath,
                outputPath, hadoopConfig, applicationsPath, applicationsPath, applicationsPath, reduceNum,
                configFile.getAbsolutePath());
        LOG.info(hadoopRunCmd);
        String outputLine = null;
        List<String> hadoopRunCmdList = Util.shellSplit(hadoopRunCmd);
        String[] hadoopRunCmds = hadoopRunCmdList.toArray(new String[0]);
        BufferedReader errorReader = null;
        Process p = null;
        long startTime = System.currentTimeMillis();
        try {
            p = Runtime.getRuntime().exec(hadoopRunCmds);
            errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            for (int i = 0; i < 1000; i++) {
                outputLine = errorReader.readLine();
                LOG.info(outputLine);
                if (Strings.isNullOrEmpty(outputLine)) {
                    LOG.warn("submit etl job fail. job id: {}, label: {}", jobId, loadLabel);
                    break;
                }

                if (outputLine.toLowerCase().contains("error")
                        || outputLine.toLowerCase().contains("exception")) {
                    failMsgs.add(outputLine);
                }

                if (outputLine.indexOf("Running job") != -1) {
                    String[] arr = outputLine.split(":");
                    etlJobId = arr[arr.length - 1].trim();
                    break;
                }
            }
        } catch (IOException e) {
            LOG.warn("submit etl job error", e);
            status.setStatusCode(TStatusCode.CANCELLED);
            failMsgs.add(e.getMessage());
            return new EtlSubmitResult(status, null);
        } finally {
            Util.deleteDirectory(configDir);
            long endTime = System.currentTimeMillis();
            LOG.info("finished submit hadoop job: {}. cost: {} ms", jobId, endTime - startTime);
            if (p != null) {
                p.destroy();
            }
            if (errorReader != null) {
                try {
                    errorReader.close();
                } catch (IOException e) {
                    LOG.warn("close buffered reader error", e);
                    status.setStatusCode(TStatusCode.CANCELLED);
                    failMsgs.add(e.getMessage());
                    return new EtlSubmitResult(status, null);
                }
            }
        }

        if (etlJobId == null) {
            status.setStatusCode(TStatusCode.CANCELLED);
        }
        return new EtlSubmitResult(status, etlJobId);
    }

    private void prepareDppApplications() throws LoadException {
        String[] envp = { "LC_ALL=" + Config.locale };
        String hadoopDppDir = applicationsPath + "/dpp";
        boolean needUpload = false;

        // get local files
        File dppDir = new File(LOCAL_DPP_DIR);
        if (!dppDir.exists() || !dppDir.isDirectory()) {
            LOG.warn("dpp dir does not exist");
            throw new LoadException("dpp dir does not exist");
        }
        File[] localFiles = dppDir.listFiles();

        // test hadoop dpp dir
        String hadoopTestCmd = String.format(HADOOP_TEST_CMD, HADOOP_CLIENT, hadoopConfig, "-d", hadoopDppDir);
        LOG.info(hadoopTestCmd);
        CommandResult testResult = Util.executeCommand(hadoopTestCmd, envp);
        if (testResult.getReturnCode() == 0) {
            String hadoopDppFilePath = hadoopDppDir + "/*";
            String hadoopCountCmd = String.format(HADOOP_COUNT_CMD, HADOOP_CLIENT, hadoopConfig, hadoopDppFilePath);
            LOG.info(hadoopCountCmd);
            CommandResult countResult = Util.executeCommand(hadoopCountCmd, envp);
            if (countResult.getReturnCode() != 0) {
                LOG.warn("hadoop count error, result: {}", countResult);
                throw new LoadException("hadoop count error. msg: " + countResult.getStderr());
            }

            Map<String, Long> fileMap = Maps.newHashMap();
            String[] fileInfos = countResult.getStdout().split("\n");
            for (String fileInfo : fileInfos) {
                String[] fileInfoArr = fileInfo.trim().split(" +");
                if (fileInfoArr.length == 4) {
                    String filePath = fileInfoArr[3];
                    String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
                    long size = Long.parseLong(fileInfoArr[2]);
                    fileMap.put(fileName, size);
                }
            }

            // diff files
            for (File file : localFiles) {
                if (!file.isFile()) {
                    continue;
                }

                String fileName = file.getName();
                if (!fileMap.containsKey(fileName)) {
                    LOG.info("hadoop dpp file does not exist. file: {}", fileName);
                    needUpload = true;
                    break;
                }

                long localSize = file.length();
                long hadoopSize = fileMap.get(fileName);
                if (localSize != hadoopSize) {
                    LOG.info("dpp files size are different. file: {}, local: {}, hadoop: {}", fileName, localSize,
                            hadoopSize);
                    needUpload = true;
                    break;
                }
            }
        } else {
            LOG.info("hadoop dir does not exist. dir: {}", hadoopDppDir);
            needUpload = true;
        }

        if (needUpload) {
            // rmdir and mkdir
            String hadoopRmrCmd = String.format(HADOOP_RMR_CMD, HADOOP_CLIENT, hadoopConfig, hadoopDppDir);
            LOG.info(hadoopRmrCmd);
            Util.executeCommand(hadoopRmrCmd, envp);
            String hadoopMkdirCmd = String.format(HADOOP_MKDIR_CMD, HADOOP_CLIENT, hadoopConfig, hadoopDppDir);
            LOG.info(hadoopMkdirCmd);
            Util.executeCommand(hadoopMkdirCmd, envp);

            // upload dpp applications
            String hadoopPutConfig = hadoopConfig + String.format(" -D speed.limit.kb=%d", HADOOP_SPEED_LIMIT_KB);
            String hadoopPutCmd = null;
            CommandResult putResult = null;
            for (File file : localFiles) {
                hadoopPutCmd = String.format(HADOOP_PUT_CMD, HADOOP_CLIENT, hadoopPutConfig,
                        LOCAL_DPP_DIR + "/" + file.getName(), hadoopDppDir);
                LOG.info(hadoopPutCmd);
                putResult = Util.executeCommand(hadoopPutCmd, envp);
                if (putResult.getReturnCode() != 0) {
                    LOG.warn("hadoop put fail. result: {}", putResult);
                    throw new LoadException("hadoop put fail. msg: " + putResult.getStderr());
                }
            }
        }
    }

    private Set<String> getInputPaths(Map<String, Object> jobConf) {
        Set<String> inputPaths = new HashSet<String>();
        Map<String, Map> tables = (Map<String, Map>) jobConf.get("tables");
        for (Map<String, Map> table : tables.values()) {
            Map<String, Map> sourceFileSchema = (Map<String, Map>) table.get("source_file_schema");
            for (Map<String, List<String>> schema : sourceFileSchema.values()) {
                List<String> fileUrls = schema.get("file_urls");
                inputPaths.addAll(fileUrls);
            }
        }
        return inputPaths;
    }

    private int calcReduceNumByInputSize(Set<String> inputPaths) throws InputSizeInvalidException {
        String[] envp = { "LC_ALL=" + Config.locale };
        int reduceNum = 0;
        String hadoopCountCmd = String.format(HADOOP_COUNT_CMD, HADOOP_CLIENT, hadoopConfig,
                StringUtils.join(inputPaths, " "));
        LOG.info(hadoopCountCmd);
        CommandResult result = Util.executeCommand(hadoopCountCmd, envp);
        if (result.getReturnCode() != 0) {
            LOG.warn("hadoop count error, result: {}", result);
            return DEFAULT_REDUCE_NUM;
        }

        // calc total size
        long totalSizeB = 0L;
        String[] fileInfos = result.getStdout().split("\n");
        for (String fileInfo : fileInfos) {
            String[] fileInfoArr = fileInfo.trim().split(" +");
            if (fileInfoArr.length == 4) {
                totalSizeB += Long.parseLong(fileInfoArr[2]);
            }
        }

        // check input size limit
        int inputSizeLimitGB = 0;
        if (inputSizeLimitGB != 0) {
            if (totalSizeB > inputSizeLimitGB * GB) {
                String failMsg = "Input file size[" + (float) totalSizeB / GB + "GB]"
                        + " exceeds system limit[" + inputSizeLimitGB + "GB]";
                LOG.warn(failMsg);
                throw new InputSizeInvalidException(failMsg);
            }
        }

        if (totalSizeB != 0) {
            reduceNum = (int) (totalSizeB / Config.dpp_bytes_per_reduce) + 1;
        }
        return reduceNum;
    }

    private int calcReduceNumByTablet(Map<String, Object> jobConf) {
        int reduceNum = 0;
        Map<String, Map> tables = (Map<String, Map>) jobConf.get("tables");
        for (Map<String, Map> table : tables.values()) {
            Map<String, Map> views = (Map<String, Map>) table.get("views");
            for (Map<String, Object> view : views.values()) {
                if (view.containsKey("hash_mod")) {
                    // hash or random
                    reduceNum += (int) view.get("hash_mod");
                } else if (view.containsKey("key_ranges")) {
                    // key range
                    List<Object> rangeList = (List<Object>) view.get("key_ranges");
                    reduceNum += rangeList.size();
                }
            }
        }
        return reduceNum;
    }

    public EtlStatus getEtlJobStatus(String etlJobId) {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.RUNNING);
        String hadoopStatusCmd = String.format(HADOOP_STATUS_CMD, HADOOP_CLIENT, hadoopConfig, etlJobId);
        LOG.info(hadoopStatusCmd);

        String[] envp = { "LC_ALL=" + Config.locale };
        CommandResult result = Util.executeCommand(hadoopStatusCmd, envp);
        String stdout = result.getStdout();
        if (result.getReturnCode() != 0) {
            if (stdout != null && stdout.contains("Could not find job")) {
                LOG.warn("cannot find hadoop etl job: {}", etlJobId);
                status.setState(TEtlState.CANCELLED);
            }
            return status;
        }

        // stats and counters
        Map<String, String> stats = new HashMap<String, String>();
        Map<String, String> counters = new HashMap<String, String>();
        String[] stdoutLines = stdout.split("\n");
        String[] array = null;
        for (String line : stdoutLines) {
            array = line.split(":");
            if (array.length == 2) {
                stats.put(array[0].trim(), array[1].trim());
            }

            array = line.split("=");
            if (array.length == 2) {
                counters.put(array[0].trim(), array[1].trim());
            }
        }
        status.setStats(stats);
        status.setCounters(counters);

        // tracking url
        for (String key : counters.keySet()) {
            if (key.startsWith("tracking URL")) {
                // remove "tracking URL: ", total 14 chars
                status.setTrackingUrl(key.substring(14) + "=" + counters.get(key));
                break;
            }
        }

        // job state
        if (stats.containsKey("job state")) {
            int jobState = Integer.parseInt(stats.get("job state"));
            if (jobState == 3 || jobState == 5 || jobState == 6) {
                // 3:failed 5or6:killed --> cancelled
                status.setState(TEtlState.CANCELLED);
            } else if (jobState == 2) {
                // 2:success --> finished
                status.setState(TEtlState.FINISHED);
            } else {
                // 0:init 1:running 4:prepare --> running
                status.setState(TEtlState.RUNNING);
            }
        }

        return status;
    }

    public Map<String, Long> getEtlFiles(String outputPath) {
        String[] envp = { "LC_ALL=" + Config.locale };
        Map<String, Long> fileMap = Maps.newHashMap();

        String fileDir = outputPath + "/" + DPP_OUTPUT_DIR;
        String hadoopLsCmd = String.format(HADOOP_LS_CMD, HADOOP_CLIENT, hadoopConfig, fileDir);
        LOG.info(hadoopLsCmd);
        CommandResult lsResult = Util.executeCommand(hadoopLsCmd, envp);
        if (lsResult.getReturnCode() != 0) {
            // check outputPath exist
            String hadoopTestCmd = String.format(HADOOP_TEST_CMD, HADOOP_CLIENT, hadoopConfig, "-d", outputPath);
            LOG.info(hadoopTestCmd);
            CommandResult testResult = Util.executeCommand(hadoopTestCmd, envp);
            if (testResult.getReturnCode() != 0) {
                LOG.info("hadoop dir does not exist. dir: {}", outputPath);
                return null;
            }

            // check outputPath + DPP_OUTPUT_DIR exist
            hadoopTestCmd = String.format(HADOOP_TEST_CMD, HADOOP_CLIENT, hadoopConfig, "-d", fileDir);
            LOG.info(hadoopTestCmd);
            testResult = Util.executeCommand(hadoopTestCmd, envp);
            if (testResult.getReturnCode() != 0) {
                LOG.info("hadoop dir does not exist. dir: {}", fileDir);
                return fileMap;
            } else {
                return null;
            }
        }

        String stdout = lsResult.getStdout();
        String[] lsFileResults = stdout.split("\n");
        for (String line : lsFileResults) {
            // drwxr-xr-x   3 palo palo          0 2014-12-08 14:37 /tmp/file
            String[] fileInfos = line.split(" +");
            if (fileInfos.length == 8) {
                String filePath = fileInfos[fileInfos.length - 1];
                long fileSize = -1;
                try {
                    fileSize = Long.parseLong(fileInfos[4]);
                } catch (NumberFormatException e) {
                    LOG.warn("file size format error. line: {}", line);
                }

                fileMap.put(filePath, fileSize);
            }
        }
        return fileMap;
    }

    public void killEtlJob(String etlJobId) {
        String[] envp = { "LC_ALL=" + Config.locale };
        String hadoopKillCmd = String.format(HADOOP_KILL_CMD, HADOOP_CLIENT, hadoopConfig, etlJobId);
        LOG.info(hadoopKillCmd);
        Util.executeCommand(hadoopKillCmd, envp);
    }

    public void deleteEtlOutputPath(String outputPath) {
        String[] envp = { "LC_ALL=" + Config.locale };
        String hadoopRmCmd = String.format(HADOOP_RMR_CMD, HADOOP_CLIENT, hadoopConfig, outputPath);
        LOG.info(hadoopRmCmd);
        Util.executeCommand(hadoopRmCmd, envp);
    }

    public static String getEtlOutputPath(String fsDefaultName, String outputPath, long dbId, String loadLabel,
                                          String etlOutputDir) {
        return String.format(ETL_OUTPUT_PATH, fsDefaultName, outputPath, dbId, loadLabel, etlOutputDir);
    }

    private static class InputSizeInvalidException extends LoadException {
        public InputSizeInvalidException(String msg) {
            super(msg);
        }
    }

}
