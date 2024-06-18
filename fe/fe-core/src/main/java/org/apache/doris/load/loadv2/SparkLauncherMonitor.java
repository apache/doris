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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkLauncherMonitor {
    private static final Logger LOG = LogManager.getLogger(SparkLauncherMonitor.class);

    public static LogMonitor createLogMonitor(SparkLoadAppHandle handle, Map<String, String> resourceSparkConfig) {
        return new LogMonitor(handle, resourceSparkConfig);
    }

    private static SparkLoadAppHandle.State fromYarnState(YarnApplicationState yarnState) {
        switch (yarnState) {
            case SUBMITTED:
            case ACCEPTED:
                return SparkLoadAppHandle.State.SUBMITTED;
            case RUNNING:
                return SparkLoadAppHandle.State.RUNNING;
            case FINISHED:
                return SparkLoadAppHandle.State.FINISHED;
            case FAILED:
                return SparkLoadAppHandle.State.FAILED;
            case KILLED:
                return SparkLoadAppHandle.State.KILLED;
            default:
                // NEW NEW_SAVING
                return SparkLoadAppHandle.State.UNKNOWN;
        }
    }

    // This monitor is use for monitoring the spark launcher process.
    // User can use this monitor to get real-time `appId`, `state` and `tracking-url`
    // of spark launcher by reading and analyze the output of process.
    public static class LogMonitor extends Thread {
        private final Process process;
        private SparkLoadAppHandle handle;
        private long submitTimeoutMs;
        private boolean isStop;
        private OutputStream outputStream;

        private static final String STATE = "state";
        private static final String QUEUE = "queue";
        private static final String START_TIME = "start time";
        private static final String FINAL_STATUS = "final status";
        private static final String URL = "tracking URL";
        private static final String USER = "user";

        // 5min
        private static final long DEFAULT_SUBMIT_TIMEOUT_MS = 300000L;
        private static final String SUBMIT_TIMEOUT_KEY = "spark.submit.timeout";

        public LogMonitor(SparkLoadAppHandle handle, Map<String, String> resourceSparkConfig) {
            this.handle = handle;
            this.process = handle.getProcess();
            this.isStop = false;

            if (MapUtils.isNotEmpty(resourceSparkConfig)
                    && StringUtils.isNotEmpty(resourceSparkConfig.get(SUBMIT_TIMEOUT_KEY))) {
                setSubmitTimeoutMs(Long.parseLong(resourceSparkConfig.get(SUBMIT_TIMEOUT_KEY)));
            } else {
                setSubmitTimeoutMs(DEFAULT_SUBMIT_TIMEOUT_MS);
            }
        }

        public void setSubmitTimeoutMs(long submitTimeoutMs) {
            this.submitTimeoutMs = submitTimeoutMs;
        }

        public long getSubmitTimeoutMs() {
            return submitTimeoutMs;
        }

        public void setRedirectLogPath(String redirectLogPath) throws IOException {
            this.outputStream = new FileOutputStream(new File(redirectLogPath), false);
            this.handle.setLogPath(redirectLogPath);
        }

        // Normally, log monitor will automatically stop if the spark app state changes
        // to RUNNING.
        // But if the spark app state changes to FAILED/KILLED/LOST, log monitor will stop
        // and kill the spark launcher process.
        // There is a `submitTimeout` for preventing the spark app state from staying in
        // UNKNOWN/SUBMITTED for a long time.
        @Override
        public void run() {
            if (handle.getState() == SparkLoadAppHandle.State.KILLED) {
                // If handle has been killed, kill the process
                process.destroyForcibly();
                return;
            }
            BufferedReader outReader = null;
            String line = null;
            long startTime = System.currentTimeMillis();
            try {
                outReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                while (!isStop && (line = outReader.readLine()) != null) {
                    if (outputStream != null) {
                        outputStream.write((line + "\n").getBytes());
                    }
                    SparkLoadAppHandle.State oldState = handle.getState();
                    SparkLoadAppHandle.State newState = oldState;
                    // parse state and appId
                    if (line.contains(STATE)) {
                        // 1. state
                        String state = regexGetState(line);
                        if (state != null) {
                            YarnApplicationState yarnState = YarnApplicationState.valueOf(state);
                            newState = fromYarnState(yarnState);
                            if (newState != oldState) {
                                handle.setState(newState);
                            }
                        }
                        // 2. appId
                        String appId = regexGetAppId(line);
                        if (appId != null) {
                            if (!appId.equals(handle.getAppId())) {
                                handle.setAppId(appId);
                            }
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("spark appId that handle get is {}, state: {}",
                                    handle.getAppId(), handle.getState().toString());
                        }
                        switch (newState) {
                            case UNKNOWN:
                            case CONNECTED:
                            case SUBMITTED:
                                // If the app stays in the UNKNOWN/CONNECTED/SUBMITTED state
                                // for more than submitTimeoutMs stop monitoring and kill the process
                                if (System.currentTimeMillis() - startTime > submitTimeoutMs) {
                                    isStop = true;
                                    handle.kill();
                                }
                                break;
                            case RUNNING:
                            case FINISHED:
                                // There's no need to parse all logs of handle process to get all the information.
                                // As soon as the state changes to RUNNING/FINISHED,
                                // stop monitoring but keep the process alive.
                                isStop = true;
                                break;
                            case KILLED:
                            case FAILED:
                            case LOST:
                                // If the state changes to KILLED/FAILED/LOST,
                                // stop monitoring and kill the process
                                isStop = true;
                                handle.kill();
                                break;
                            default:
                                Preconditions.checkState(false, "wrong spark app state");
                        }
                    } else if (line.contains(QUEUE) || line.contains(START_TIME) || line.contains(FINAL_STATUS)
                            || line.contains(URL) || line.contains(USER)) { // parse other values
                        String value = getValue(line);
                        if (!Strings.isNullOrEmpty(value)) {
                            try {
                                if (line.contains(QUEUE)) {
                                    handle.setQueue(value);
                                } else if (line.contains(START_TIME)) {
                                    handle.setStartTime(Long.parseLong(value));
                                } else if (line.contains(FINAL_STATUS)) {
                                    handle.setFinalStatus(FinalApplicationStatus.valueOf(value));
                                } else if (line.contains(URL)) {
                                    handle.setUrl(value);
                                } else if (line.contains(USER)) {
                                    handle.setUser(value);
                                }
                            } catch (IllegalArgumentException e) {
                                LOG.warn("parse log encounter an error, line: {}, msg: {}", line, e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception monitoring process.", e);
            } finally {
                try {
                    if (outReader != null) {
                        outReader.close();
                    }
                    if (outputStream != null) {
                        outputStream.close();
                    }
                } catch (IOException e) {
                    LOG.warn("close buffered reader error", e);
                }
            }
        }

        // e.g.
        // input: "final status: SUCCEEDED"
        // output: "SUCCEEDED"
        private static String getValue(String line) {
            String result = null;
            List<String> entry = Splitter.onPattern(":").trimResults().limit(2).splitToList(line);
            if (entry.size() == 2) {
                result = entry.get(1);
            }
            return result;
        }

        // e.g.
        // input: "Application report for application_1573630236805_6864759 (state: ACCEPTED)"
        // output: "ACCEPTED"
        private static String regexGetState(String line) {
            String result = null;
            Matcher stateMatcher = Pattern.compile("(?<=\\(state: )(.+?)(?=\\))").matcher(line);
            if (stateMatcher.find()) {
                result = stateMatcher.group();
            }
            return result;
        }

        // e.g.
        // input: "Application report for application_1573630236805_6864759 (state: ACCEPTED)"
        // output: "application_1573630236805_6864759"
        private static String regexGetAppId(String line) {
            String result = null;
            Matcher appIdMatcher = Pattern.compile("application_[0-9]+_[0-9]+").matcher(line);
            if (appIdMatcher.find()) {
                result = appIdMatcher.group();
            }
            return result;
        }
    }
}
