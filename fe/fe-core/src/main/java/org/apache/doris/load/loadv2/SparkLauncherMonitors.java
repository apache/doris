package org.apache.doris.load.loadv2;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkLauncherMonitors {
    private static final Logger LOG = LogManager.getLogger(SparkLauncherMonitors.class);
    // 5min
    private static final long SUBMIT_APP_TIMEOUT_MS = 300 * 1000;

    private LogMonitor logMonitor;

    public static LogMonitor createLogMonitor(SparkLoadAppHandle handle) {
        return new LogMonitor(handle);
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

    public static class LogMonitor extends Thread {
        private final Process process;
        private SparkLoadAppHandle handle;
        private long submitTimeoutMs;
        private boolean isStop;

        private static final String STATE = "state";
        private static final String QUEUE = "queue";
        private static final String START_TIME = "start time";
        private static final String FINAL_STATUS = "final status";
        private static final String URL = "tracking URL";
        private static final String USER = "user";

        public LogMonitor(SparkLoadAppHandle handle) {
            this.handle = handle;
            this.process = handle.getProcess();
            this.isStop = false;
        }

        public void setSubmitTimeoutMs(long submitTimeoutMs) {
            this.submitTimeoutMs = submitTimeoutMs;
        }

        // Monitor the process's output
        @Override
        public void run() {
            BufferedReader outReader = null;
            String line = null;
            long startTime = System.currentTimeMillis();
            try {
                Preconditions.checkState(process.isAlive());
                outReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                while (!isStop && (line = outReader.readLine()) != null) {
                    LOG.info("Monitor Log: " + line);
                    // parse state and appId
                    if (line.contains(STATE)) {
                        SparkLoadAppHandle.State oldState = handle.getState();
                        SparkLoadAppHandle.State newState = oldState;
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

                        LOG.info("spark appId that handle get is {}, state: {}", handle.getAppId(), handle.getState().toString());
                        switch (newState) {
                            case UNKNOWN:
                            case CONNECTED:
                            case SUBMITTED:
                                // If the app stays in the UNKNOWN/CONNECTED/SUBMITTED state for more than submitTimeoutMs
                                // stop monitoring and kill the process
                                if (System.currentTimeMillis() - startTime > submitTimeoutMs) {
                                    isStop = true;
                                    handle.kill();
                                }
                                break;
                            case RUNNING:
                            case FINISHED:
                                // There's no need to parse all logs of handle process to get all the information.
                                // As soon as the state changes to RUNNING/KILLED/FAILED/FINISHED/LOST,
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
                    }
                    // parse other values
                    else if (line.contains(QUEUE) || line.contains(START_TIME) || line.contains(FINAL_STATUS) ||
                            line.contains(URL) || line.contains(USER)) {
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
                } catch (IOException e) {
                    LOG.warn("close buffered reader error", e);
                }
            }
        }

        private static String getValue(String line) {
            String result = null;
            List<String> entry = Splitter.onPattern(":").trimResults().splitToList(line);
            if (entry.size() == 2) {
                result = entry.get(1);
            }
            return result;
        }

        // Regex convert str such as "XXX (state: ACCEPTED)" to "ACCEPTED"
        private static String regexGetState(String line) {
            String result = null;
            Matcher stateMatcher = Pattern.compile("(?<=\\(state: )(.+?)(?=\\))").matcher(line);
            if (stateMatcher.find()) {
                result = stateMatcher.group();
            }
            return result;
        }

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
