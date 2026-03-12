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

package org.apache.doris.tso;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.journal.local.LocalJournal;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantLock;


public class TSOService extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TSOService.class);

    // Global timestamp with physical time and logical counter
    private final TSOTimestamp globalTimestamp = new TSOTimestamp();
    // Lock for thread-safe access to global timestamp
    private final ReentrantLock lock = new ReentrantLock();
    // Guard value for time window updates (in milliseconds)
    private static final long UPDATE_TIME_WINDOW_GUARD = 1;

    /**
     * Constructor initializes the TSO service with update interval
     */
    public TSOService() {
        super("TSO-service", Config.tso_service_update_interval_ms);
    }

    /**
     * Start the TSO service and calibrate timestamp if needed
     */
    @Override
    public synchronized void start() {
        super.start();

        for (int i = 0; i < Config.max_update_tso_retry_count; i++) {
            lock.lock();
            boolean isInitialized = false;
            try {
                isInitialized = (globalTimestamp.getPhysicalTimestamp() != 0);
            } finally {
                lock.unlock();
            }

            if (!isInitialized) {
                LOG.info("TSO service timestamp is not calibrated, start calibrate timestamp");
                try {
                    calibrateTimestamp();
                } catch (Exception e) {
                    LOG.warn("TSO service calibrate timestamp failed", e);
                }
            }
        }
    }

    /**
     * Periodically update timestamp after catalog is ready
     * This method is called by the MasterDaemon framework
     */
    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_feature_tso) {
            return;
        }
        boolean updated = false;
        Throwable lastFailure = null;
        for (int i = 0; i < Config.max_update_tso_retry_count; i++) {
            lock.lock();
            boolean isInitialized = false;
            try {
                isInitialized = (globalTimestamp.getPhysicalTimestamp() != 0);
            } finally {
                lock.unlock();
            }

            if (!isInitialized) {
                LOG.info("TSO service timestamp is not calibrated, start calibrate timestamp");
                try {
                    calibrateTimestamp();
                    return;
                } catch (Exception e) {
                    lastFailure = e;
                    LOG.warn("TSO service calibrate timestamp failed", e);
                }
            }

            try {
                updateTimestamp();
                updated = true;
                break; // Update successful, exit the loop
            } catch (Exception e) {
                lastFailure = e;
                LOG.warn("TSO service update timestamp failed, retry: {}", i, e);
                MetricRepo.COUNTER_TSO_CLOCK_UPDATE_FAILED.increase(1L);
                try {
                    sleep(Config.tso_service_update_interval_ms);
                } catch (InterruptedException ie) {
                    LOG.warn("TSO service sleep interrupted", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (updated) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("TSO service updated timestamp");
            }
        } else if (lastFailure != null) {
            LOG.warn("TSO service update timestamp failed after {} retries",
                    Config.max_update_tso_retry_count, lastFailure);
        } else {
            LOG.warn("TSO service update timestamp failed after {} retries", Config.max_update_tso_retry_count);
        }
    }

    /**
     * Generate a single TSO timestamp
     *
     * @return Composed TSO timestamp combining physical time and logical counter
     * @throws RuntimeException if TSO is not calibrated or other errors occur
     */
    public Long getTSO() {
        int maxGetTSORetryCount = Math.max(1, Config.max_get_tso_retry_count);
        for (int i = 0; i < maxGetTSORetryCount; i++) {
            // Wait for environment to be ready and ensure we're running on master FE
            Env env = Env.getCurrentEnv();
            if (env == null || !env.isReady()) {
                LOG.warn("TSO service wait for catalog ready");
                try {
                    sleep(200);
                } catch (InterruptedException ie) {
                    LOG.warn("TSO service sleep interrupted", ie);
                    Thread.currentThread().interrupt();
                }
                continue;
            } else if (!env.isMaster()) {
                LOG.warn("TSO service only run on master FE");
                try {
                    sleep(200);
                } catch (InterruptedException ie) {
                    LOG.warn("TSO service sleep interrupted", ie);
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            Pair<Long, Long> pair = generateTSO();
            long physical = pair.first;
            long logical = pair.second;

            if (physical == 0) {
                throw new RuntimeException("TSO timestamp is not calibrated, please check");
            }

            // Check for logical counter overflow
            if (logical >= TSOTimestamp.MAX_LOGICAL_COUNTER) {
                LOG.warn("TSO timestamp logical counter overflow, please check");
                try {
                    sleep(Config.tso_service_update_interval_ms);
                } catch (InterruptedException ie) {
                    LOG.warn("TSO service sleep interrupted", ie);
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TSO_CLOCK_GET_SUCCESS.increase(1L);
            }
            return TSOTimestamp.composeTimestamp(physical, logical);
        }
        return -1L;
    }

    /**
     * Get the current composed TSO timestamp
     *
     * @return Current TSO timestamp combining physical time and logical counter
     */
    public long getCurrentTSO() {
        lock.lock();
        try {
            return globalTimestamp.composeTimestamp();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Calibrate the TSO timestamp when service starts
     * This ensures the timestamp is consistent with the last persisted value
     *
     * Algorithm:
     * - If Tnow - Tlast < 1ms, then Tnext = Tlast + 1
     * - Otherwise Tnext = Tnow
     */
    private void calibrateTimestamp() {
        // Check if Env is ready before calibration
        Env env = Env.getCurrentEnv();
        if (env == null || !env.isReady() || !env.isMaster()) {
            LOG.warn("Env is not ready or not master, skip TSO timestamp calibration");
            return;
        }

        long timeLast = env.getWindowEndTSO(); // Last timestamp from EditLog replay
        long timeNow = System.currentTimeMillis() + Config.tso_time_offset_debug_mode;

        // Calculate next physical time to ensure monotonicity
        long nextPhysicalTime;
        if (timeNow - timeLast < 1) {
            nextPhysicalTime = timeLast + 1;
        } else {
            nextPhysicalTime = timeNow;
        }

        // Construct new timestamp (physical time with reset logical counter)
        setTSOPhysical(nextPhysicalTime, true);

        // Write the right boundary of time window to BDBJE for persistence
        long timeWindowEnd = nextPhysicalTime + Config.tso_service_window_duration_ms;
        env.setWindowEndTSO(timeWindowEnd);
        writeTimestampToBDBJE(timeWindowEnd);

        LOG.info("TSO timestamp calibrated: lastTimestamp={}, currentMillis={}, nextPhysicalTime={}, timeWindowEnd={}",
                timeLast, timeNow, nextPhysicalTime, timeWindowEnd);
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TSO_CLOCK_CALCULATED.increase(1L);
        }
    }

    /**
     * Update timestamp periodically to maintain time window
     * This method handles various time-related issues:
     * 1. Clock drift detection
     * 2. Clock backward detection
     * 3. Logical counter overflow handling
     * 4. Time window renewal
     */
    private void updateTimestamp() {
        // Check if Env is ready
        Env env = Env.getCurrentEnv();
        if (env == null || !env.isReady() || !env.isMaster()) {
            LOG.warn("Env is not ready or not master, skip TSO timestamp update");
            return;
        }

        // 1. Check if TSO has been calibrated
        long currentTime = System.currentTimeMillis() + Config.tso_time_offset_debug_mode;
        long prevPhysicalTime = 0;
        long prevLogicalCounter = 0;

        lock.lock();
        try {
            prevPhysicalTime = globalTimestamp.getPhysicalTimestamp();
            prevLogicalCounter = globalTimestamp.getLogicalCounter();
        } finally {
            lock.unlock();
        }

        if (prevPhysicalTime == 0) {
            LOG.error("TSO timestamp is not calibrated, please check");
        }

        // 2. Check for serious clock issues
        long timeLag = currentTime - prevPhysicalTime;
        if (timeLag >= 3 * Config.tso_service_update_interval_ms) {
            // Clock drift (time difference too large), log clearly and trigger corresponding metric
            LOG.warn("TSO clock drift detected, lastPhysicalTime={}, currentTime={}, "
                            + "timeLag={} (exceeds 3 * update interval {})",
                    prevPhysicalTime, currentTime, timeLag, 3 * Config.tso_service_update_interval_ms);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TSO_CLOCK_DRIFT_DETECTED.increase(1L);
            }
        } else if (timeLag < 0) {
            // Clock backward (current time earlier than last recorded time)
            // log clearly and trigger corresponding metric
            LOG.warn("TSO clock backward detected, lastPhysicalTime={}, currentTime={}, "
                            + "timeLag={} (current time is earlier than last physical time)",
                    prevPhysicalTime, currentTime, timeLag);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TSO_CLOCK_BACKWARD_DETECTED.increase(1L);
            }
        }

        // 3. Update time based on conditions
        long nextPhysicalTime = prevPhysicalTime;
        if (timeLag > UPDATE_TIME_WINDOW_GUARD) {
            // Align physical time to current time
            nextPhysicalTime = currentTime;
        } else if (prevLogicalCounter > TSOTimestamp.MAX_LOGICAL_COUNTER / 2) {
            // Logical counter nearly full → advance to next millisecond
            nextPhysicalTime = prevPhysicalTime + 1;
        } else {
            // Logical counter not nearly full → just increment logical counter
            // do nothing
        }

        // 4. Check if time window right boundary needs renewal
        if ((env.getWindowEndTSO() - nextPhysicalTime) <= UPDATE_TIME_WINDOW_GUARD) {
            // Time window right boundary needs renewal
            long nextWindowEnd = nextPhysicalTime + Config.tso_service_window_duration_ms;
            env.setWindowEndTSO(nextWindowEnd);
            writeTimestampToBDBJE(nextWindowEnd);
        }

        // 5. Update global timestamp
        setTSOPhysical(nextPhysicalTime, false);
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TSO_CLOCK_UPDATED.increase(1L);
        }
    }

    /**
     * Write the right boundary of TSO time window to BDBJE for persistence
     *
     * @param timestamp The timestamp to write
     */
    private void writeTimestampToBDBJE(long timestamp) {
        try {
            // Check if Env is ready
            Env env = Env.getCurrentEnv();
            if (env == null) {
                LOG.warn("Env is null, skip writing TSO timestamp to BDBJE");
                return;
            }

            // Check if Env is ready and is master
            if (!env.isReady()) {
                LOG.warn("Env is not ready, skip writing TSO timestamp to BDBJE");
                return;
            }

            if (!env.isMaster()) {
                LOG.warn("Current node is not master, skip writing TSO timestamp to BDBJE");
                return;
            }

            TSOTimestamp tsoTimestamp = new TSOTimestamp(timestamp, 0);

            // Check if EditLog is available
            EditLog editLog = env.getEditLog();
            if (editLog == null) {
                LOG.warn("EditLog is null, skip writing TSO timestamp to BDBJE");
                return;
            }

            // Additional check to ensure EditLog's journal is properly initialized
            if (editLog.getJournal() == null) {
                LOG.warn("EditLog's journal is null, skip writing TSO timestamp to BDBJE");
                return;
            }

            if (editLog.getJournal() instanceof LocalJournal) {
                if (!((LocalJournal) editLog.getJournal()).isReadyToFlush()) {
                    LOG.warn("EditLog's journal is not ready to flush, skip writing TSO timestamp to BDBJE");
                    return;
                }
            }

            if (Config.enable_tso_persist_journal) {
                editLog.logTSOTimestampWindowEnd(tsoTimestamp);
            } else {
                LOG.debug("TSO timestamp {} is not persisted to journal, "
                        + "please check if enable_tso_persist_journal is set to true",
                        tsoTimestamp);
            }
        } catch (Exception e) {
            LOG.error("Failed to write TSO timestamp to BDBJE", e);
        }
    }

    /**
     * Generate a single TSO timestamp by incrementing the logical counter
     *
     * @return Pair of (physicalTime, updatedLogicalCounter) for the base timestamp
     */
    private Pair<Long, Long> generateTSO() {
        lock.lock();
        try {
            long physicalTime = globalTimestamp.getPhysicalTimestamp();
            if (physicalTime == 0) {
                return Pair.of(0L, 0L);
            }
            long logicalCounter = globalTimestamp.getLogicalCounter();
            globalTimestamp.setLogicalCounter(logicalCounter + 1);
            logicalCounter = globalTimestamp.getLogicalCounter();
            return Pair.of(physicalTime, logicalCounter);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set the physical time component of the global timestamp
     *
     * @param next New physical time value
     * @param force Whether to force update even if physical time is zero
     */
    private void setTSOPhysical(long next, boolean force) {
        lock.lock();
        try {
            // Do not update the zero physical time if the `force` flag is false.
            if (!force && globalTimestamp.getPhysicalTimestamp() == 0) {
                return;
            }
            if (next - globalTimestamp.getPhysicalTimestamp() > 0) {
                globalTimestamp.setPhysicalTimestamp(next);
                globalTimestamp.setLogicalCounter(0L);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replay handler for TSO window end timestamp from edit log.
     * This method updates the Env's window end TSO value.
     * It is safe to call during checkpoint replay when TSOService may not be initialized.
     *
     * @param windowEnd New window end physical time
     */
    public void replayWindowEndTSO(TSOTimestamp windowEnd) {
        Env env = Env.getCurrentEnv();
        if (env == null) {
            return;
        }
        env.setWindowEndTSO(windowEnd.getPhysicalTimestamp());
    }
}
