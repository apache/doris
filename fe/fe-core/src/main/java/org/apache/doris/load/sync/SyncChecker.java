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

package org.apache.doris.load.sync;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.sync.SyncJob.JobState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class SyncChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(SyncChecker.class);

    private final SyncJobManager syncJobManager;

    public SyncChecker(SyncJobManager syncJobManager) {
        super("sync checker", Config.sync_checker_interval_second * 1000L);
        this.syncJobManager = syncJobManager;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("start check sync jobs.");
        }
        try {
            process();
            cleanOldSyncJobs();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of SyncChecker", e);
        }
    }

    private void process() throws UserException {
        // update jobs need schedule
        this.syncJobManager.updateNeedSchedule();
        // get jobs need schedule
        List<SyncJob> needScheduleJobs = this.syncJobManager.getSyncJobs(JobState.PENDING);
        for (SyncJob job : needScheduleJobs) {
            SyncFailMsg.MsgType msgType = null;
            UserException exception = null;
            try {
                job.execute();
            } catch (MetaNotFoundException | DdlException e) {
                msgType = SyncFailMsg.MsgType.SCHEDULE_FAIL;
                exception = e;
                LOG.warn(e.getMessage());
            } catch (UserException e) {
                msgType = SyncFailMsg.MsgType.UNKNOWN;
                exception = e;
                LOG.warn(e.getMessage());
            }
            // cancel job
            if (exception != null) {
                job.cancel(msgType, exception.getMessage());
            }
        }
    }

    private void cleanOldSyncJobs() {
        // clean up expired sync jobs
        this.syncJobManager.cleanOldSyncJobs();
        this.syncJobManager.cleanOverLimitSyncJobs();
    }
}
