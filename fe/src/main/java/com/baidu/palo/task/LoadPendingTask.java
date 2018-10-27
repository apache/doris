// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.load.EtlSubmitResult;
import com.baidu.palo.load.FailMsg.CancelType;
import com.baidu.palo.load.Load;
import com.baidu.palo.load.LoadChecker;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.thrift.TStatusCode;

import com.google.common.base.Joiner;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;

public abstract class LoadPendingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(LoadPendingTask.class);
    private static final int RETRY_NUM = 5;

    protected final LoadJob job;
    protected final Load load;
    protected Database db;

    public LoadPendingTask(LoadJob job) {
        this.job = job;
        this.signature = job.getId();
        this.load = Catalog.getInstance().getLoadInstance();
    }

    @Override
    protected void exec() {
        // check job state
        if (job.getState() != JobState.PENDING) {
            return;
        }

        // check timeout
        if (LoadChecker.checkTimeout(job)) {
            load.cancelLoadJob(job, CancelType.TIMEOUT, "pending timeout to cancel");
            return;
        }
        
        // get db
        long dbId = job.getDbId();
        db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            load.cancelLoadJob(job, CancelType.ETL_SUBMIT_FAIL, "db does not exist. id: " + dbId);
            return;
        }
        
        // create etl request
        try {
            createEtlRequest();
        } catch (Exception e) {
            LOG.info("create etl request failed.{}", e);
            load.cancelLoadJob(job, CancelType.ETL_SUBMIT_FAIL, "create job request fail. " + e.getMessage());
            return;
        }

        // submit etl job and retry 5 times if error
        EtlSubmitResult result = null;
        int retry = 0;
        while (retry < RETRY_NUM) {
            result = submitEtlJob(retry);
            if (result != null) {
                if (result.getStatus().getStatus_code() == TStatusCode.OK) {
                    if (load.updateLoadJobState(job, JobState.ETL)) {
                        LOG.info("submit etl job success. job: {}", job);
                        return;
                    }
                }
            }
            ++retry;
        }

        String failMsg = "submit etl job fail";
        if (result != null) {
            List<String> failMsgs = result.getStatus().getError_msgs();
            failMsg = Joiner.on(";").join(failMsgs);
        }

        load.cancelLoadJob(job, CancelType.ETL_SUBMIT_FAIL, failMsg);
        LOG.warn("submit etl job fail. job: {}", job);
    }
    
    protected abstract void createEtlRequest() throws Exception;

    protected abstract EtlSubmitResult submitEtlJob(int retry);
}
