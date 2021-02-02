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

package org.apache.doris.task;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.EtlSubmitResult;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadChecker;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Joiner;

import java.util.List;
import java.util.UUID;

public abstract class LoadPendingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(LoadPendingTask.class);
    private static final int RETRY_NUM = 5;

    protected final LoadJob job;
    protected final Load load;
    protected Database db;

    public LoadPendingTask(LoadJob job) {
        this.job = job;
        this.signature = job.getId();
        this.load = Catalog.getCurrentCatalog().getLoadInstance();
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
        db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            load.cancelLoadJob(job, CancelType.ETL_SUBMIT_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        try {
            // yiguolei: get transactionid here, because create etl request will get schema and partition info
            // create etl request and make some guarantee for schema change and rollup
            if (job.getTransactionId() < 0) {
                long transactionId = Catalog.getCurrentGlobalTransactionMgr()
                        .beginTransaction(dbId, job.getAllTableIds(), DebugUtil.printId(UUID.randomUUID()),
                                          new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                                          LoadJobSourceType.FRONTEND,
                                          job.getTimeoutSecond());
                job.setTransactionId(transactionId);
            }
            createEtlRequest();
        } catch (Exception e) {
            LOG.info("create etl request failed.", e);
            load.cancelLoadJob(job, CancelType.ETL_SUBMIT_FAIL, "create job request fail. " + e.getMessage());
            return;
        }

        // submit etl job and retry 5 times if error
        EtlSubmitResult result = null;
        int retry = 0;
        while (retry < RETRY_NUM) {
            result = submitEtlJob(retry);
            if (result != null) {
                if (result.getStatus().getStatusCode() == TStatusCode.OK) {
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
            List<String> failMsgs = result.getStatus().getErrorMsgs();
            failMsg = Joiner.on(";").join(failMsgs);
        }

        load.cancelLoadJob(job, CancelType.ETL_SUBMIT_FAIL, failMsg);
        LOG.warn("submit etl job fail. job: {}", job);
    }
    
    protected abstract void createEtlRequest() throws Exception;

    protected abstract EtlSubmitResult submitEtlJob(int retry);
}
