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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.FailMsg;
import org.apache.doris.transaction.BeginTransactionException;

import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * LoadScheduler will schedule the pending LoadJob which belongs to LoadManager.
 * The function of execute will be called in LoadScheduler.
 * The status of LoadJob will be changed to loading after LoadScheduler.
 */
public class LoadJobScheduler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(LoadJobScheduler.class);

    private LinkedBlockingQueue<LoadJob> needScheduleJobs = Queues.newLinkedBlockingQueue();

    // Used to implement a job granularity lock for update global dict table serially
    // The runningBitmapTables keeps the doris table id of the spark load job which in state pending or etl
    private Map<Long, Set<Long>> runningBitmapTableMap = new HashMap<>();

    public LoadJobScheduler() {
        super("Load job scheduler", Config.load_checker_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of LoadJobScheduler with error message {}", e.getMessage(), e);
        }
    }

    private void process() throws InterruptedException {
        while (true) {
            // take one load job from queue
            LoadJob loadJob = needScheduleJobs.poll();
            if (loadJob == null) {
                return;
            }

            // schedule job
            try {
                if (!isAllowBitmapTableLoad(loadJob)) {
                    continue;
                }
                loadJob.execute();
            } catch (LabelAlreadyUsedException | AnalysisException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                                 .add("error_msg", "There are error properties in job. Job will be cancelled")
                                 .build(), e);
                // transaction not begin, so need not abort
                loadJob.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL, e.getMessage()),
                        false, true);
            } catch (LoadException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                                 .add("error_msg", "Failed to submit etl job. Job will be cancelled")
                                 .build(), e);
                // transaction already begin, so need abort
                loadJob.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL, e.getMessage()),
                                              true, true);
            } catch (DuplicatedRequestException e) {
                // should not happen in load job scheduler, there is no request id.
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                        .add("error_msg", "Failed to begin txn with duplicate request. Job will be rescheduled later")
                        .build(), e);
                needScheduleJobs.put(loadJob);
                return;
            } catch (BeginTransactionException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                                 .add("error_msg", "Failed to begin txn when job is scheduling. "
                                         + "Job will be rescheduled later")
                                 .build(), e);
                needScheduleJobs.put(loadJob);
                return;
            }
        }
    }

    private boolean isAllowBitmapTableLoad(LoadJob loadJob) throws InterruptedException {
        // only deal spark load job
        if (!(loadJob instanceof SparkLoadJob)) {
            return true;
        }

        SparkLoadJob sparkLoadJob = (SparkLoadJob)loadJob;
        // get load table which contains bitmap column
        Set<Long> tableIdWithBitmapColumn = sparkLoadJob.getTableWithBitmapColumn();

        if (!addRunningTable(sparkLoadJob.getId(), tableIdWithBitmapColumn)) {
            // the job needs to wait
            needScheduleJobs.add(loadJob);
            return false;
        }

        return true;
    }

    // add lock cases as below:
    // 1 LoadJobScheduler.submit success
    // 2 SparkLoadJob in etl then replay
    public boolean addRunningTable(Long jobId, Set<Long> tableIds) {
        // skip spark load job which needn't build global dict
        if (tableIds.size() == 0) {
            return true;
        }

        synchronized (runningBitmapTableMap) {
            Set<Long> runningTableIdSet = new HashSet<>();
            for (Set<Long> set : runningBitmapTableMap.values()) {
                runningTableIdSet.addAll(set);
            }

            for (Long tableId : tableIds) {
                // there is already a running job for the same table, so it needs to wait
                if (runningTableIdSet.contains(tableId)) {
                    return false;
                }
            }
            if (runningBitmapTableMap.containsKey(jobId)) {
                LOG.warn("unexpected case: runningBitmapTableMap contains job id {}, should add only once", jobId);
            }
            runningBitmapTableMap.put(jobId, tableIds);
            return true;
        }
    }

    // release lock cases as below
    // 1 SparkLoadJob in pending status then failed
    // 2 SparkLoadJob in etl then status failed
    // 3 SparkLoadJob in etl then status finish
    // 4 LoadJobScheduler.submit failed
    // 5 SparkLoadJob's state convert to Loading
    // 6 transaction abort
    public void removeRunningTable(Long jobId, Set<Long> tableIds) {
        if (tableIds.size() == 0) {
            return;
        }
        synchronized (runningBitmapTableMap) {
            runningBitmapTableMap.remove(jobId);
        }
    }

    public boolean isQueueFull() {
        return needScheduleJobs.size() > Config.desired_max_waiting_jobs;
    }

    public void submitJob(LoadJob job) {
        needScheduleJobs.add(job);
    }

    public void submitJob(List<LoadJob> jobs) {
        needScheduleJobs.addAll(jobs);
    }
}
