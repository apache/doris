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
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.LoadJob;
import org.apache.doris.thrift.TEtlState;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// Used to process pull load etl task
@Deprecated
public class PullLoadEtlTask extends LoadEtlTask {
    private static final Logger LOG = LogManager.getLogger(PullLoadEtlTask.class);
    private PullLoadJobMgr mgr;

    public PullLoadEtlTask(LoadJob job) {
        super(job);
        mgr = Catalog.getCurrentCatalog().getPullLoadJobMgr();
    }

    @Override
    protected String getErrorMsg() {
        String errMsg = null;
        PullLoadJob pullLoadJob = mgr.getJob(job.getId());
        if (pullLoadJob != null) {
            PullLoadTask failureTask = pullLoadJob.getFailureTask();
            if (failureTask != null) {
                if (failureTask.getExecuteStatus() != null) {
                    errMsg = "Broker etl failed: " + failureTask.getExecuteStatus().getErrorMsg();
                }
            }
        }
        return errMsg != null ? errMsg : super.getErrorMsg();
    }

    @Override
    protected boolean updateJobEtlStatus() {
        PullLoadJob pullLoadJob = mgr.getJob(job.getId());
        EtlStatus etlStatus = job.getEtlJobStatus();
        if (pullLoadJob == null) {
            LOG.warn("pullLoadJob is null. JobId is {}", job.getId());
            return false;
        }
        switch (pullLoadJob.getState()) {
            case CANCELED:
            case FAILED:
                etlStatus.setState(TEtlState.CANCELLED);
                break;
            case FINISHED:
                updateFinishInfo(pullLoadJob);
                etlStatus.setState(TEtlState.FINISHED);
                break;
            case RUNNING:
                etlStatus.setState(TEtlState.RUNNING);
                break;
            default:
                etlStatus.setState(TEtlState.UNKNOWN);
                break;
        }
        return true;
    }

    private void updateFinishInfo(PullLoadJob pullLoadJob) {
        Map<String, Long> fileMap = Maps.newHashMap();
        long numRowsNormal = 0;
        long numRowsAbnormal = 0;
        String trackingUrl = null;
        for (PullLoadTask task : pullLoadJob.tasks) {
            fileMap.putAll(task.getFileMap());

            String value = task.getCounters().get(DPP_NORMAL_ALL);
            if (value != null) {
                numRowsNormal += Long.valueOf(value);
            }
            value = task.getCounters().get(DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal += Long.valueOf(value);
            }
            if (trackingUrl == null && task.getTrackingUrl() != null) {
                trackingUrl = task.getTrackingUrl();
            }
        }
        Map<String, String> counters = Maps.newHashMap();
        counters.put(DPP_NORMAL_ALL, "" + numRowsNormal);
        counters.put(DPP_ABNORMAL_ALL, "" + numRowsAbnormal);

        EtlStatus etlJobStatus = job.getEtlJobStatus();
        etlJobStatus.setFileMap(fileMap);
        etlJobStatus.setCounters(counters);
        if (trackingUrl != null) {
            etlJobStatus.setTrackingUrl(trackingUrl);
        }
    }

    @Override
    protected void processEtlRunning() throws LoadException {
    }

    @Override
    protected Map<String, Pair<String, Long>> getFilePathMap() throws LoadException {
        Map<String, Long> fileMap = job.getEtlJobStatus().getFileMap();
        if (fileMap == null) {
            throw new LoadException("get etl files error");
        }

        Map<String, Pair<String, Long>> filePathMap = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : fileMap.entrySet()) {
            String partitionIndexBucket = getPartitionIndexBucketString(entry.getKey());
            // http://host:8000/data/dir/file
            filePathMap.put(partitionIndexBucket, Pair.create(entry.getKey(), entry.getValue()));
        }

        return filePathMap;
    }
}
