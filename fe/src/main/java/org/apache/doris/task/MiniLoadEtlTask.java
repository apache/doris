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
import org.apache.doris.load.MiniEtlTaskInfo;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.LoadJob;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TMiniLoadEtlStatusResult;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Map;

public class MiniLoadEtlTask extends LoadEtlTask {
    private static final Logger LOG = LogManager.getLogger(MiniLoadEtlTask.class);

    public MiniLoadEtlTask(LoadJob job) {
        super(job);
    }

    @Override
    protected boolean updateJobEtlStatus() {
        // update etl tasks status
        if (job.miniNeedGetTaskStatus()) {
            LOG.debug("get mini etl task status actively. job: {}", job);
            for (MiniEtlTaskInfo taskInfo : job.getMiniEtlTasks().values()) {
                TEtlState etlState = taskInfo.getTaskStatus().getState();
                if (etlState == TEtlState.RUNNING) {
                    updateEtlTaskStatus(taskInfo);
                }
            }
        }

        // update etl job status
        updateEtlJobStatus();
        return true;
    }
     
    private boolean updateEtlTaskStatus(MiniEtlTaskInfo taskInfo) {
        // get etl status
        TMiniLoadEtlStatusResult result = getMiniLoadEtlStatus(taskInfo.getBackendId(), taskInfo.getId());
        LOG.info("mini load etl status: {}, job: {}", result, job);
        if (result == null) {
            return false;
        }
        TStatus tStatus = result.getStatus();
        if (tStatus.getStatus_code() != TStatusCode.OK) {
            LOG.warn("get buck load etl status fail. msg: {}, job: {}", tStatus.getError_msgs(), job);
            return false;
        }
        
        // update etl task status
        EtlStatus taskStatus = taskInfo.getTaskStatus();
        if (taskStatus.setState(result.getEtl_state())) {
            if (result.isSetCounters()) {
                taskStatus.setCounters(result.getCounters());
            }
            if (result.isSetTracking_url()) {
                taskStatus.setTrackingUrl(result.getTracking_url());
            }
            if (result.isSetFile_map()) {
                taskStatus.setFileMap(result.getFile_map());
            }       
        }

        return true;
    }

    private TMiniLoadEtlStatusResult getMiniLoadEtlStatus(long backendId, long taskId) {
        TMiniLoadEtlStatusResult result = null; 
        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null || !backend.isAlive()) {
            String failMsg = "backend is null or is not alive";
            LOG.error(failMsg);
            return result;
        }

        AgentClient client = new AgentClient(backend.getHost(), backend.getBePort());
        return client.getEtlStatus(job.getId(), taskId);
    }   

    private boolean updateEtlJobStatus() {
        boolean hasCancelledTask = false;
        boolean hasRunningTask = false;
        long normalNum = 0;
        long abnormalNum = 0;
        Map<String, Long> fileMap = Maps.newHashMap();
        String trackingUrl = EtlStatus.DEFAULT_TRACKING_URL;

        EtlStatus etlJobStatus = job.getEtlJobStatus();
        for (MiniEtlTaskInfo taskInfo : job.getMiniEtlTasks().values()) {
            EtlStatus taskStatus = taskInfo.getTaskStatus();
            switch (taskStatus.getState()) {
                case RUNNING:
                    hasRunningTask = true;
                    break;
                case CANCELLED:
                    hasCancelledTask = true;
                    break;
                case FINISHED:
                    // counters and file list
                    Map<String, String> counters = taskStatus.getCounters();
                    if (counters.containsKey(DPP_NORMAL_ALL)) {
                        normalNum += Long.parseLong(counters.get(DPP_NORMAL_ALL));
                    }
                    if (counters.containsKey(DPP_ABNORMAL_ALL)) {
                        abnormalNum += Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
                    }
                    fileMap.putAll(taskStatus.getFileMap());
                    if (!taskStatus.getTrackingUrl().equals(EtlStatus.DEFAULT_TRACKING_URL)) {
                        trackingUrl = taskStatus.getTrackingUrl();
                    }
                    break;
                default:
                    break;
            }
        }

        if (hasCancelledTask) {
            etlJobStatus.setState(TEtlState.CANCELLED);
        } else if (hasRunningTask) {
            etlJobStatus.setState(TEtlState.RUNNING);
        } else {
            etlJobStatus.setState(TEtlState.FINISHED);
            Map<String, String> counters = Maps.newHashMap();
            counters.put(DPP_NORMAL_ALL, String.valueOf(normalNum));
            counters.put(DPP_ABNORMAL_ALL, String.valueOf(abnormalNum));
            etlJobStatus.setCounters(counters);
            etlJobStatus.setFileMap(fileMap);
            etlJobStatus.setTrackingUrl(trackingUrl);
        }
        
        return true;
    }
    
    @Override
    protected void processEtlRunning() throws LoadException {
        // update mini etl job progress
        int finishedTaskNum = 0;
        Map<Long, MiniEtlTaskInfo> idToEtlTask = job.getMiniEtlTasks();
        for (MiniEtlTaskInfo taskInfo : idToEtlTask.values()) {
            EtlStatus taskStatus = taskInfo.getTaskStatus();
            if (taskStatus.getState() == TEtlState.FINISHED) {
                ++finishedTaskNum;
            }
        }

        int progress = (int) (finishedTaskNum * 100 / idToEtlTask.size());
        if (progress >= 100) {
            // set progress to 100 when status is FINISHED
            progress = 99;
        }

        job.setProgress(progress);
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
