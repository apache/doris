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

package org.apache.doris.cdcclient.controller;

import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.model.rest.RestResponse;
import org.apache.doris.cdcclient.service.PipelineCoordinator;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.WriteRecordRequest;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientController {
    private static final Logger LOG = LoggerFactory.getLogger(ClientController.class);

    @Autowired private PipelineCoordinator pipelineCoordinator;

    /** init source reader */
    @RequestMapping(path = "/api/initReader", method = RequestMethod.POST)
    public Object initSourceReader(@RequestBody JobBaseConfig jobConfig) {
        try {
            SourceReader reader = Env.getCurrentEnv().getReader(jobConfig);
            return RestResponse.success("Source reader initialized successfully");
        } catch (Exception ex) {
            LOG.error("Failed to create reader, jobId={}", jobConfig.getJobId(), ex);
            return RestResponse.internalError(ExceptionUtils.getRootCauseMessage(ex));
        }
    }

    /** Fetch source splits for snapshot */
    @RequestMapping(path = "/api/fetchSplits", method = RequestMethod.POST)
    public Object fetchSplits(@RequestBody FetchTableSplitsRequest ftsReq) {
        try {
            SourceReader reader = Env.getCurrentEnv().getReader(ftsReq);
            List splits = reader.getSourceSplits(ftsReq);
            return RestResponse.success(splits);
        } catch (Exception ex) {
            LOG.error("Failed to fetch splits, jobId={}", ftsReq.getJobId(), ex);
            return RestResponse.internalError(ExceptionUtils.getRootCauseMessage(ex));
        }
    }

    /** Fetch records from source reader */
    @RequestMapping(path = "/api/fetchRecords", method = RequestMethod.POST)
    public Object fetchRecords(@RequestBody FetchRecordRequest recordReq) {
        try {
            return RestResponse.success(pipelineCoordinator.fetchRecords(recordReq));
        } catch (Exception ex) {
            LOG.error("Failed fetch record, jobId={}", recordReq.getJobId(), ex);
            return RestResponse.internalError(ex.getMessage());
        }
    }

    /** Fetch records from source reader and Write records to backend */
    @RequestMapping(path = "/api/writeRecords", method = RequestMethod.POST)
    public Object writeRecord(@RequestBody WriteRecordRequest recordReq) {
        LOG.info(
                "Received write record request for jobId={}, taskId={}, meta={}",
                recordReq.getJobId(),
                recordReq.getTaskId(),
                recordReq.getMeta());
        pipelineCoordinator.writeRecordsAsync(recordReq);
        return RestResponse.success("Request accepted, processing asynchronously");
    }

    /** Fetch lastest end meta */
    @RequestMapping(path = "/api/fetchEndOffset", method = RequestMethod.POST)
    public Object fetchEndOffset(@RequestBody JobBaseConfig jobConfig) {
        LOG.info("Fetching end offset for job {}", jobConfig.getJobId());
        SourceReader reader = Env.getCurrentEnv().getReader(jobConfig);
        return RestResponse.success(reader.getEndOffset(jobConfig));
    }

    /** compare datasource Binlog Offset */
    @RequestMapping(path = "/api/compareOffset", method = RequestMethod.POST)
    public Object compareOffset(@RequestBody CompareOffsetRequest compareOffsetRequest) {
        SourceReader reader = Env.getCurrentEnv().getReader(compareOffsetRequest);
        return RestResponse.success(reader.compareOffset(compareOffsetRequest));
    }

    /** Close job */
    @RequestMapping(path = "/api/close", method = RequestMethod.POST)
    public Object close(@RequestBody JobBaseConfig jobConfig) {
        LOG.info("Closing job {}", jobConfig.getJobId());
        Env env = Env.getCurrentEnv();
        SourceReader reader = env.getReader(jobConfig);
        reader.close(jobConfig);
        env.close(jobConfig.getJobId());
        pipelineCoordinator.closeJobStreamLoad(jobConfig.getJobId());
        return RestResponse.success(true);
    }

    /** get task fail reason */
    @RequestMapping(path = "/api/getFailReason/{taskId}", method = RequestMethod.POST)
    public Object getFailReason(@PathVariable("taskId") String taskId) {
        return RestResponse.success(pipelineCoordinator.getTaskFailReason(taskId));
    }
}
