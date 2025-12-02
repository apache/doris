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
import org.apache.doris.cdcclient.model.request.FetchRecordReq;
import org.apache.doris.cdcclient.model.request.FetchTableSplitsReq;
import org.apache.doris.cdcclient.model.request.WriteRecordReq;
import org.apache.doris.cdcclient.model.rest.ResponseEntityBuilder;
import org.apache.doris.cdcclient.service.PipelineCoordinator;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class ClientController {
    private static final Logger LOG = LoggerFactory.getLogger(ClientController.class);

    @Autowired private PipelineCoordinator pipelineCoordinator;

    /** Fetch source splits for snapshot */
    @RequestMapping(path = "/api/fetchSplits", method = RequestMethod.POST)
    public Object fetchSplits(@RequestBody FetchTableSplitsReq ftsReq) {
        try {
            SourceReader<?, ?> reader = Env.getCurrentEnv().getReader(ftsReq);
            List splits = reader.getSourceSplits(ftsReq);
            return ResponseEntityBuilder.ok(splits);
        } catch (IllegalArgumentException ex) {
            LOG.error("Failed to fetch splits, jobId={}", ftsReq.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    /** Fetch records from source reader */
    @RequestMapping(path = "/api/fetchRecords", method = RequestMethod.POST)
    public Object fetchRecords(@RequestBody FetchRecordReq recordReq) {
        try {
            SourceReader<?, ?> reader = Env.getCurrentEnv().getReader(recordReq);
            return ResponseEntityBuilder.ok(reader.read(recordReq));
        } catch (Exception ex) {
            LOG.error("Failed fetch record, jobId={}", recordReq.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    /** Fetch records from source reader and Write records to backend */
    @RequestMapping(path = "/api/writeRecords", method = RequestMethod.POST)
    public Object writeRecord(@RequestBody WriteRecordReq recordReq) {
        pipelineCoordinator.writeRecordsAsync(recordReq);
        return ResponseEntityBuilder.ok("Request accepted, processing asynchronously");
    }

    /** Fetch lastest end meta */
    @RequestMapping(path = "/api/fetchEndOffset", method = RequestMethod.POST)
    public Object fetchEndOffset(@RequestBody WriteRecordReq recordReq) {
        pipelineCoordinator.writeRecordsAsync(recordReq);
        return ResponseEntityBuilder.ok("Request accepted, processing asynchronously");
    }

    @RequestMapping(path = "/api/close/{jobId}", method = RequestMethod.POST)
    public Object close(@PathVariable long jobId) {
        Env env = Env.getCurrentEnv();
        env.close(jobId);
        pipelineCoordinator.closeJob(jobId);
        return ResponseEntityBuilder.ok(true);
    }
}
