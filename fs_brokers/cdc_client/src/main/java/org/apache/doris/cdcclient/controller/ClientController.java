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
import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.doris.cdcclient.model.req.FetchRecordReq;
import org.apache.doris.cdcclient.model.req.WriteRecordReq;
import org.apache.doris.cdcclient.model.resp.RecordWithMeta;
import org.apache.doris.cdcclient.model.resp.WriteMetaResp;
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
    public Object fetchSplits(@RequestBody JobConfig config) throws Exception {
        try {
            SourceReader<?, ?> reader =
                    Env.getCurrentEnv()
                            .getReader(
                                    config.getJobId(), config.getDataSource(), config.getConfig());
            List splits = reader.getSourceSplits(config);
            return ResponseEntityBuilder.ok(splits);
        } catch (IllegalArgumentException ex) {
            LOG.error("Failed to fetch splits, jobId={}", config.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    /** Fetch records from source reader */
    @RequestMapping(path = "/api/fetchRecords", method = RequestMethod.POST)
    public Object fetchRecords(@RequestBody FetchRecordReq recordReq) {
        try {
            RecordWithMeta response = pipelineCoordinator.read(recordReq);
            return ResponseEntityBuilder.ok(response);
        } catch (Exception ex) {
            LOG.error("Failed fetch record, jobId={}", recordReq.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    /** Fetch records from source reader and Write records to backend */
    @RequestMapping(path = "/api/writeRecords", method = RequestMethod.POST)
    public Object writeRecord(@RequestBody WriteRecordReq recordReq) {
        try {
            WriteMetaResp response = pipelineCoordinator.readAndWrite(recordReq);
            return ResponseEntityBuilder.ok(response);
        } catch (Exception ex) {
            LOG.error("Failed to write record, jobId={}", recordReq.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    @RequestMapping(path = "/api/close/{jobId}", method = RequestMethod.POST)
    public Object close(@PathVariable long jobId) {
        Env env = Env.getCurrentEnv();
        env.close(jobId);
        pipelineCoordinator.closeJob(jobId);
        return ResponseEntityBuilder.ok(true);
    }
}
