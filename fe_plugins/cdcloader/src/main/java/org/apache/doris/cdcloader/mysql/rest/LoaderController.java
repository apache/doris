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

package org.apache.doris.cdcloader.mysql.rest;


import org.apache.doris.cdcloader.common.rest.ResponseEntityBuilder;
import org.apache.doris.cdcloader.mysql.config.LoadContext;
import org.apache.doris.cdcloader.mysql.reader.SourceReader;
import org.apache.doris.cdcloader.mysql.rest.model.FetchRecordReq;
import org.apache.doris.cdcloader.mysql.rest.model.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class LoaderController {
    private static final Logger LOG = LoggerFactory.getLogger(LoaderController.class);
    @RequestMapping(path = "/api/fetchRecords", method = RequestMethod.POST)
    public Object fetchRecords(@RequestBody FetchRecordReq recordReq) {
        LoadContext context  = LoadContext.getInstance();
        SourceReader reader = context.getSourceReader();
        try{
            Object response = reader.read(recordReq);
            return ResponseEntityBuilder.ok(response);
        }catch (Exception ex){
            LOG.error("Failed fetch record,", ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    @RequestMapping(path = "/api/fetchSplits", method = RequestMethod.POST)
    public Object fetchSplits(@RequestBody JobConfig config) throws Exception {
        LoadContext context  = LoadContext.getInstance();
        SourceReader reader = context.getSourceReader();
        List splits = reader.getSourceSplits(config);
        return ResponseEntityBuilder.ok(splits);
    }

    @RequestMapping(path = "/api/status", method = RequestMethod.GET)
    public Object isStarted(@RequestParam("jobId") long jobId) {
        return ResponseEntityBuilder.ok(true);
    }

    @RequestMapping(path = "/api/job/cdc/heartbeat", method = RequestMethod.POST)
    public Object heartbeat(@RequestParam("jobId") long jobId) {
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/api/close/{jobId}", method = RequestMethod.POST)
    public Object close(@PathVariable long jobId) {
        LoadContext context  = LoadContext.getInstance();
        SourceReader reader = context.getSourceReader();
        reader.close(jobId);
        return ResponseEntityBuilder.ok(true);
    }

}
