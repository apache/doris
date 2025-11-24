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

import org.apache.doris.cdcclient.common.JobManager;
import org.apache.doris.cdcclient.model.FetchRecordReq;
import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.doris.cdcclient.model.rest.ResponseEntityBuilder;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.cdcclient.source.reader.SourceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Locale;
import java.util.Map;

@RestController
public class ClientController {
    private static final Logger LOG = LoggerFactory.getLogger(ClientController.class);

    /**
     * Fetch source splits for snapshot
     * @param config
     * @return
     * @throws Exception
     */
    @RequestMapping(path = "/api/fetchSplits", method = RequestMethod.POST)
    public Object fetchSplits(@RequestBody JobConfig config) throws Exception {
        try {
            SourceReader reader =
                    prepareReader(config.getJobId(), config.getDataSource(), config.getConfig());
            List splits = reader.getSourceSplits(config);
            return ResponseEntityBuilder.ok(splits);
        } catch (IllegalArgumentException ex) {
            LOG.error("Failed to fetch splits, jobId={}", config.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    /**
     * Fetch records from source reader
     * @param recordReq
     * @return
     */
    @RequestMapping(path = "/api/fetchRecords", method = RequestMethod.POST)
    public Object fetchRecords(@RequestBody FetchRecordReq recordReq) {
        try {
            SourceReader reader =
                    prepareReader(recordReq.getJobId(), recordReq.getDataSource(), recordReq.getConfig());
            Object response = reader.read(recordReq);
            return ResponseEntityBuilder.ok(response);
        } catch (Exception ex) {
            LOG.error("Failed fetch record, jobId={}", recordReq.getJobId(), ex);
            return ResponseEntityBuilder.badRequest(ex.getMessage());
        }
    }

    /**
     * Write records to backend
     * @param recordReq
     * @return
     */
    @RequestMapping(path = "/api/writeRecord", method = RequestMethod.POST)
    public Object writeRecord(@RequestBody FetchRecordReq recordReq) {
        // 拉取数据，写入到backend，这里先不实现
       return null;
    }

    @RequestMapping(path = "/api/close/{jobId}", method = RequestMethod.POST)
    public Object close(@PathVariable long jobId) {
        JobManager manager = JobManager.getInstance();
        manager.close(jobId);
        return ResponseEntityBuilder.ok(true);
    }

    private DataSource resolveDataSource(String source) {
        if (source == null || source.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing dataSource");
        }
        try {
            return DataSource.valueOf(source.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported dataSource: " + source, ex);
        }
    }

    private SourceReader prepareReader(
            Long jobId, String dataSource, Map<String, String> config) {
        DataSource parsed = resolveDataSource(dataSource);
        JobManager manager = JobManager.getInstance();
        manager.attachOptions(jobId, parsed, config);
        return manager.getOrCreateReader(jobId, parsed);
    }
}
