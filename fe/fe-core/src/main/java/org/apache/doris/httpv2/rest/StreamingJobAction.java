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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;

import com.google.common.base.Strings;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StreamingJobAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(StreamingJobAction.class);

    @RequestMapping(path = "/api/streaming/commit_offset", method = RequestMethod.PUT)
    public Object commitOffset(@RequestBody CommitOffsetRequest offsetRequest, HttpServletRequest request) {
        String authToken = request.getHeader("token");
        // if auth token is not null, check it first
        if (!Strings.isNullOrEmpty(authToken)) {
            if (!checkClusterToken(authToken)) {
                throw new UnauthorizedException("Invalid token: " + authToken);
            }
            return updateOffset(offsetRequest);
        } else {
            // only use for token
            throw new UnauthorizedException("Miss token");
        }
    }

    private Object updateOffset(CommitOffsetRequest offsetRequest) {
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(offsetRequest.getJobId());
        if (job == null) {
            String errMsg = "Job " + offsetRequest.getJobId() + " not found";
            return ResponseEntityBuilder.okWithCommonError(errMsg);
        }
        if (!(job instanceof StreamingInsertJob)) {
            return ResponseEntityBuilder
                    .okWithCommonError("Job " + offsetRequest.getJobId() + " is not a streaming job");
        }

        StreamingInsertJob streamingJob = (StreamingInsertJob) job;
        try {
            LOG.info("Committing offset with {}", offsetRequest.toString());
            streamingJob.commitOffset(offsetRequest);
            return ResponseEntityBuilder.ok("Offset committed successfully");
        } catch (Exception e) {
            LOG.warn("Failed to commit offset for job {}, offset {}: {}", offsetRequest.getJobId(),
                    offsetRequest.getOffset(), e.getMessage());
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }
}
