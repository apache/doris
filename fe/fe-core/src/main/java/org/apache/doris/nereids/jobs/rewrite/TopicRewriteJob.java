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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Stream;

/** TopicRewriteJob */
public class TopicRewriteJob implements RewriteJob {

    public final String topicName;
    public final List<RewriteJob> jobs;

    /** constructor */
    public TopicRewriteJob(String topicName, List<RewriteJob> jobs) {
        this.topicName = topicName;
        this.jobs = jobs.stream()
                .flatMap(job -> job instanceof TopicRewriteJob
                        ? ((TopicRewriteJob) job).jobs.stream()
                        : Stream.of(job)
                )
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public void execute(JobContext jobContext) {
        throw new AnalysisException("should not execute topic rewrite job " + topicName + " directly.");
    }

    @Override
    public boolean isOnce() {
        return true;
    }

}
