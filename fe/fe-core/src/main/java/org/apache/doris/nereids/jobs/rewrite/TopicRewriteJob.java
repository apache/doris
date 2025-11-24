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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** TopicRewriteJob */
public class TopicRewriteJob implements RewriteJob {

    public final String topicName;
    public final List<RewriteJob> jobs;
    public final Optional<Predicate<CascadesContext>> condition;

    /** constructor */
    public TopicRewriteJob(String topicName, List<RewriteJob> jobs, Predicate<CascadesContext> condition) {
        this.topicName = topicName;
        this.condition = Optional.ofNullable(condition);
        this.jobs = jobs.stream()
                .flatMap(job -> {
                    if (job instanceof TopicRewriteJob && !((TopicRewriteJob) job).condition.isPresent()) {
                        return ((TopicRewriteJob) job).jobs.stream();
                    } else {
                        return Stream.of(job);
                    }
                })
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
