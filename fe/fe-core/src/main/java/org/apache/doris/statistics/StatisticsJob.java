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

package org.apache.doris.statistics;

import org.apache.doris.analysis.AnalyzeStmt;

import com.google.common.collect.Maps;

import org.glassfish.jersey.internal.guava.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.util.Lists;

/*
Used to store statistics job info,
including job status, progress, etc.
 */
public class StatisticsJob {

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED
    }

    private long id = -1;
    private JobState jobState = JobState.PENDING;
    // optional
    // to be collected table stats
    private List<Long> tableId = Lists.newArrayList();
    // to be collected column stats
    private Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
    private Map<String, String> properties;
    // end

    private List<StatisticsTask> taskList = Lists.newArrayList();

    public long getId() {
        return id;
    }

    /*
        AnalyzeStmt: Analyze t1(c1), t2
        StatisticsJob:
          tableId [t1, t2]
          tableIdToColumnName <t1, [c1]> <t2, [c1,c2,c3]>
         */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) {
        // TODO
        return new StatisticsJob();
    }

    public Set<Long> relatedTableId() {
        Set<Long> relatedTableId = Sets.newHashSet();
        relatedTableId.addAll(tableId);
        relatedTableId.addAll(tableIdToColumnName.keySet());
        return relatedTableId;
    }
}
