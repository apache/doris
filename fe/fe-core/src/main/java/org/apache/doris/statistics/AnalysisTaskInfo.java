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

import java.util.Set;

public class AnalysisTaskInfo extends AnalysisInfo {

    public final long taskId;

    public final String colName;

    public final Set<String> partitions;

    // True means this task is a table level task for external table.
    // This kind of task is mainly to collect the number of rows of a table.
    public final boolean externalTableLevelTask;

    public static class Builder extends AnalysisInfo.Builder<Builder> {
        private long taskId;
        private String colName;
        private Set<String> partitions;
        private boolean externalTableLevelTask;

        public Builder() {
        }

        public Builder(AnalysisJobInfo jobInfo) {
            super(jobInfo);
        }

        public Builder taskId(long taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder colName(String colName) {
            this.colName = colName;
            return this;
        }

        public Builder partitions(Set<String> partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder externalTableLevelTask(boolean externalTableLevelTask) {
            this.externalTableLevelTask = externalTableLevelTask;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AnalysisTaskInfo build() {
            return new AnalysisTaskInfo(this);
        }
    }

    private AnalysisTaskInfo(Builder builder) {
        super(builder);
        this.taskId = builder.taskId;
        this.colName = builder.colName;
        this.partitions = builder.partitions;
        this.externalTableLevelTask = builder.externalTableLevelTask;
    }
}
