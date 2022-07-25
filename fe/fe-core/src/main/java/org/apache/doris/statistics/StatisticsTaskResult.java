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


import org.apache.doris.statistics.StatsCategory.Category;
import org.apache.doris.statistics.StatsGranularity.Granularity;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StatisticsTaskResult {
    private List<TaskResult> taskResults;

    public StatisticsTaskResult(List<TaskResult> taskResults) {
        this.taskResults = taskResults;
    }

    public List<TaskResult> getTaskResults() {
        return taskResults;
    }

    public void setTaskResults(List<TaskResult> taskResults) {
        this.taskResults = taskResults;
    }

    public static class TaskResult {
        private long dbId = -1L;
        private long tableId = -1L;
        private String partitionName = "";
        private String columnName = "";

        private Category category;
        private Granularity granularity;
        private Map<StatsType, String> statsTypeToValue;

        public long getDbId() {
            return dbId;
        }

        public void setDbId(long dbId) {
            this.dbId = dbId;
        }

        public long getTableId() {
            return tableId;
        }

        public void setTableId(long tableId) {
            this.tableId = tableId;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public void setPartitionName(String partitionName) {
            this.partitionName = partitionName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public Category getCategory() {
            return category;
        }

        public void setCategory(Category category) {
            this.category = category;
        }

        public Granularity getGranularity() {
            return granularity;
        }

        public void setGranularity(Granularity granularity) {
            this.granularity = granularity;
        }

        public Map<StatsType, String> getStatsTypeToValue() {
            return statsTypeToValue;
        }

        public void setStatsTypeToValue(Map<StatsType, String> statsTypeToValue) {
            this.statsTypeToValue = statsTypeToValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TaskResult that = (TaskResult) o;
            return dbId == that.dbId
                    && tableId == that.tableId
                    && partitionName.equals(that.partitionName)
                    && columnName.equals(that.columnName)
                    && category == that.category
                    && granularity == that.granularity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbId, tableId, partitionName,
                    columnName, category, granularity);
        }
    }
}
