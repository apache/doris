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

import org.apache.doris.common.Pair;
import org.apache.doris.info.TableNameInfo;

import java.util.Set;

/**
 * Wrapper class for table analysis jobs with priority information.
 * Used in priority queues to sort jobs by query frequency and other factors.
 */
public class PriorityTableJob implements Comparable<PriorityTableJob> {
    private final TableNameInfo tableNameInfo;
    private final Set<Pair<String, String>> columns;
    private final double priorityScore;
    private final long createTime;

    public PriorityTableJob(TableNameInfo tableNameInfo, Set<Pair<String, String>> columns, double priorityScore) {
        this.tableNameInfo = tableNameInfo;
        this.columns = columns;
        this.priorityScore = priorityScore;
        this.createTime = System.currentTimeMillis();
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public Set<Pair<String, String>> getColumns() {
        return columns;
    }

    public double getPriorityScore() {
        return priorityScore;
    }

    public long getCreateTime() {
        return createTime;
    }

    /**
     * Compare by priority score (higher score = higher priority).
     * If scores are equal, compare by create time (older = higher priority).
     */
    @Override
    public int compareTo(PriorityTableJob other) {
        // Higher score means higher priority (so we reverse the comparison)
        int scoreCompare = Double.compare(other.priorityScore, this.priorityScore);
        if (scoreCompare != 0) {
            return scoreCompare;
        }
        // If scores are equal, older jobs have higher priority
        return Long.compare(this.createTime, other.createTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PriorityTableJob that = (PriorityTableJob) obj;
        return tableNameInfo.equals(that.tableNameInfo);
    }

    @Override
    public int hashCode() {
        return tableNameInfo.hashCode();
    }

    @Override
    public String toString() {
        return "PriorityTableJob{" +
                "table=" + tableNameInfo +
                ", priorityScore=" + priorityScore +
                ", columns=" + columns.size() +
                '}';
    }
}

