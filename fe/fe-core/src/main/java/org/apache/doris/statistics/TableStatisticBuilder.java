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

public class TableStatisticBuilder {
    public long rowCount;
    public long lastAnalyzeTimeInMs;
    public String updateTime;

    public TableStatisticBuilder() {
    }

    public TableStatisticBuilder(TableStatistic tableStatistic) {
        this.rowCount = tableStatistic.rowCount;
        this.updateTime = tableStatistic.updateTime;
    }

    public TableStatisticBuilder setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public TableStatisticBuilder setLastAnalyzeTimeInMs(long lastAnalyzeTimeInMs) {
        this.lastAnalyzeTimeInMs = lastAnalyzeTimeInMs;
        return this;
    }

    public TableStatisticBuilder setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
        return this;
    }

    public TableStatistic build() {
        return new TableStatistic(rowCount, lastAnalyzeTimeInMs, updateTime);
    }
}
