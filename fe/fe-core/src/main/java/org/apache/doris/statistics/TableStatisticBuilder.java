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
    public long updateRows;
    public int healthy;
    public long dataSizeInBytes;
    public String updateTime;
    public String lastAnalyzeTime;

    public TableStatisticBuilder() {
    }

    public TableStatisticBuilder(TableStatistic tableStatistic) {
        this.rowCount = tableStatistic.rowCount;
        this.updateRows = tableStatistic.updateRows;
        this.healthy = tableStatistic.healthy;
        this.dataSizeInBytes = tableStatistic.dataSizeInBytes;
        this.updateTime = tableStatistic.updateTime;
        this.lastAnalyzeTime = tableStatistic.lastAnalyzeTime;
    }

    public long getRowCount() {
        return rowCount;
    }

    public TableStatisticBuilder setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public long getUpdateRows() {
        return updateRows;
    }

    public TableStatisticBuilder setUpdateRows(long updateRows) {
        this.updateRows = updateRows;
        return this;
    }

    public double getHealthy() {
        return healthy;
    }

    public TableStatisticBuilder setHealthy(int healthy) {
        this.healthy = healthy;
        return this;
    }

    public long getDataSizeInBytes() {
        return dataSizeInBytes;
    }

    public TableStatisticBuilder setDataSizeInBytes(long dataSizeInBytes) {
        this.dataSizeInBytes = dataSizeInBytes;
        return this;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public TableStatisticBuilder setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
        return this;
    }

    public String getLastAnalyzeTime() {
        return lastAnalyzeTime;
    }

    public TableStatisticBuilder setLastAnalyzeTime(String lastAnalyzeTime) {
        this.lastAnalyzeTime = lastAnalyzeTime;
        return this;
    }


    public TableStatistic build() {
        return new TableStatistic(rowCount, updateRows,
                healthy, dataSizeInBytes, updateTime, lastAnalyzeTime);
    }
}
