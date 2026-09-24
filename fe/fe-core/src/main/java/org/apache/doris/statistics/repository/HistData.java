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

package org.apache.doris.statistics.repository;

public class HistData {

    public final StatsId statsId;

    public final double sampleRate;

    public final String buckets;

    public final String updateTime;

    public HistData(ResultRow row) {
        // histogram_statistics has no part_id column, StatsId(ResultRow) reads the column_statistics layout
        this.statsId = new StatsId(row.get(0), Long.parseLong(row.get(1)), Long.parseLong(row.get(2)),
                Long.parseLong(row.get(3)), Long.parseLong(row.get(4)), row.get(5), null);
        this.sampleRate = Double.parseDouble(row.get(6));
        this.buckets = row.get(7);
        this.updateTime = row.get(8);
    }
}
