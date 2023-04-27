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

public enum StatsType {
    ROW_COUNT("row_count"),
    DATA_SIZE("data_size"),
    NDV("ndv"),
    AVG_SIZE("avg_size"),
    MAX_SIZE("max_size"),
    NUM_NULLS("num_nulls"),
    MIN_VALUE("min_value"),
    MAX_VALUE("max_value"),
    HISTOGRAM("histogram"),
    // only for test
    UNKNOWN("unknown");

    private final String value;

    StatsType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static StatsType fromString(String value) {
        for (StatsType type : StatsType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid StatsType: " + value);
    }
}
