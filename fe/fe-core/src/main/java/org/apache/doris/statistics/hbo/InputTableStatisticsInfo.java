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

package org.apache.doris.statistics.hbo;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Describes plan statistics which are derived from history based optimizer.
 */
public class InputTableStatisticsInfo {
    private final Optional<String> hash;
    private final Optional<List<PlanStatistics>> inputTableStatistics;

    public InputTableStatisticsInfo(Optional<String> hash, Optional<List<PlanStatistics>> inputTableStatistics) {
        this.hash = Objects.requireNonNull(hash, "hash is null");
        this.inputTableStatistics = Objects.requireNonNull(inputTableStatistics,
                "inputTableStatistics is null");
    }

    public Optional<String> getHash() {
        return hash;
    }

    public Optional<List<PlanStatistics>> getInputTableStatistics() {
        return inputTableStatistics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputTableStatisticsInfo that = (InputTableStatisticsInfo) o;
        return Objects.equals(hash, that.hash) && Objects.equals(inputTableStatistics, that.inputTableStatistics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, inputTableStatistics);
    }
}
