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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

public class PlanNodeCanonicalInfo
{
    private final String hash;
    private final List<PlanStatistics> inputTableStatistics;

    public PlanNodeCanonicalInfo(String hash, List<PlanStatistics> inputTableStatistics)
    {
        this.hash = requireNonNull(hash, "hash is null");
        this.inputTableStatistics = requireNonNull(inputTableStatistics, "inputTableStatistics is null");
    }

    public String getHash()
    {
        return hash;
    }

    public List<PlanStatistics> getInputTableStatistics()
    {
        return inputTableStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeCanonicalInfo that = (PlanNodeCanonicalInfo) o;
        return hash == that.hash && inputTableStatistics.equals(that.inputTableStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(System.identityHashCode(hash), inputTableStatistics);
    }
}
