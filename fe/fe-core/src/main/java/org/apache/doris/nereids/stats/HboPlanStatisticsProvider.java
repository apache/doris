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

package org.apache.doris.nereids.stats;

import org.apache.doris.planner.PlanNodeAndHash;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;

import java.util.List;
import java.util.Map;

public interface HboPlanStatisticsProvider
{
    /**
     * Given a list of plan node hashes, returns historical statistics for them.
     * Some entries in return value may be missing if no corresponding history exists.
     * This can be called even when hash of a plan node is not present.
     *
     * TODO: Using PlanNode as map key can be expensive, we can use Plan node id as a map key.
     */
    Map<PlanNodeAndHash, RecentRunsPlanStatistics> getHboStats(List<PlanNodeAndHash> nodeIds);

    RecentRunsPlanStatistics getHboStats(PlanNodeAndHash PlanNodeAndHash);

    /**
     * Given plan hashes and corresponding statistics after a query is run, store them for future retrieval.
     */
    void putHboStats(Map<PlanNodeAndHash, RecentRunsPlanStatistics> hashesAndStatistics);
}