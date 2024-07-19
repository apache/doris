/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("query43") {
    if (isCloudMode()) {
        return
    }
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql 'set topn_opt_limit_threshold = 1024'
    def ckBench = """SELECT DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') ORDER BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') LIMIT 10 OFFSET 1000"""
    qt_ckbench_shape_43 """
    explain shape plan
    ${ckBench}
    """
}
