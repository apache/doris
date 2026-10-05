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

suite("push_filter_window_eqset") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false';
    sql "set enable_parallel_result_sink=false;"

    /**
    check the filter is pushed through window
    PhysicalResultSink
    --PhysicalDistribute[DistributionSpecGather]
    ----PhysicalProject
    ------PhysicalWindow
    --------PhysicalQuickSort[LOCAL_SORT]
    ----------PhysicalDistribute[DistributionSpecHash]
    ------------PhysicalProject
    >>>>>--------filter((region.r_regionkey = 1))
    ----------------PhysicalOlapScan[region]
    **/
    qt_eqset """
        explain shape plan
        select y, rn
        from (
            select r_regionkey as x, r_regionkey as y, row_number() over(partition by r_regionkey) as rn from region 
        ) T
        where y = 1;
    """
}