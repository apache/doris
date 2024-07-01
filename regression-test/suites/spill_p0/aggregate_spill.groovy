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

suite("aggregate_spill") {
    sql """
        set enable_agg_spill = true;
    """
    sql """
        set enable_force_spill = true;
    """
    sql """
        set min_revocable_mem = 1;
    """
    sql """
        set parallel_pipeline_task_num = 4;
    """
    sql """
        drop table if exists aggregate_spill_test;
    """
    sql """
        CREATE TABLE `aggregate_spill_test` (k1 int, k2 int replace) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into aggregate_spill_test values(1, 1), (2, 1), (3, 1), (4, 1), (5, 1);
    """
    qt_aggregate_spill """
        select count(), k2 from aggregate_spill_test group by k2 limit 1;
    """
}