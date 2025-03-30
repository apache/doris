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

suite("test_slow_close", "nonConcurrent") {
    sql "set enable_spill = false"
    sql "set enable_force_spill = false"
    sql "set disable_join_reorder=true;"
    sql "set runtime_filter_type='bloom_filter';"
    sql "set parallel_pipeline_task_num=3"
    sql "set ignore_runtime_filter_ids='1,2';"
    sql "set enable_runtime_filter_prune=false;"

    sql """ drop table if exists t1; """
    sql """ drop table if exists t3; """
    sql """ drop table if exists t5; """

    sql """
        create table t1 (
            k1 int null,
            k2 int null
        )
        duplicate key (k1)
        distributed BY hash(k1) buckets 16
        properties("replication_num" = "1");
        """

    sql """
        create table t3 (
            k1 int null,
            k2 int null
        )
        duplicate key (k1)
        distributed BY hash(k1) buckets 16
        properties("replication_num" = "1");

    """

    sql """
        create table t5 (
            k1 int null,
            k2 int null
        )
        duplicate key (k1)
        distributed BY hash(k1) buckets 16
        properties("replication_num" = "1");
    """

    sql """
    insert into t1 select e1,e1 from (select 1 k1) as t lateral view explode_numbers(100000) tmp1 as e1;
    """
    
    sql """
    insert into t3 values(1,1),(2,2),(3,3);
    """

    sql """
    insert into t5 values(1,1),(2,2),(3,3),(4,4),(5,5);
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("Pipeline::make_all_runnable.sleep",[pipeline_id: 4])
        qt_sql "select count(*),sleep(2) from (select t1.k1 from t5 join [broadcast] t1 on t1.k1=t5.k1) tmp join [broadcast] t3 join t3 t3s [broadcast] on tmp.k1=t3.k1 and t3s.k1=t3.k1 where t3.k2=5;"
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("Pipeline::make_all_runnable.sleep")
    }

    sql "set ignore_runtime_filter_ids='0';"
    try {
        GetDebugPoint().enableDebugPointForAllBEs("PipelineTask::execute.open_sleep",[pipeline_id: 4, task_id: 7])
        GetDebugPoint().enableDebugPointForAllBEs("PipelineTask::execute.sink_eos_sleep",[pipeline_id: 4, task_id: 15])
        qt_sql "select count(*),sleep(2) from (select t1.k1 from t5 join [broadcast] t1 on t1.k1=t5.k1) tmp join [broadcast] t3 join t3 t3s [broadcast] on tmp.k1=t3.k1 and t3s.k1=t3.k1 where t3.k2=5;"
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("PipelineTask::execute.open_sleep")
        GetDebugPoint().disableDebugPointForAllBEs("PipelineTask::execute.sink_eos_sleep")
    }
}
