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

suite("test_terminate", "nonConcurrent") {
    sql "set enable_spill = true"
    sql "set enable_force_spill = true"
    sql "set disable_join_reorder=true;"
    sql "set runtime_filter_type='bloom_filter';"
    sql "set parallel_pipeline_task_num=2"
    sql "set enable_runtime_filter_prune=false;"

    sql """ drop table if exists t1; """
    sql """ drop table if exists t2; """

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
        create table t2 (
            k1 int null,
            k2 int null
        )
        duplicate key (k1)
        distributed BY hash(k1) buckets 16
        properties("replication_num" = "1");

    """

    sql """
    insert into t1 values(1,1),(2,2),(3,3);
    """
    
    sql """
    insert into t2 values(1,1),(2,2),(3,3);
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("PipelineTask::execute.terminate",[pipeline_id: 1, task_id: 1, fragment_id: 3])
        sql """ select * from t1 join [shuffle] t2 on t1.k2 = t2.k2  """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("PipelineTask::execute.terminate")
    }
}
