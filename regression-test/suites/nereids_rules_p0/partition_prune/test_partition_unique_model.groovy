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

suite("test_partition_unique_model") {
    // filter about invisible column "DORIS_DELETE_SIGN = 0" has no impaction on partition pruning
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set partition_pruning_expand_threshold=10;"
    sql "drop table if exists xinan;"
    sql """
        create table xinan
        (
            init_date int null
        )
        engine=olap
        unique key(init_date)
        partition by range(init_date)
        (partition p202209 values[("20220901"), ("20221001")),
        partition p202210 values[("20221001"), ("20221101")))
        distributed by hash (init_date) buckets auto
        properties(
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql "insert into xinan values(20220901), (20221003);"

    explain {
        sql "select * from xinan where init_date >=20221001;"
        contains "partitions=1/2 (p202210)"
    }

    explain {
        sql "select * from xinan where init_date<20221101;"
        contains "partitions=2/2 (p202209,p202210)"
    } 

    explain {
        sql "select * from xinan where init_date >=20221001 and init_date<20221101;"
        contains "partitions=1/2 (p202210)"
    }

}
