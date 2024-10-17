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

suite("push_down_multi_filter_through_window") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set ignore_shape_nodes='PhysicalDistribute'"
    sql "drop table if exists push_down_multi_predicate_through_window_t"
    multi_sql """
    CREATE TABLE push_down_multi_predicate_through_window_t (c1 INT, c2 INT, c3 VARCHAR(50)) properties("replication_num"="1");
    INSERT INTO push_down_multi_predicate_through_window_t (c1, c2, c3) VALUES(1, 10, 'A'),(2, 20, 'B'),(3, 30, 'C'),(4, 40, 'D');
    """
    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn <= 1;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1, c2 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1, c2 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rk > 1;")
        notContains "VPartitionTopN"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn > 1;")
        notContains "VPartitionTopN"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn, rank() over(partition by c1 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rn <= 1 and rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, row_number() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn <= 10 and rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 10"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, row_number() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, row_number() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn <= 10;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 10"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, rank() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn <= 1 and rk <= 10;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, rank() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn <= 10 and rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, rank() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn > 1 and rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1 order by c3) as rk, rank() over(partition by c1, c2 order by c3) as rn from push_down_multi_predicate_through_window_t) t where rn <= 1 and rk > 1;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn, rank() over(partition by c1 order by c3) as rk from push_down_multi_predicate_through_window_t) t limit 10;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 10"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1, c2 order by c3) as rn, rank() over(partition by c1 order by c3) as rk from push_down_multi_predicate_through_window_t) t limit 10;")
        contains "VPartitionTopN"
        contains "functions: rank"
        contains "partition limit: 10"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn, row_number() over(partition by c1 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rn <= 10 and rk <= 1;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn, row_number() over(partition by c1 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rn <= 1 and rk <= 10;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn, rank() over(partition by c1 order by c3) as rk1, rank() over(partition by c2 order by c3) as rk2 from push_down_multi_predicate_through_window_t) t where rn <= 1 and rk1 <= 10 and rk2 <= 100;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1 order by c3) as rn1, row_number() over(partition by c2 order by c3) as rn2, rank() over(partition by c1, c2 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rn1 <= 10 and rn2 <= 1 and rk <= 100;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select rank() over(partition by c1, c2 order by c3) as rk, row_number() over(partition by c1 order by c3) as rn1, row_number() over(partition by c2 order by c3) as rn2 from push_down_multi_predicate_through_window_t) t where rn1 <= 1 and rn2 <= 10 and rk <= 100;")
        contains "VPartitionTopN"
        contains "functions: row_number"
        contains "partition limit: 1"
    }

    explain {
        sql ("select * from (select row_number() over(partition by c1, c2 order by c3) as rn, rank() over(partition by c1 order by c3) as rk from push_down_multi_predicate_through_window_t) t where rn <= 1 or rk <= 1;")
        notContains "VPartitionTopN"
    }
}
