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

suite("transform_outer_join_to_anti") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    sql """drop table if exists eliminate_outer_join_A;"""
    sql """drop table if exists eliminate_outer_join_B;"""
    sql """
        create table eliminate_outer_join_A ( a int not null, null_a int )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table eliminate_outer_join_B ( b int not null, null_b int )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    explain {
        sql("select eliminate_outer_join_A.* from eliminate_outer_join_A left outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_B.b is null")
        contains "ANTI JOIN"
    }

    explain {
        sql("select eliminate_outer_join_B.* from eliminate_outer_join_A right outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_A.a is null")
        contains "ANTI JOIN"
    }

    explain {
        sql("select eliminate_outer_join_A.* from eliminate_outer_join_A left outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_B.null_b is null")
        contains "OUTER JOIN"
    }

    explain {
        sql("select eliminate_outer_join_B.* from eliminate_outer_join_A right outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_A.null_a is null")
        contains "OUTER JOIN"
    }

    explain {
        sql("select eliminate_outer_join_A.* from eliminate_outer_join_A left outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_B.b is null or eliminate_outer_join_A.null_a is null")
        contains "OUTER JOIN"
    }

    explain {
        sql("select * from eliminate_outer_join_A left outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_B.b is null and eliminate_outer_join_A.null_a is null")
        contains "ANTI JOIN"
    }

    explain {
        sql("select * from eliminate_outer_join_A left outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_B.b is null and eliminate_outer_join_B.null_b is null")
        contains "ANTI JOIN"
    }

    explain {
        sql("select * from eliminate_outer_join_A right outer join eliminate_outer_join_B on eliminate_outer_join_B.b = eliminate_outer_join_A.a where eliminate_outer_join_A.a is null and eliminate_outer_join_B.null_b is null and eliminate_outer_join_A.null_a is null")
        contains "ANTI JOIN"
    }
}

