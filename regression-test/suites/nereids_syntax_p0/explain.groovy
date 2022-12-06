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

suite("nereids_explain") {
    sql """
        SET enable_vectorized_engine=true
    """

    sql """
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    explain {
        sql("select count(2) + 1, sum(2) + sum(lo_suppkey) from lineorder")
        contains "(sum(2) + sum(lo_suppkey))[#24]"
        contains "project output tuple id: 3"
    }


    explain {
        sql("physical plan select 100")
        contains "PhysicalOneRowRelation"
    }

    explain {
        sql("logical plan select 100")
        contains "LogicalOneRowRelation"
    }

    explain {
        sql("parsed plan select 100")
        contains "UnboundOneRowRelation"
    }
}
