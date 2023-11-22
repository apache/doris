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
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql "SET enable_pipeline_engine=true"

    explain {
        sql("select count(2) + 1, sum(2) + sum(lo_suppkey) from lineorder")
        contains "sum(2) + sum(lo_suppkey)"
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

    explain {
        sql("plan select * from lineorder where lo_orderkey > (select avg(lo_orderkey) from lineorder)")
        contains "*LogicalSubQueryAlias"
    }

    explain {
        sql("plan with s as (select * from supplier) select * from s as s1, s as s2")
        contains "*LogicalSubQueryAlias"
    }

    explain {
        sql """
        verbose 
        select case 
            when 1=1 then cast(1 as int) 
            when 1>1 then cast(1 as float)
            else 0.0 end;
            """
        contains "SlotDescriptor{id=0, col=null, colUniqueId=null, type=DOUBLE, nullable=false, isAutoIncrement=false}"
    }

    def explainStr = sql("select sum(if(lo_tax=1,lo_tax,0)) from lineorder where false").toString()
    assertTrue(!explainStr.contains("projections"))
}
