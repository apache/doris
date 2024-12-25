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

suite("push_topn_to_agg") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql "set topn_opt_limit_threshold=1024"
    // limit -> agg
        // verify switch
    sql "set push_topn_to_agg=false"
    explain{
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey limit 4;"
        multiContains ("sortByGroupKey:false", 2)
    }
    sql "set push_topn_to_agg=true"
    explain{
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey limit 4;"
        multiContains ("sortByGroupKey:true", 2)
        notContains("STREAMING")
    }

    // when apply this opt, trun off STREAMING
    // limit -> proj -> agg, 
    explain{
        sql "select sum(c_custkey), c_name from customer group by c_name limit 6;"
        multiContains ("sortByGroupKey:true", 2)
        notContains("STREAMING")
    }

    // topn -> agg
    explain{
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey order by o_custkey limit 8;"
        multiContains ("sortByGroupKey:true", 2)
        notContains("STREAMING")
    }

    // order keys are part of group keys, 
    // 1. adjust group keys (o_custkey, o_clerk) -> o_clerk, o_custkey
    // 2. append o_custkey to order key 
    explain{
        sql "select sum(o_shippriority)  from orders group by o_custkey, o_clerk order by o_clerk limit 11;"
        contains("sortByGroupKey:true")
        contains("group by: o_clerk[#10], o_custkey[#9]")
        contains("order by: o_clerk[#18] ASC, o_custkey[#19] ASC")
    }


    // one distinct 
    explain {
        sql "select sum(distinct o_shippriority) from orders group by o_orderkey limit 13; "
        contains("VTOP-N")
        contains("order by: o_orderkey")
        multiContains("sortByGroupKey:true", 1)
    }

    // multi distinct 
    explain {
        sql "select count(distinct o_clerk), sum(distinct o_shippriority) from orders group by o_orderkey limit 14; "
        contains("VTOP-N")
        contains("order by: o_orderkey")
        multiContains("sortByGroupKey:true", 2)
    }

    // use group key as sort key to enable topn-push opt
    explain {
        sql "select sum(o_shippriority) from orders group by o_clerk limit 14; "
        contains("sortByGroupKey:true")
    }

    // group key is expression
    explain {
        sql "select sum(o_shippriority), o_clerk+1 from orders group by o_clerk+1 limit 15; "
        contains("sortByGroupKey:true")
    }

    // order key is not part of group key
    explain {
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey order by o_custkey+1 limit 16; "
        contains("sortByGroupKey:false")
        notContains("sortByGroupKey:true")
    }

    // topn + one phase agg
    explain {
        sql "select sum(ps_availqty), ps_partkey, ps_suppkey from partsupp group by ps_partkey, ps_suppkey order by ps_partkey, ps_suppkey limit 18;"
        contains("sortByGroupKey:true")
    }
}