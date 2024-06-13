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

    // order key should be prefix of group key
    explain{
        sql "select o_custkey, sum(o_shippriority), o_clerk  from orders group by o_custkey, o_clerk order by o_clerk, o_custkey limit 11;"
        multiContains("sortByGroupKey:false", 2)
    }

    // order key should be prefix of group key
    explain{
        sql "select o_custkey, o_clerk, sum(o_shippriority) as x from orders group by o_custkey, o_clerk order by o_custkey, x limit 12;"
        multiContains("sortByGroupKey:false", 2)
    }

    // one phase agg is optimized 
    explain {
        sql "select sum(o_shippriority) from orders group by o_orderkey limit 13; "
        contains("sortByGroupKey:true")
    }

    // group key is not output of limit, deny opt
    explain {
        sql "select sum(o_shippriority) from orders group by o_clerk limit 14; "
        contains("sortByGroupKey:false")
    }

    // group key is part of output of limit, apply opt
    explain {
        sql "select sum(o_shippriority), o_clerk from orders group by o_clerk limit 15; "
        contains("sortByGroupKey:true")
    }

    // order key is not prefix of group key
    explain {
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey order by o_custkey+1 limit 16; "
        contains("sortByGroupKey:false")
    }

    // order key is not prefix of group key
    explain {
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey order by o_custkey+1 limit 17; "
        contains("sortByGroupKey:false")
    }

    // topn + one phase agg
    explain {
        sql "select sum(ps_availqty), ps_partkey, ps_suppkey from partsupp group by ps_partkey, ps_suppkey order by ps_partkey, ps_suppkey limit 18;"
        contains("sortByGroupKey:true")
    }

    // sort key is prefix of group key, make all group key to sort key(ps_suppkey) and then apply push-topn-agg rule
    explain {
        sql "select sum(ps_availqty), ps_partkey, ps_suppkey from partsupp group by ps_partkey, ps_suppkey order by ps_partkey limit 19;"
        contains("sortByGroupKey:true")
    }

    explain {
        sql "select sum(ps_availqty), ps_suppkey, ps_availqty from partsupp group by ps_suppkey, ps_availqty order by ps_suppkey limit 19;"
        contains("sortByGroupKey:true")
    }

    // sort key is not prefix of group key, deny
    explain {
        sql "select sum(ps_availqty), ps_partkey, ps_suppkey from partsupp group by ps_partkey, ps_suppkey order by ps_suppkey limit 20;"
        contains("sortByGroupKey:false")
    }

    multi_sql """
    drop table if exists t1;
    CREATE TABLE IF NOT EXISTS t1
        (
        k1 TINYINT
        )
        ENGINE=olap
        AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );

    insert into t1 values (0),(1);

    drop table if exists t2;
    CREATE TABLE IF NOT EXISTS t2
        (
        k1 TINYINT
        )
        ENGINE=olap
        AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    insert into t2 values(5),(6);
    """

    // the result of following sql may be unstable, run 3 times
    qt_stable_1 """
    select * from (
        select k1 from t1
        UNION
        select k1 from t2
    ) as b order by k1  limit 2;
    """
    qt_stable_2 """
    select * from (
        select k1 from t1
        UNION
        select k1 from t2
    ) as b order by k1  limit 2;
    """
    qt_stable_3 """
    select * from (
        select k1 from t1
        UNION
        select k1 from t2
    ) as b order by k1  limit 2;
    """
}