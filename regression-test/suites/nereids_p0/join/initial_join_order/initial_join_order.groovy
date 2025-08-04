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

suite("initial_join_order") {
    sql """
        drop table if exists t1;
        
        CREATE TABLE IF NOT EXISTS t1 (
        k int(11) NULL COMMENT "",
        v varchar(50) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 29
        PROPERTIES ("replication_num" = "1");

        insert into t1 values (1, 'a');

        alter table t1 modify column k set stats ('row_count'='1000', 'ndv'='1', 'num_nulls'='0', 'min_value'='0');

        drop table if exists t2;
        
        CREATE TABLE IF NOT EXISTS t2 (
        k int(11) NULL COMMENT "",
        v varchar(50) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 57
        PROPERTIES ("replication_num" = "1");

        insert into t2 values (1, 'a');
        alter table t2 modify column k set stats ('row_count'='1000000', 'ndv'='10', 'num_nulls'='0', 'min_value'='0');

        set runtime_filter_mode=off;
        set memo_max_group_expression_size=1;
        set enable_parallel_result_sink = true;
    """

    qt_inner """
        explain shape plan
        select * from t1 join t2 on t1.k = t2.k
    """

    explain {
        sql """
        shape plan
        select * from t1 left join t2 on t1.k = t2.k
        """
        contains "RIGHT_OUTER_JOIN"
    }

    // do not swap left semi
    explain {
        sql """
            shape plan
            select * from t1 left semi join t2 on t1.k = t2.k
            """
        contains "LEFT_SEMI_JOIN"
    }


    // do not swap left anti
    explain {
        sql """
            shape plan
            select * from t1 left anti join t2 on t1.k = t2.k
            """
        contains "LEFT_ANTI_JOIN"
    }

    explain {
        sql """
            shape plan
            select * from t1 right semi join t2 on t1.k = t2.k
            """
        contains "LEFT_SEMI_JOIN"
    }


    explain {
        sql """
            shape plan
            select * from t1 right anti join t2 on t1.k = t2.k
            """
        contains "LEFT_ANTI_JOIN"
    }

    explain {
        sql """
        shape plan
        select * from t1 right semi join t2 on t1.k = t2.k
        """
        contains "LEFT_SEMI_JOIN"
    }
    
    explain {
        sql """
        shape plan
        select * from t1 right anti join t2 on t1.k = t2.k
        """
        contains "LEFT_ANTI_JOIN"
    }
    
}