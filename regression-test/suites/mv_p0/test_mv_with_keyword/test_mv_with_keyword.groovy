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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_mv_with_keyword") {

    multi_sql """
        drop table if exists t1_dup;
        create table t1_dup (
            `install` int,
            ```install``` int,
            c1 int
        )
        duplicate key(`install`)
        distributed BY hash(`install`) buckets 1
        rollup (r1(```install```, `install`))
        properties("replication_num" = "1");
        insert into t1_dup values(1,1,1), (2,2,2);

        drop table if exists t1_agg;
        create table t1_agg (
            `install` int,
            ```install``` int,
            `kill` int sum
        )
        aggregate key(`install`, ```install```)
        distributed BY hash(`install`) buckets 1
        rollup (r1(```install```, `kill`))
        properties("replication_num" = "1");
        insert into t1_agg values(1,1,1), (2,2,2);
    """
    createMV("create materialized view mv1 as select ```install``` as `select` from t1_dup where ```install``` > 0;")
    createMV("create materialized view mv2 as select `install` as ```select``` from t1_dup where `install` > 0;")
    createMV("create materialized view mv1 as select ```install``` as `select` from t1_agg group by ```install```;")
    createMV("create materialized view mv2 as select `install` as ```select```, sum(`kill`) as ```kill``` from t1_agg group by `install`;")
    explain {
        sql("select ```install``` as `select` from t1_dup where ```install``` > 0;")
        contains "(mv1)"
    }
    explain {
        sql("select `install` as ```select``` from t1_dup where `install` > 0;")
        contains "(mv2)"
    }
    explain {
        sql("select ```install``` as `select` from t1_agg group by ```install```;")
        contains "(mv1)"
    }
    explain {
        sql("select `install` as ```select```, sum(`kill`) as ```kill``` from t1_agg group by `install`;")
        contains "(mv2)"
    }
}