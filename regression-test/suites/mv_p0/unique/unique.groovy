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

suite ("unique") {

    sql """ DROP TABLE IF EXISTS u_table; """

    sql """
            create table u_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            unique key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "false");
        """
        // only mor table can have mv

    sql "insert into u_table select 1,1,1,'a';"
    sql "insert into u_table select 20,2,2,'b';"

    test {
        sql """create materialized view k12s3m as select k1,sum(k2),max(k2) from u_table group by k1;"""
        exception "must not has grouping columns"
    }

    test {
        sql """create materialized view kadj as select k4 from u_table"""
        exception "The materialized view of uniq table must contain all key columns. column:k1"
    }

    test {
        sql """create materialized view kadj as select k4,k1,k2,k3 from u_table"""
        exception "The materialized view not support value column before key column"
    }

    createMV("create materialized view kadj as select k3,k2,k1,k4 from u_table;")

    createMV("create materialized view kadj2 as select k1,k3,k2,length(k4) from u_table;")

    createMV("create materialized view k31l42 as select k1,k3,length(k1),k2 from u_table;")
    sql "insert into u_table select 300,-3,null,'c';"

    sql """analyze table u_table with sync;"""
    sql """set enable_stats=false;"""
    explain {
        sql("select k3,length(k1),k2 from u_table order by 1,2,3;")
        contains "(k31l42)"
    }
    qt_select "select k3,length(k1),k2 from u_table order by 1,2,3;"


    test {
        sql """create materialized view kadp as select k4 from u_table group by k4;"""
        exception "must not has grouping columns"
    }

    qt_select_star "select * from u_table order by k1;"

    sql """set enable_stats=true;"""
    explain {
        sql("select k3,length(k1),k2 from u_table order by 1,2,3;")
        contains "(k31l42)"
    }

    // todo: support match query
}
