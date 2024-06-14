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

suite ("mvljc") {
    sql """ DROP TABLE IF EXISTS d_table; """
    sql """set enable_nereids_planner=true"""
    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"

    createMV ("""CREATE MATERIALIZED VIEW mvljc AS SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1;""")

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"

    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1 order by 1,2;")
        contains "(mvljc)"
    }
    qt_select_mv "SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1 order by 1,2;"

    explain {
        sql("SELECT a.k1 + 1 FROM ( SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1 ) a, d_table order by 1;")
        contains "(mvljc)"
    }
    qt_select_mv "SELECT a.k1 + 1 FROM ( SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1 ) a, d_table order by 1;"

    sql """set enable_stats=true;"""
    explain {
        sql("SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1 order by 1,2;")
        contains "(mvljc)"
    }

    explain {
        sql("SELECT a.k1 + 1 FROM ( SELECT k1, SUM(k2) FROM d_table WHERE k3 < 2 or k2 < 0 GROUP BY k1 ) a, d_table order by 1;")
        contains "(mvljc)"
    }
}
