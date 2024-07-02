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

suite ("k123p") {
    sql """set enable_nereids_planner=true;"""
    sql """ DROP TABLE IF EXISTS d_table; """

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

    createMV ("""create materialized view k123p1w as select k1,k2+k3 from d_table where k1 = 1;""")
    createMV ("""create materialized view k123p4w as select k1,k2+k3 from d_table where k4 = "b";""")

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"

    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    qt_select_star "select * from d_table order by k1;"

    explain {
        sql("select k1,k2+k3 from d_table order by k1;")
        contains "(d_table)"
    }
    qt_select_mv "select k1,k2+k3 from d_table order by k1;"

    explain {
        sql("select k1,k2+k3 from d_table where k1 = 1 order by k1;")
        contains "(k123p1w)"
    }
    qt_select_mv "select k1,k2+k3 from d_table where k1 = 1 order by k1;"

    explain {
        sql("select k1,k2+k3 from d_table where k1 = 2 order by k1;")
        contains "(d_table)"
    }
    qt_select_mv "select k1,k2+k3 from d_table where k1 = 2 order by k1;"

    explain {
        sql("select k1,k2+k3 from d_table where k1 = '1' order by k1;")
        contains "(k123p1w)"
    }
    qt_select_mv "select k1,k2+k3 from d_table where k1 = '1' order by k1;"

    explain {
        sql("select k1,k2+k3 from d_table where k4 = 'b' order by k1;")
        contains "(k123p4w)"
    }
    qt_select_mv "select k1,k2+k3 from d_table where k4 = 'b' order by k1;"

    explain {
        sql("select k1,k2+k3 from d_table where k4 = 'a' order by k1;")
        contains "(d_table)"
    }
    qt_select_mv "select k1,k2+k3 from d_table where k4 = 'a' order by k1;"

    explain {
        sql("""select k1,k2+k3 from d_table where k1 = 2 and k4 = "b";""")
        contains "(k123p4w)"
    }
    qt_select_mv """select k1,k2+k3 from d_table where k1 = 2 and k4 = "b" order by k1;"""

    qt_select_mv_constant """select bitmap_empty() from d_table where true;"""

    sql """set enable_stats=true;"""
    explain {
        sql("select k1,k2+k3 from d_table order by k1;")
        contains "(d_table)"
    }

    explain {
        sql("select k1,k2+k3 from d_table where k1 = 1 order by k1;")
        contains "(k123p1w)"
    }

    explain {
        sql("select k1,k2+k3 from d_table where k1 = 2 order by k1;")
        contains "(d_table)"
    }

    explain {
        sql("select k1,k2+k3 from d_table where k1 = '1' order by k1;")
        contains "(k123p1w)"
    }

    explain {
        sql("select k1,k2+k3 from d_table where k4 = 'b' order by k1;")
        contains "(k123p4w)"
    }

    explain {
        sql("select k1,k2+k3 from d_table where k4 = 'a' order by k1;")
        contains "(d_table)"
    }

    explain {
        sql("""select k1,k2+k3 from d_table where k1 = 2 and k4 = "b";""")
        contains "(k123p4w)"
    }
}
