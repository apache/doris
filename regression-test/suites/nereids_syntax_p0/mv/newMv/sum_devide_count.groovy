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

suite ("sum_devide_count") {
    sql """ DROP TABLE IF EXISTS sum_devide_count; """

    sql """
            create table sum_devide_count(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into sum_devide_count select 1,1,1,'a';"
    sql "insert into sum_devide_count select 2,2,2,'b';"
    sql "insert into sum_devide_count select 3,-3,null,'c';"

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    createMV ("create materialized view kavg as select k1,k4,sum(k2),count(k2) from sum_devide_count group by k1,k4;")

    sleep(3000)

    sql "insert into sum_devide_count select -4,-4,-4,'d';"
    sql "insert into sum_devide_count select 3,2,null,'c';"

    sql "analyze table sum_devide_count with sync;"
    sql """set enable_stats=false;"""

    qt_select_star "select * from sum_devide_count order by k1,k2,k3,k4;"

    explain {
        sql("select k1,k4,sum(k2)/count(k2) from sum_devide_count group by k1,k4 order by k1,k4;")
        contains "(kavg)"
    }
    order_qt_select_mv "select k1,k4,sum(k2)/count(k2) from sum_devide_count group by k1,k4 order by k1,k4;"

    explain {
        sql("select k1,sum(k2)/count(k2) from sum_devide_count group by k1 order by k1;")
        contains "(kavg)"
    }
    order_qt_select_mv "select k1,sum(k2)/count(k2) from sum_devide_count group by k1 order by k1;"

    explain {
        sql("select k4,sum(k2)/count(k2) from sum_devide_count group by k4 order by k4;")
        contains "(kavg)"
    }
    order_qt_select_mv "select k4,sum(k2)/count(k2) from sum_devide_count group by k4 order by k4;"

    explain {
        sql("select sum(k2)/count(k2) from sum_devide_count;")
        contains "(kavg)"
    }
    order_qt_select_mv "select sum(k2)/count(k2) from sum_devide_count;"

    sql """set enable_stats=true;"""

    explain {
        sql("select k1,k4,sum(k2)/count(k2) from sum_devide_count group by k1,k4 order by k1,k4;")
        contains "(kavg)"
    }

    explain {
        sql("select k1,sum(k2)/count(k2) from sum_devide_count group by k1 order by k1;")
        contains "(kavg)"
    }

    explain {
        sql("select k4,sum(k2)/count(k2) from sum_devide_count group by k4 order by k4;")
        contains "(kavg)"
    }

    explain {
        sql("select sum(k2)/count(k2) from sum_devide_count;")
        contains "(kavg)"
    }
}
