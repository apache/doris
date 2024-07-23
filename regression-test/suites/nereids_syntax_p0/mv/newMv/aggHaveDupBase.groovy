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

suite ("agg_have_dup_base") {

    def tbName1 = "agg_have_dup_base"
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    sql """ DROP TABLE IF EXISTS agg_have_dup_base; """
    sql """
            create table agg_have_dup_base(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into agg_have_dup_base select 1,1,1,'a';"
    sql "insert into agg_have_dup_base select 2,2,2,'b';"
    sql "insert into agg_have_dup_base select 3,-3,null,'c';"
    sql "insert into agg_have_dup_base select 3,-3,null,'c';"

    createMV( "create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;")

    sleep(3000)


    sql "insert into agg_have_dup_base select -4,-4,-4,'d';"

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table agg_have_dup_base with sync;"
    sql """set enable_stats=false;"""


    order_qt_select_star "select * from agg_have_dup_base order by k1;"

    explain {
        sql("select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;")
        contains "(k12s3m)"
    }
    order_qt_select_mv "select k1,sum(k2),max(k2) from agg_have_dup_base group by k1 order by k1;"

    explain {
        sql("select k1,sum(k2) from agg_have_dup_base group by k1;")
        contains "(k12s3m)"
    }
    order_qt_select_mv "select k1,sum(k2) from agg_have_dup_base group by k1 order by k1;"

    explain {
        sql("select k1,max(k2) from agg_have_dup_base group by k1;")
        contains "(k12s3m)"
    }
    order_qt_select_mv "select k1,max(k2) from agg_have_dup_base group by k1 order by k1;"

    explain {
        sql("select unix_timestamp(k1) tmp,sum(k2) from agg_have_dup_base group by tmp;")
        contains "(k12s3m)"
    }
    order_qt_select_mv "select unix_timestamp(k1) tmp,sum(k2) from agg_have_dup_base group by tmp order by tmp;"

    sql """set enable_stats=true;"""

    explain {
        sql("select k1,sum(k2),max(k2) from agg_have_dup_base group by k1;")
        contains "(k12s3m)"
    }

    explain {
        sql("select k1,sum(k2) from agg_have_dup_base group by k1;")
        contains "(k12s3m)"
    }

    explain {
        sql("select k1,max(k2) from agg_have_dup_base group by k1;")
        contains "(k12s3m)"
    }

    explain {
        sql("select unix_timestamp(k1) tmp,sum(k2) from agg_have_dup_base group by tmp;")
        contains "(k12s3m)"
    }
}
