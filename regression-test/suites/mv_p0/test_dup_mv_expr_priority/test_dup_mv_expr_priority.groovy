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

suite ("test_dup_mv_expr_priority") {
    sql """ DROP TABLE IF EXISTS table_ngrambf;"""

    sql """
            CREATE TABLE table_ngrambf (
            `siteid` int(11) NULL DEFAULT "10" COMMENT "",
            `citycode` smallint(6) NULL COMMENT "",
            `username` varchar(32) NULL DEFAULT "" COMMENT "",
            INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256") COMMENT 'username ngram_bf index'
            ) ENGINE=OLAP
            duplicate KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
            DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
            properties("replication_num" = "1");
        """

    sql """insert into table_ngrambf values(1,1,"123_abc_中国");"""

    createMV ("""create materialized view test_mv_1 as select element_at(split_by_string(username,"_"),1) ,element_at(split_by_string(username,"_"),2) ,element_at(split_by_string(username,"_"),3)  ,siteid,citycode from table_ngrambf ;""")

    sql """insert into table_ngrambf values(1,1,"123_def_美国");"""

    sql """analyze table table_ngrambf with sync;"""
    sql """set enable_stats=false;"""

    explain {
        sql("""select  element_at(split_by_string(username,"_"),1) ,element_at(split_by_string(username,"_"),2) ,element_at(split_by_string(username,"_"),3)  ,siteid,citycode from table_ngrambf order by citycode;""")
        contains "(test_mv_1)"
    }
    qt_select_mv """select  element_at(split_by_string(username,"_"),1) ,element_at(split_by_string(username,"_"),2) ,element_at(split_by_string(username,"_"),3)  ,siteid,citycode from table_ngrambf order by citycode;"""

    sql """set enable_stats=true;"""
    explain {
        sql("""select  element_at(split_by_string(username,"_"),1) ,element_at(split_by_string(username,"_"),2) ,element_at(split_by_string(username,"_"),3)  ,siteid,citycode from table_ngrambf order by citycode;""")
        contains "(test_mv_1)"
    }

}
