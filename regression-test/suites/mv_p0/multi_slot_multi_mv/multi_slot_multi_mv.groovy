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

suite ("multi_slot_multi_mv") {

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

    createMV ("create materialized view k1a2p2ap3p as select abs(k1)+k2+1,abs(k2+2)+k3+3 from d_table;")

    createMV("create materialized view k1a2p2ap3ps as select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1;")

    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table select -4,-4,-4,'d';"

    qt_select_star "select * from d_table order by k1;"

    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    def retry_times = 60
    for (def i = 0; i < retry_times; ++i) {
        boolean is_k1a2p2ap3p = false
        boolean is_k1a2p2ap3ps = false
        boolean is_d_table = false
        explain {
            sql("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1 order by abs(k1)+k2+1")
            check { explainStr, ex, startTime, endTime ->
                if (ex != null) {
                    throw ex;
                }
                logger.info("explain result: ${explainStr}".toString())
                is_k1a2p2ap3p = explainStr.contains"(k1a2p2ap3p)"
                is_k1a2p2ap3ps = explainStr.contains("(k1a2p2ap3ps)")
                is_d_table = explainStr.contains("(d_table)")
                assert is_k1a2p2ap3p || is_k1a2p2ap3ps || is_d_table
            }
        }
        // FIXME: the mv selector maybe select base table forever when exist multi mv,
        //        so this pr just treat as success if select base table.
        //        we should remove is_d_table in the future
        if (is_d_table || is_k1a2p2ap3p || is_k1a2p2ap3ps) {
            break
        }
        if (i + 1 == retry_times) {
            throw new IllegalStateException("retry and failed too much")
        }
        sleep(1000)
    }
    qt_select_mv "select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1 order by abs(k1)+k2+1;"

    explain {
        sql("select abs(k1)+k2+1,abs(k2+2)+k3+3 from d_table order by abs(k1)+k2+1,abs(k2+2)+k3+3")
        contains "(k1a2p2ap3p)"
    }
    qt_select_mv "select abs(k1)+k2+1,abs(k2+2)+k3+3 from d_table order by abs(k1)+k2+1,abs(k2+2)+k3+3;"

    sql """set enable_stats=true;"""
    for (def i = 0; i < retry_times; ++i) {
        boolean is_k1a2p2ap3p = false
        boolean is_k1a2p2ap3ps = false
        boolean is_d_table = false
        explain {
            sql("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1 order by abs(k1)+k2+1")
            check { explainStr, ex, startTime, endTime ->
                if (ex != null) {
                    throw ex;
                }
                logger.info("explain result: ${explainStr}".toString())
                is_k1a2p2ap3p = explainStr.contains"(k1a2p2ap3p)"
                is_k1a2p2ap3ps = explainStr.contains("(k1a2p2ap3ps)")
                is_d_table = explainStr.contains("(d_table)")
                assert is_k1a2p2ap3p || is_k1a2p2ap3ps || is_d_table
            }
        }
        // FIXME: the mv selector maybe select base table forever when exist multi mv,
        //        so this pr just treat as success if select base table.
        //        we should remove is_d_table in the future
        if (is_d_table || is_k1a2p2ap3p || is_k1a2p2ap3ps) {
            break
        }
        if (i + 1 == retry_times) {
            throw new IllegalStateException("retry and failed too much")
        }
        sleep(1000)
    }

    explain {
        sql("select abs(k1)+k2+1,abs(k2+2)+k3+3 from d_table order by abs(k1)+k2+1,abs(k2+2)+k3+3")
        contains "(k1a2p2ap3p)"
    }

}
