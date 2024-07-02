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

suite ("multi_slot") {
    sql """ DROP TABLE IF EXISTS multi_slot; """

    sql """
            create table multi_slot(
                k int null,
                v variant null
            )
            duplicate key (k)
            distributed BY hash(k) buckets 3
            properties("replication_num" = "1");
        """

    sql """insert into multi_slot select 1,'{"k1" : 1, "k2" : 1, "k3" : "a"}';"""
    sql """insert into multi_slot select 2,'{"k1" : 2, "k2" : 2, "k3" : "b"}';"""
    sql """insert into multi_slot select 3,'{"k1" : -3, "k2" : null, "k3" : "c"}';"""
    sql """insert into multi_slot select 4,'{"k1" : -3, "k2" : null, "k4" : {"k44" : 456}}';"""
    order_qt_select_star "select abs(cast(v['k4']['k44'] as int)), sum(abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3) from multi_slot group by abs(cast(v['k4']['k44'] as int))"

    createMV ("create materialized view k1a2p2ap3p as select abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3 from multi_slot;")

    createMV("create materialized view k1a2p2ap3ps as select abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,sum(abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3) from multi_slot group by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1;")

    createMV("create materialized view k1a2p2ap3psp as select abs(cast(v['k4']['k44'] as int)), sum(abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3) from multi_slot group by abs(cast(v['k4']['k44'] as int));")

    sql """insert into multi_slot select -4,'{"k1" : -4, "k2" : -4, "k3" : "d"}';"""
    sql """insert into multi_slot select -5,'{"k1" : -4, "k2" : -4, "k4" : "d"}';"""
    sql """insert into multi_slot select -6,'{"k1" : -4, "k2" : -4, "k4" : {"k44" : 789}}';"""

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"


    order_qt_select_star "select abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3 from multi_slot;"
    order_qt_select_star "select * from multi_slot order by cast(v['k1'] as int);"
    // TODO fix and remove enable_rewrite_element_at_to_slot
    order_qt_select_star "select /*+SET_VAR(enable_rewrite_element_at_to_slot=false) */ abs(cast(v['k4']['k44'] as int)), sum(abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3) from multi_slot group by abs(cast(v['k4']['k44'] as int))"

    // def retry_times = 60
    // for (def i = 0; i < retry_times; ++i) {
    //     boolean is_k1a2p2ap3p = false
    //     boolean is_k1a2p2ap3ps = false
    //     boolean is_d_table = false
    //     explain {
    //         sql("select  /*+SET_VAR(enable_rewrite_element_at_to_slot=false) */ abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,sum(abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3) from multi_slot group by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1 order by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1")
    //         check { explainStr, ex, startTime, endTime ->
    //             if (ex != null) {
    //                 throw ex;
    //             }
    //             logger.info("explain result: ${explainStr}".toString())
    //             is_k1a2p2ap3p = explainStr.contains"(k1a2p2ap3p)"
    //             is_k1a2p2ap3ps = explainStr.contains("(k1a2p2ap3ps)")
    //             is_d_table = explainStr.contains("(multi_slot)")
    //             assert is_k1a2p2ap3p || is_k1a2p2ap3ps || is_d_table
    //         }
    //     }
    //     // FIXME: the mv selector maybe select base table forever when exist multi mv,
    //     //        so this pr just treat as success if select base table.
    //     //        we should remove is_d_table in the future
    //     if (is_d_table || is_k1a2p2ap3p || is_k1a2p2ap3ps) {
    //         break
    //     }
    //     if (i + 1 == retry_times) {
    //         throw new IllegalStateException("retry and failed too much")
    //     }
    //     sleep(1000)
    // }
    // order_qt_select_mv "select  /*+SET_VAR(enable_rewrite_element_at_to_slot=false) */ abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,sum(abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3) from multi_slot group by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1 order by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1;"

    // explain {
    //     sql("select abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3 from multi_slot order by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3")
    //     contains "(k1a2p2ap3p)"
    // }
    // order_qt_select_mv "select abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3 from multi_slot order by abs(cast(v['k1'] as int))+cast(v['k2'] as int)+1,abs(cast(v['k2'] as int)+2)+cast(v['k3'] as int)+3;"

}
