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

suite("test_hive_other", "p0") {

    def q01 = {
        qt_q24 """ select name, count(1) as c from student group by name order by c desc;"""
        qt_q25 """ select lo_orderkey, count(1) as c from lineorder group by lo_orderkey order by c desc;"""
        qt_q26 """ select * from test1 order by col_1;"""
        qt_q27 """ select * from string_table order by p_partkey desc;"""
        qt_q28 """ select * from account_fund order by batchno;"""
        qt_q29 """ select * from sale_table order by bill_code limit 01;"""
        qt_q30 """ select count(card_cnt) from hive01;"""
        qt_q31 """ select * from test2 order by id;"""
        qt_q32 """ select * from test_hive_doris order by id;"""

        order_qt_q33 """ select dt, * from table_with_vertical_line order by dt desc limit 10;"""
        order_qt_q34 """ select dt, k2 from table_with_vertical_line order by k2 desc limit 10;"""
        order_qt_q35 """ select dt, k2 from table_with_vertical_line where dt='2022-11-24' order by k2 desc limit 10;"""
        order_qt_q36 """ select k2, k5 from table_with_vertical_line where dt='2022-11-25' order by k2 desc limit 10;"""
        order_qt_q37 """ select count(*) from table_with_vertical_line;"""
        order_qt_q38 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25') order by k2 desc limit 10;"""
        order_qt_q39 """ select k2, k5 from table_with_vertical_line where dt in ('2022-11-25', '2022-11-24') order by k2 desc limit 10;"""
        order_qt_q40 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-25') order by k2 desc limit 10;"""
        order_qt_q41 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') and dt in ('2022-11-24') order by k2 desc limit 10;"""
        order_qt_q42 """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-24') order by k2 desc limit 10;"""

        order_qt_q43 """ select dt, * from table_with_x01 order by dt desc limit 10;"""
        order_qt_q44 """ select dt, k2 from table_with_x01 order by k2 desc limit 10;"""
        order_qt_q45 """ select dt, k2 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        order_qt_q46 """ select k2, k5 from table_with_x01 where dt='2022-11-10' order by k2 desc limit 10;"""
        order_qt_q47 """ select count(*) from table_with_x01;"""
        order_qt_q48 """ select k2, k5 from table_with_x01 where dt in ('2022-11-25') order by k2 desc limit 10;"""
        order_qt_q49 """ select k2, k5 from table_with_x01 where dt in ('2022-11-10', '2022-11-10') order by k2 desc limit 10;"""
        order_qt_q50 """ select dt, dt, k2, k5, dt from table_with_x01 where dt in ('2022-11-10') or dt in ('2022-11-10') order by k2 desc limit 10;"""
    }


    def set_be_config = { ->
        String[][] backends = sql """ show backends; """
        assertTrue(backends.size() > 0)
        for (String[] backend in backends) {
            // No need to set this config anymore, but leave this code sample here
            // StringBuilder setConfigCommand = new StringBuilder();
            // setConfigCommand.append("curl -X POST http://")
            // setConfigCommand.append(backend[2])
            // setConfigCommand.append(":")
            // setConfigCommand.append(backend[5])
            // setConfigCommand.append("/api/update_config?")
            // String command1 = setConfigCommand.toString() + "enable_new_load_scan_node=true"
            // logger.info(command1)
            // String command2 = setConfigCommand.toString() + "enable_new_file_scanner=true"
            // logger.info(command2)
            // def process1 = command1.execute()
            // int code = process1.waitFor()
            // assertEquals(code, 0)
            // def process2 = command2.execute()
            // code = process1.waitFor()
            // assertEquals(code, 0)
        }
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        set_be_config.call()

        sql """admin set frontend config ("enable_multi_catalog" = "true")"""
        sql """drop catalog if exists hive"""
        sql """
            create catalog hive properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://127.0.0.1:${hms_port}'
            );
            """
        sql """switch hive"""
        sql """use `default`"""
        // order_qt_show_tables """show tables"""

        q01()

        sql """refresh catalog hive"""
        q01()
        sql """refresh database `default`"""
        // order_qt_show_tables2 """show tables"""
        q01()
        sql """refresh table `default`.table_with_vertical_line"""
        order_qt_after_refresh """ select dt, dt, k2, k5, dt from table_with_vertical_line where dt in ('2022-11-25') or dt in ('2022-11-24') order by k2 desc limit 10;"""        
    }
}




