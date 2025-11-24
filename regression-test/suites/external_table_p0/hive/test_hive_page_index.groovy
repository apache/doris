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

suite("test_hive_page_index", "p0,external,hive,external_docker,external_docker_hive") {


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String catalog_name = "${hivePrefix}_test_hive_page_index"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
    
        sql """switch ${catalog_name}"""
        sql """ use `default` """ 

        order_qt_q1 """select * from alltypes_tiny_pages_plain where id < 30 or id > 7270 and bigint_col = 20;"""
        order_qt_q2 """select * from alltypes_tiny_pages_plain where id > 2300 or id < 2310 and bigint_col = 20 ;"""
        order_qt_q3 """select * from alltypes_tiny_pages_plain where id < 100 or month = 7 or year = 2010;"""
        order_qt_q4 """select * from alltypes_tiny_pages_plain where bigint_col = 0 or month = 7;"""
        order_qt_q5 """select * from alltypes_tiny_pages_plain where bigint_col = 0 or year = 2010;"""
        order_qt_q6 """select * from alltypes_tiny_pages_plain where bigint_col = 0 or month = 7 or year = 2011;"""
        order_qt_q7 """select * from alltypes_tiny_pages_plain where date_string_col = '02/02/09' or month = 1;"""
        order_qt_q8 """select * from alltypes_tiny_pages_plain where date_string_col = '02/02/09' or year = 2010;"""
        order_qt_q9 """select * from alltypes_tiny_pages_plain where timestamp_col < '2009-12-08 00:19:03' or smallint_col = 9;"""
        order_qt_q10 """select * from alltypes_tiny_pages_plain where (double_col > 70 and double_col < 71) or ( month = 8 and year = 2010 and bigint_col = 0);"""



        order_qt_q13 """select * from decimals_1_10 where d_1 = 1 or d_10 = 2;"""
        order_qt_q14 """select * from decimals_1_10 where d_1 = 3 or d_10 = 4;"""
        order_qt_q15 """select * from decimals_1_10 where d_1 = 5 or d_10 = 6;"""
        order_qt_q16 """select * from decimals_1_10 where d_1 = 9 or d_10 = 10;"""

        order_qt_q17 """select * from decimals_1_10 where d_1 < 3 or d_10 > 7;"""
        order_qt_q18 """select * from decimals_1_10 where d_1 > 2 or d_10 < 5;"""
        order_qt_q19 """select * from decimals_1_10 where d_1 > 5 or d_10 < 3;"""

        order_qt_q20 """select * from decimals_1_10 where d_1 = 2 or d_10 = 6 or d_1 = 8;"""
        order_qt_q21 """select * from decimals_1_10 where (d_1 > 5 and d_10 < 9) or d_1 = 1;"""
        order_qt_q22 """select * from decimals_1_10 where (d_1 > 2 and d_10 < 5) or (d_1 > 7);"""


            
            
        order_qt_q23 """select * from alltypes_tiny_pages where id < 30 or id > 7270 and bigint_col = 20;"""
        order_qt_q24 """select * from alltypes_tiny_pages where id > 2300 or id < 2310;"""
        order_qt_q25 """select * from alltypes_tiny_pages where id < 100 or month = 7 or year = 2010;"""
        order_qt_q26 """select * from alltypes_tiny_pages where bigint_col = 0 or month = 7;"""
        order_qt_q27 """select * from alltypes_tiny_pages where bigint_col = 0 or year = 2010;"""
        order_qt_q28 """select * from alltypes_tiny_pages where bigint_col = 0 or month = 7 or year = 2011;"""
        order_qt_q29 """select * from alltypes_tiny_pages where date_string_col = '02/02/09' or month = 1;"""
        order_qt_q30 """select * from alltypes_tiny_pages where timestamp_col < '2009-12-08 00:19:03' or smallint_col = 9;"""
        order_qt_q31 """select * from alltypes_tiny_pages where date_string_col = '02/02/09' or year = 2010;"""
        order_qt_q32 """select * from alltypes_tiny_pages where (double_col > 70 and double_col < 71) or ( month = 8 and year = 2010 and bigint_col = 0);"""


        order_qt_q33 """ select * from decimals_1_10 where d_1 is null or d_10 is null ; """
        order_qt_q34 """ select * from decimals_1_10 where d_1 is null"""
        order_qt_q35 """ select * from decimals_1_10 where d_10 is null ; """

    
    }








}
