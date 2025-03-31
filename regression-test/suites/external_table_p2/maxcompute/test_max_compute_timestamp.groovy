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



/*


drop table if EXISTS datetime_tb1;
CREATE TABLE datetime_tb1  (col1 datetime);
INSERT INTO TABLE datetime_tb1 VALUES(datetime "2023-02-02 00:00:00");

drop table if EXISTS timestamp_tb1;
CREATE TABLE timestamp_tb1  (col1 TIMESTAMP,col2 TIMESTAMP_NTZ);
INSERT INTO TABLE timestamp_tb1 VALUES(timestamp "2023-02-02 00:00:00.123456789", timestamp_ntz "2023-02-02 00:00:00.123456789");

drop table if EXISTS timestamp_tb2;
CREATE TABLE  timestamp_tb2  (col1 TIMESTAMP,col2 TIMESTAMP_NTZ);
INSERT INTO TABLE timestamp_tb2 VALUES(timestamp "2023-02-02 00:00:00.123456", timestamp_ntz "2023-02-02 00:00:00.123456" );
*/

suite("test_max_compute_timestamp", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {


    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk");
        String mc_db = "mc_datalake"
        String mc_catalog_name = "test_max_compute_timestamp"

        sql """drop catalog if exists ${mc_catalog_name};"""
        sql """
            create catalog if not exists ${mc_catalog_name} properties (
                "type" = "max_compute",
                "mc.default.project" = "${mc_db}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.datetime_predicate_push_down" = "false"
            );
        """
        sql """ switch ${mc_catalog_name} """
        sql """ use ${mc_db}"""

        sql """ set time_zone = "Asia/Shanghai" """
        qt_1_1 """ select * from datetime_tb1;"""
        qt_1_2 """ select * from datetime_tb1 where col1 > "2023-02-02 00:00:00.000";"""
        qt_1_3 """ select * from datetime_tb1 where col1 >= "2023-02-02 00:00:00.000";"""
        qt_1_4 """ select * from datetime_tb1 where col1 = "2023-02-02 00:00:00.000";"""
        qt_1_5 """ select * from datetime_tb1 where col1 <= "2023-02-02 00:00:00.000";"""
        qt_1_6 """ select * from datetime_tb1 where col1 < "2023-02-02 00:00:00.000";"""
        qt_1_7 """ select * from datetime_tb1 where col1 != "2023-02-02 00:00:00.000";"""


        qt_2_1 """select * from timestamp_tb1;"""
        qt_2_2 """select * from timestamp_tb1 where col1 > "2023-02-02 00:00:00.123456";"""
        qt_2_3 """select * from timestamp_tb1 where col1 >= "2023-02-02 00:00:00.123456";"""
        qt_2_4 """select * from timestamp_tb1 where col1 = "2023-02-02 00:00:00.123456";"""
        qt_2_5 """select * from timestamp_tb1 where col1 <= "2023-02-02 00:00:00.123456";"""
        qt_2_6 """select * from timestamp_tb1 where col1 < "2023-02-02 00:00:00.123456";"""
        qt_2_7 """select * from timestamp_tb1 where col1 != "2023-02-02 00:00:00.123456"; """
        
        qt_2_8 """ select * from timestamp_tb1 where col2 > "2023-02-02 00:00:00.123456"; """ 
        qt_2_9 """ select * from timestamp_tb1 where col2 >= "2023-02-02 00:00:00.123456"; """ 
        qt_2_10 """ select * from timestamp_tb1 where col2 = "2023-02-02 00:00:00.123456"; """ 
        qt_2_11 """ select * from timestamp_tb1 where col2 <= "2023-02-02 00:00:00.123456"; """ 
        qt_2_12 """ select * from timestamp_tb1 where col2 < "2023-02-02 00:00:00.123456"; """ 
        qt_2_13 """ select * from timestamp_tb1 where col2 != "2023-02-02 00:00:00.123456"; """ 



        qt_3_1 """ select * from timestamp_tb2;"""
        qt_3_2 """ select * from timestamp_tb2 where col1 > "2023-02-02 00:00:00.123456";"""
        qt_3_3 """ select * from timestamp_tb2 where col1 >= "2023-02-02 00:00:00.123456";"""
        qt_3_4 """ select * from timestamp_tb2 where col1 = "2023-02-02 00:00:00.123456";"""
        qt_3_5 """ select * from timestamp_tb2 where col1 <= "2023-02-02 00:00:00.123456";"""
        qt_3_6 """ select * from timestamp_tb2 where col1 < "2023-02-02 00:00:00.123456";"""
        qt_3_7 """ select * from timestamp_tb2 where col1 != "2023-02-02 00:00:00.123456";"""
        qt_3_8 """ select * from timestamp_tb2 where col2 > "2023-02-02 00:00:00.123456";"""
        qt_3_9 """ select * from timestamp_tb2 where col2 >= "2023-02-02 00:00:00.123456";"""
        qt_3_10 """ select * from timestamp_tb2 where col2 = "2023-02-02 00:00:00.123456";"""
        qt_3_11 """ select * from timestamp_tb2 where col2 <= "2023-02-02 00:00:00.123456";"""
        qt_3_12 """ select * from timestamp_tb2 where col2 < "2023-02-02 00:00:00.123456";"""
        qt_3_13 """ select * from timestamp_tb2 where col2 != "2023-02-02 00:00:00.123456";"""


        sql """ set time_zone = "UTC" """

        qt_4_1 """ select * from datetime_tb1;"""
        qt_4_2 """ select * from datetime_tb1 where col1 > "2023-02-01 16:00:00.000";"""
        qt_4_3 """ select * from datetime_tb1 where col1 >= "2023-02-01 16:00:00.000";"""
        qt_4_4 """ select * from datetime_tb1 where col1 = "2023-02-01 16:00:00.000";"""
        qt_4_5 """ select * from datetime_tb1 where col1 <= "2023-02-01 16:00:00.000";"""
        qt_4_6 """ select * from datetime_tb1 where col1 < "2023-02-01 16:00:00.000";"""
        qt_4_7 """ select * from datetime_tb1 where col1 != "2023-02-01 16:00:00.000";"""


        qt_5_1 """select * from timestamp_tb1;"""
        qt_5_2 """select * from timestamp_tb1 where col1 > "2023-02-01 16:00:00.123456";"""
        qt_5_3 """select * from timestamp_tb1 where col1 >= "2023-02-01 16:00:00.123456";"""
        qt_5_4 """select * from timestamp_tb1 where col1 = "2023-02-01 16:00:00.123456";"""
        qt_5_5 """select * from timestamp_tb1 where col1 <= "2023-02-01 16:00:00.123456";"""
        qt_5_6 """select * from timestamp_tb1 where col1 < "2023-02-01 16:00:00.123456";"""
        qt_5_7 """select * from timestamp_tb1 where col1 != "2023-02-01 16:00:00.123456"; """
        
        qt_5_8 """ select * from timestamp_tb1 where col2 > "2023-02-02 00:00:00.123456"; """ 
        qt_5_9 """ select * from timestamp_tb1 where col2 >= "2023-02-02 00:00:00.123456"; """ 
        qt_5_10 """ select * from timestamp_tb1 where col2 = "2023-02-02 00:00:00.123456"; """ 
        qt_5_11 """ select * from timestamp_tb1 where col2 <= "2023-02-02 00:00:00.123456"; """ 
        qt_5_12 """ select * from timestamp_tb1 where col2 < "2023-02-02 00:00:00.123456"; """ 
        qt_5_13 """ select * from timestamp_tb1 where col2 != "2023-02-02 00:00:00.123456"; """ 

        
        


        qt_6_1 """ select * from timestamp_tb2;"""
        qt_6_2 """ select * from timestamp_tb2 where col1 > "2023-02-01 16:00:00.123456";"""
        qt_6_3 """ select * from timestamp_tb2 where col1 >= "2023-02-01 16:00:00.123456";"""
        qt_6_4 """ select * from timestamp_tb2 where col1 = "2023-02-01 16:00:00.123456";"""
        qt_6_5 """ select * from timestamp_tb2 where col1 <= "2023-02-01 16:00:00.123456";"""
        qt_6_6 """ select * from timestamp_tb2 where col1 < "2023-02-01 16:00:00.123456";"""
        qt_6_7 """ select * from timestamp_tb2 where col1 != "2023-02-01 16:00:00.123456";"""
    
        qt_6_8 """ select * from timestamp_tb1 where col2 > "2023-02-02 00:00:00.123456"; """ 
        qt_6_9 """ select * from timestamp_tb1 where col2 >= "2023-02-02 00:00:00.123456"; """ 
        qt_6_10 """ select * from timestamp_tb1 where col2 = "2023-02-02 00:00:00.123456"; """ 
        qt_6_11 """ select * from timestamp_tb1 where col2 <= "2023-02-02 00:00:00.123456"; """ 
        qt_6_12 """ select * from timestamp_tb1 where col2 < "2023-02-02 00:00:00.123456"; """ 
        qt_6_13 """ select * from timestamp_tb1 where col2 != "2023-02-02 00:00:00.123456"; """ 

        

        sql """drop catalog if exists ${mc_catalog_name}_2;"""
        sql """
            create catalog if not exists ${mc_catalog_name}_2 properties (
                "type" = "max_compute",
                "mc.default.project" = "${mc_db}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.datetime_predicate_push_down" = "true"
            );
        """
        sql """ switch ${mc_catalog_name}_2 """
        sql """ use ${mc_db}"""


        sql """ set time_zone = "Asia/Shanghai" """
        qt_7_1 """ select * from datetime_tb1;"""
        qt_7_2 """ select * from datetime_tb1 where col1 > "2023-02-02 00:00:00.000";"""
        qt_7_3 """ select * from datetime_tb1 where col1 >= "2023-02-02 00:00:00.000";"""
        qt_7_4 """ select * from datetime_tb1 where col1 = "2023-02-02 00:00:00.000";"""
        qt_7_5 """ select * from datetime_tb1 where col1 <= "2023-02-02 00:00:00.000";"""
        qt_7_6 """ select * from datetime_tb1 where col1 < "2023-02-02 00:00:00.000";"""
        qt_7_7 """ select * from datetime_tb1 where col1 != "2023-02-02 00:00:00.000";"""


        qt_8_1 """ select * from timestamp_tb2;"""
        qt_8_2 """ select * from timestamp_tb2 where col1 > "2023-02-02 00:00:00.123456";"""
        qt_8_3 """ select * from timestamp_tb2 where col1 >= "2023-02-02 00:00:00.123456";"""
        qt_8_4 """ select * from timestamp_tb2 where col1 = "2023-02-02 00:00:00.123456";"""
        qt_8_5 """ select * from timestamp_tb2 where col1 <= "2023-02-02 00:00:00.123456";"""
        qt_8_6 """ select * from timestamp_tb2 where col1 < "2023-02-02 00:00:00.123456";"""
        qt_8_7 """ select * from timestamp_tb2 where col1 != "2023-02-02 00:00:00.123456";"""
        qt_8_8 """ select * from timestamp_tb2 where col2 > "2023-02-02 00:00:00.123456";"""
        qt_8_9 """ select * from timestamp_tb2 where col2 >= "2023-02-02 00:00:00.123456";"""
        qt_8_10 """ select * from timestamp_tb2 where col2 = "2023-02-02 00:00:00.123456";"""
        qt_8_11 """ select * from timestamp_tb2 where col2 <= "2023-02-02 00:00:00.123456";"""
        qt_8_12 """ select * from timestamp_tb2 where col2 < "2023-02-02 00:00:00.123456";"""
        qt_8_13 """ select * from timestamp_tb2 where col2 != "2023-02-02 00:00:00.123456";"""

        sql """ set time_zone = "UTC" """

        qt_9_1 """ select * from datetime_tb1;"""
        qt_9_2 """ select * from datetime_tb1 where col1 > "2023-02-01 16:00:00.000";"""
        qt_9_3 """ select * from datetime_tb1 where col1 >= "2023-02-01 16:00:00.000";"""
        qt_9_4 """ select * from datetime_tb1 where col1 = "2023-02-01 16:00:00.000";"""
        qt_9_5 """ select * from datetime_tb1 where col1 <= "2023-02-01 16:00:00.000";"""
        qt_9_6 """ select * from datetime_tb1 where col1 < "2023-02-01 16:00:00.000";"""
        qt_9_7 """ select * from datetime_tb1 where col1 != "2023-02-01 16:00:00.000";"""



        qt_10_1 """ select * from timestamp_tb2;"""
        qt_10_2 """ select * from timestamp_tb2 where col1 > "2023-02-01 16:00:00.123456";"""
        qt_10_3 """ select * from timestamp_tb2 where col1 >= "2023-02-01 16:00:00.123456";"""
        qt_10_4 """ select * from timestamp_tb2 where col1 = "2023-02-01 16:00:00.123456";"""
        qt_10_5 """ select * from timestamp_tb2 where col1 <= "2023-02-01 16:00:00.123456";"""
        qt_10_6 """ select * from timestamp_tb2 where col1 < "2023-02-01 16:00:00.123456";"""
        qt_10_7 """ select * from timestamp_tb2 where col1 != "2023-02-01 16:00:00.123456";"""
    
        qt_10_8 """ select * from timestamp_tb1 where col2 > "2023-02-02 00:00:00.123456"; """ 
        qt_10_9 """ select * from timestamp_tb1 where col2 >= "2023-02-02 00:00:00.123456"; """ 
        qt_10_10 """ select * from timestamp_tb1 where col2 = "2023-02-02 00:00:00.123456"; """ 
        qt_10_11 """ select * from timestamp_tb1 where col2 <= "2023-02-02 00:00:00.123456"; """ 
        qt_10_12 """ select * from timestamp_tb1 where col2 < "2023-02-02 00:00:00.123456"; """ 
        qt_10_13 """ select * from timestamp_tb1 where col2 != "2023-02-02 00:00:00.123456"; """ 

    }
}