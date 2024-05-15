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

suite("test_hive_view", "p0,external,hive,external_docker,external_docker_hive") {



    def run = {
        qt_q00 """
           select * from view_0;
        """

        qt_q01 """
           select * from view_1 order by r_name;
        """

        qt_q02 """
           select * from view_2 order by r_name;
        """

        qt_q03 """
           select * from view_3 order by r_name;
        """

        qt_q04 """
           select * from view_4 order by r_name;
        """

        qt_q05 """
           select * from view_5 order by r_name;
        """

        qt_q06 """
           select * from view_6 order by r_name;
        """

        qt_q07 """
           select * from view_7 order by r_name;
        """
    }

//    String enabled = context.config.otherConfigs.get("enableHiveTest")
//    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
//        logger.info("disable Hive test.")
//        return;
//    }
    
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_catalog_${hivePrefix}_view"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """switch ${catalog_name}"""
        sql """use `tpch1_orc`"""

        // test legacy planner
        sql """set enable_nereids_planner=false;"""
        run()

        // test nereids planner
        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
        run()

        sql """drop catalog if exists ${catalog_name}"""
    }
}



