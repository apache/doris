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

import org.apache.doris.regression.action.ProfileAction

suite("test_parquet_join_runtime_filter", "p0,external") {

    def profileAction = new ProfileAction(context)

    def extractFilteredGroupsValue = { String profileText ->
        def values = []
        boolean inFactScan = false
        profileText.eachLine { line ->
            if (line =~ /^\s*[A-Z_]+_OPERATOR\(/) {
                inFactScan = (line =~ /^\s*FILE_SCAN_OPERATOR\([^)]*\btable_name=fact_big\):/).find()
            }
            if (inFactScan) {
                def counter = (line =~ /RowGroupsFiltered:\s*(?:sum\s+)?(\d+)/)
                if (counter.find()) {
                    values.add(counter.group(1).toLong())
                }
            }
        }
        // A profile can repeat scan sections. Use the largest fact scan count, not the
        // number or sum of matches, so duplicates cannot inflate the filtering result.
        return values.sort { a, b -> b <=> a }
    }

    def getProfileWithToken = { token ->
        // Wait for asynchronous profile collection instead of assuming a fixed delay is enough.
        return profileAction.getProfileBySql(token, ["table_name=fact_big)", "RowGroupsFiltered:"])
    }
    // session vars
    sql "unset variable all;"
    sql "set profile_level=2;"
    sql "set enable_profile=true;"
    sql " set parallel_pipeline_task_num = 1;"
    sql " set file_split_size = 100000;"

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return;
    }
    for (String hivePrefix : ["hive2"]) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_parquet_join_runtime_filter"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        
        sql """ use `default` """


        for (int wait_time : [0, 10, 100]) {
            sql """ set runtime_filter_wait_time_ms = ${wait_time}; """ 

            def f1 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c1 = 5
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertFalse(filter_result.isEmpty(), "Missing RowGroupsFiltered for fact_big")
                assertTrue(filter_result[0] > 40, "Insufficient fact_big filtering: ${filter_result}")
            }



            def f2 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c1 in (1,2)
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertFalse(filter_result.isEmpty(), "Missing RowGroupsFiltered for fact_big")
                assertTrue(filter_result[0] > 30, "Insufficient fact_big filtering: ${filter_result}")
            }




            def f3 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c1 < 3  
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertFalse(filter_result.isEmpty(), "Missing RowGroupsFiltered for fact_big")
                assertTrue(filter_result[0] > 30, "Insufficient fact_big filtering: ${filter_result}")
            }



            def f4 = {
                def t1 = UUID.randomUUID().toString()
                def sql_result = sql """
                    select *, "${t1}" from fact_big as a  join dim_small as b on a.k = b.k  where b.c2 >= 50   
                """
                def filter_result = extractFilteredGroupsValue(getProfileWithToken(t1));
                logger.info("sql_result = ${sql_result}");
                logger.info("filter_result = ${filter_result}");

                assertFalse(filter_result.isEmpty(), "Missing RowGroupsFiltered for fact_big")
                assertTrue(filter_result[0] > 40, "Insufficient fact_big filtering: ${filter_result}")
            }


            f1()
            f2()
            f3()
            f4()
        }     

        sql """drop catalog ${catalog_name};"""
    }




  
}
