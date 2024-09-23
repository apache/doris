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

suite("test_hms_event_notification_multi_catalog", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String useMetaCache : ["true","false"] ) {

    for (String hivePrefix : [ "hive2","hive3"]) {
        try {
            setHivePrefix(hivePrefix)
            hive_docker """ set hive.stats.autogather=false; """
            

            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_hms_event_notification_multi_catalog_${hivePrefix}"
            String catalog_name_2 = "test_hms_event_notification_multi_catalog_${hivePrefix}_2"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            int wait_time = 10000;

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                "use_meta_cache" = "${useMetaCache}",
                "hive.enable_hms_events_incremental_sync" ="true",
               "hive.hms_events_batch_size_per_rpc" = "10000"
            );"""

            sql """drop catalog if exists ${catalog_name_2}"""
            sql """create catalog if not exists ${catalog_name_2} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                "use_meta_cache" = "${useMetaCache}",
                "hive.enable_hms_events_incremental_sync" ="true",
               "hive.hms_events_batch_size_per_rpc" = "100000"
            );"""
            sleep(wait_time);

            sql """ switch ${catalog_name} """
            
            String tb1 = """${catalog_name}_tb_1"""
            String tb2 = """${catalog_name}_tb_2"""
            String db1  = "${catalog_name}_db_1";
            String db2  = "${catalog_name}_db_2";
            String partition_tb = "${catalog_name}_partition_tb"; 

            try {
                hive_docker """ use  ${db1};""" 
            }catch (Exception e){
            }

            hive_docker """ drop table if exists ${tb1};""" 
            hive_docker """ drop table if exists ${tb2};""" 
            hive_docker """ drop table if exists ${partition_tb} """ 
            hive_docker """ drop database if exists ${db1};""" 
            hive_docker """ drop database if exists ${db2};"""

//CREATE DATABASE            
            hive_docker """ create  database  ${db1};""" 
            hive_docker """ create  database  ${db2};"""
            sleep(wait_time);

            List<List<String>>  dbs = sql """ show databases """ 
            logger.info("result = " + dbs);
            
            int flag_db_count = 0 ; 
            dbs.forEach {
                if (it[0] == db1) {
                    flag_db_count ++;
                }else if (it[0] == db2) {
                    flag_db_count ++;
                } 
            }
            assertTrue(flag_db_count == 2);

            sql """ switch ${catalog_name_2} """
            dbs = sql """ show databases """ 
            logger.info("result = " + dbs);
            flag_db_count = 0 ; 
            dbs.forEach {
                if (it[0] == db1) {
                    flag_db_count ++;
                }else if (it[0] == db2) {
                    flag_db_count ++;
                } 
            }
            assertTrue(flag_db_count == 2);
            
            sql """ switch ${catalog_name} """



//ALTER DATABASE
            if (hivePrefix == "hive3") {
                String db2_location = (sql """ SHOW CREATE DATABASE ${db2} """)[0][1] 
                logger.info("db2 location = " + db2_location )

                def loc_start = db2_location.indexOf("hdfs://")
                def loc_end = db2_location.indexOf(".db") + 3 
                db2_location  = db2_location.substring(loc_start, loc_end)
                logger.info("db2 location = " + db2_location )

                String new_db2_location = db2_location.replace("warehouse", "new_warehouse_xxx")
                logger.info("change db2 location to   ${new_db2_location} ")

                logger.info(" alter database begin")
                hive_docker """ ALTER DATABASE  ${db2} SET LOCATION '${new_db2_location}'; """
                logger.info(" alter database end")
                sleep(wait_time);
                
                String query_db2_location =  (sql """ SHOW CREATE DATABASE ${db2} """)[0][1] 
                logger.info("query_db2_location  =  ${query_db2_location} ")

                loc_start = query_db2_location.indexOf("hdfs://")
                loc_end = query_db2_location.indexOf(".db") + 3 
                query_db2_location = query_db2_location.substring(loc_start, loc_end)
                assertTrue(query_db2_location == new_db2_location);



                sql """ switch ${catalog_name_2} """
                query_db2_location =  (sql """ SHOW CREATE DATABASE ${db2} """)[0][1] 
                logger.info("query_db2_location  =  ${query_db2_location} ")

                loc_start = query_db2_location.indexOf("hdfs://")
                loc_end = query_db2_location.indexOf(".db") + 3 
                query_db2_location = query_db2_location.substring(loc_start, loc_end)
                assertTrue(query_db2_location == new_db2_location);
                sql """ switch ${catalog_name} """
            }


//DROP DATABASE
            hive_docker """drop  database ${db2}; """;
            sleep(wait_time);
            dbs = sql """ show databases """ 
            logger.info("result = " + dbs);
            flag_db_count = 0 ; 
            dbs.forEach {
                if (it[0].toString() == db1) {
                    flag_db_count ++;
                } else if (it[0].toString() == db2) {
                    logger.info(" exists ${db2}")
                    assertTrue(false);
                } 
            }
            assertTrue(flag_db_count == 1);

            sql """ switch ${catalog_name_2} """
            dbs = sql """ show databases """ 
            logger.info("result = " + dbs);
            flag_db_count = 0 ; 
            dbs.forEach {
                if (it[0].toString() == db1) {
                    flag_db_count ++;
                } else if (it[0].toString() == db2) {
                    logger.info(" exists ${db2}")
                    assertTrue(false);
                } 
            }
            assertTrue(flag_db_count == 1);
            sql """ switch ${catalog_name} """

//CREATE TABLE
            hive_docker """ use ${db1} """
            sql """ use ${db1} """                         
            List<List<String>>  tbs = sql """ show tables; """ 
            logger.info(" tbs = ${tbs}")
            assertTrue(tbs.isEmpty())


            hive_docker """ create  table ${tb1} (id int,name string) ;"""
            hive_docker """ create  table ${tb2} (id int,name string) ;"""
            sleep(wait_time);
            tbs = sql """ show tables; """ 
            logger.info(" tbs = ${tbs}")
            int flag_tb_count = 0 ; 
            tbs.forEach {
                logger.info("it[0] = " + it[0])
                if (it[0].toString() == "${tb1}") {
                    flag_tb_count ++;
                    logger.info(" ${tb1} exists ")
                }else if (it[0].toString() == tb2) {
                    flag_tb_count ++;
                    logger.info(" ${tb2} exists ")
                } 
            }
            assertTrue(flag_tb_count == 2);
            

            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            tbs = sql """ show tables; """ 
            logger.info(" tbs = ${tbs}")
            flag_tb_count = 0 ; 
            tbs.forEach {
                logger.info("it[0] = " + it[0])
                if (it[0].toString() == "${tb1}") {
                    flag_tb_count ++;
                    logger.info(" ${tb1} exists ")
                }else if (it[0].toString() == tb2) {
                    flag_tb_count ++;
                    logger.info(" ${tb2} exists ")
                } 
            }
            assertTrue(flag_tb_count == 2);
            sql """ switch ${catalog_name} """
            sql """ use ${db1} """


//ALTER TABLE
            List<List<String>> ans = sql """ select * from ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.isEmpty())
            
            hive_docker """ insert into  ${tb1}  select 1,"xxx"; """    
            sleep(wait_time);
            ans = sql """ select * from ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 1)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")

            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            ans = sql """ select * from ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 1)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """

            hive_docker """ insert into  ${tb1} values( 2,"yyy"); """    
            sleep(wait_time);
            ans = sql """ select * from ${tb1} order by id """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")
            assertTrue(ans[1][0].toString() == "2")
            assertTrue(ans[1][1].toString() == "yyy")


            ans = sql """ desc ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "id")
            assertTrue(ans[0][1].toString() == "int")
            assertTrue(ans[1][0].toString() == "name")
            assertTrue(ans[1][1].toString() == "text")


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            ans = sql """ select * from ${tb1} order by id """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")
            assertTrue(ans[1][0].toString() == "2")
            assertTrue(ans[1][1].toString() == "yyy")

            ans = sql """ desc ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "id")
            assertTrue(ans[0][1].toString() == "int")
            assertTrue(ans[1][0].toString() == "name")
            assertTrue(ans[1][1].toString() == "text")

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """


            hive_docker """ alter table ${tb1} change column id id bigint; """    
            sleep(wait_time);
            ans = sql """ desc ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "id")
            assertTrue(ans[0][1].toString() == "bigint")
            assertTrue(ans[1][0].toString() == "name")
            assertTrue(ans[1][1].toString() == "text")    
            ans = sql """ select * from ${tb1} order by id """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")
            assertTrue(ans[1][0].toString() == "2")
            assertTrue(ans[1][1].toString() == "yyy")


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            ans = sql """ desc ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "id")
            assertTrue(ans[0][1].toString() == "bigint")
            assertTrue(ans[1][0].toString() == "name")
            assertTrue(ans[1][1].toString() == "text")    
            ans = sql """ select * from ${tb1} order by id """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")
            assertTrue(ans[1][0].toString() == "2")
            assertTrue(ans[1][1].toString() == "yyy")

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """



            hive_docker """ alter table ${tb1} change column name new_name string; """    
            sleep(wait_time);
            ans = sql """ desc ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "id")
            assertTrue(ans[0][1].toString() == "bigint")
            assertTrue(ans[1][0].toString() == "new_name")
            assertTrue(ans[1][1].toString() == "text")    
            ans = sql """ select * from ${tb1} order by id """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")
            assertTrue(ans[1][0].toString() == "2")
            assertTrue(ans[1][1].toString() == "yyy")

            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            ans = sql """ desc ${tb1} """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "id")
            assertTrue(ans[0][1].toString() == "bigint")
            assertTrue(ans[1][0].toString() == "new_name")
            assertTrue(ans[1][1].toString() == "text")    
            ans = sql """ select * from ${tb1} order by id """ 
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 2)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][1].toString() == "xxx")
            assertTrue(ans[1][0].toString() == "2")
            assertTrue(ans[1][1].toString() == "yyy")

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """

            


//DROP TABLE
            hive_docker  """ drop table ${tb2} """ 
            sleep(wait_time);
            tbs = sql """ show tables; """

            logger.info(""" tbs = ${tbs}""")

            flag_tb_count = 0 ; 
            tbs.forEach {
                if (it[0] == tb1) {
                    flag_tb_count ++;
                } else if (it[0] == tb2) {
                    logger.info("exists ${tb1}")
                    assertTrue(false);
                } 
            }
            assertTrue(flag_tb_count == 1);


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            tbs = sql """ show tables; """
            logger.info(""" tbs = ${tbs}""")
            flag_tb_count = 0 ; 
            tbs.forEach {
                if (it[0] == tb1) {
                    flag_tb_count ++;
                } else if (it[0] == tb2) {
                    logger.info("exists ${tb2}")
                    assertTrue(false);
                } 
            }
            assertTrue(flag_tb_count == 1);

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """



            
            hive_docker  """ drop table ${tb1} """ 
            sleep(wait_time);
            tbs = sql """ show tables; """

            logger.info(""" tbs = ${tbs}""")

            tbs.forEach {
                if (it[0] == tb1) {
                    logger.info("exists ${tb1}")
                    assertTrue(false);
                } else if (it[0] == tb2) {
                    logger.info("exists ${tb2}")
                    assertTrue(false);
                } 
            }


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            tbs = sql """ show tables; """
            logger.info(""" tbs = ${tbs}""")
            tbs.forEach {
                if (it[0] == tb1) {
                    logger.info("exists ${tb1}")
                    assertTrue(false);
                } else if (it[0] == tb2) {
                    logger.info("exists ${tb2}")
                    assertTrue(false);
                } 
            }
            sql """ switch ${catalog_name} """
            sql """ use ${db1} """



//ADD PARTITION

            hive_docker """ use ${db1} """
            sql """ use ${db1} """  

            hive_docker  """ CREATE TABLE ${partition_tb} (
                        id INT,
                        name STRING,
                        age INT
                    )
                    PARTITIONED BY (country STRING); """ 
            hive_docker """ 
                INSERT INTO TABLE ${partition_tb} PARTITION (country='USA')
                VALUES (1, 'John Doe', 30),
                       (2, 'Jane Smith', 25);"""
                       
            hive_docker """ 
                INSERT INTO TABLE ${partition_tb} PARTITION (country='India')
                VALUES (3, 'Rahul Kumar', 28),
                       (4, 'Priya Singh', 24);
                """
            sleep(wait_time);
            ans = sql """ select * from ${partition_tb} order by id"""
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 4)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][3].toString() == "USA")
            assertTrue(ans[1][3].toString() == "USA")            
            assertTrue(ans[3][0].toString() == "4")
            assertTrue(ans[2][3].toString() == "India")
            assertTrue(ans[3][3].toString() == "India")

            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            ans = sql """ select * from ${partition_tb} order by id"""
            logger.info("ans = ${ans}")
            assertTrue(ans.size() == 4)
            assertTrue(ans[0][0].toString() == "1")
            assertTrue(ans[0][3].toString() == "USA")
            assertTrue(ans[1][3].toString() == "USA")            
            assertTrue(ans[3][0].toString() == "4")
            assertTrue(ans[2][3].toString() == "India")
            assertTrue(ans[3][3].toString() == "India")

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """



            List<List<String>> pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 2)
            int flag_partition_count = 0 ; 
            pars.forEach {
                if (it[0] == "country=India") {
                    flag_partition_count ++;
                } else if (it[0] == "country=USA") {
                    flag_partition_count ++;
                } 
            }
            assertTrue(flag_partition_count ==2) 

            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 2)
            flag_partition_count = 0 ; 
            pars.forEach {
                if (it[0] == "country=India") {
                    flag_partition_count ++;
                } else if (it[0] == "country=USA") {
                    flag_partition_count ++;
                } 
            }
            assertTrue(flag_partition_count ==2) 

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """




            hive_docker """ 
                ALTER TABLE ${partition_tb} ADD PARTITION (country='Canada');
                """
            sleep(wait_time);
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 3)
            flag_partition_count = 0 ; 
            pars.forEach {
                if (it[0].toString() == "country=India") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=USA") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=Canada") {
                    flag_partition_count ++;
                }
            }
            assertTrue(flag_partition_count ==3) 


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 3)
            flag_partition_count = 0 ; 
            pars.forEach {
                if (it[0].toString() == "country=India") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=USA") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=Canada") {
                    flag_partition_count ++;
                }
            }
            assertTrue(flag_partition_count ==3) 


            sql """ switch ${catalog_name} """
            sql """ use ${db1} """




//ALTER PARTITION
            hive_docker """ 
                alter table ${partition_tb} partition(country='USA') rename to partition(country='US') ;
            """
            sleep(wait_time);
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 3)
            flag_partition_count = 0 ; 
            pars.forEach {
                if (it[0].toString() == "country=India") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=US") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=Canada") {
                    flag_partition_count ++;
                }
            }
            assertTrue(flag_partition_count ==3) 


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 3)
            flag_partition_count = 0 ; 
            pars.forEach {
                if (it[0].toString() == "country=India") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=US") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=Canada") {
                    flag_partition_count ++;
                }
            }
            assertTrue(flag_partition_count ==3) 

            sql """ switch ${catalog_name} """
            sql """ use ${db1} """




//DROP PARTITION
            hive_docker """ 
                ALTER TABLE ${partition_tb} DROP PARTITION (country='Canada');
            """
            sleep(wait_time);
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 2)
            flag_partition_count = 0 
            pars.forEach {
                if (it[0].toString() == "country=India") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=US") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=Canada") {
                    logger.info("exists partition canada")
                    assertTrue(false);
                }
            }
            assertTrue(flag_partition_count ==2) 


            sql """ switch ${catalog_name_2} """
            sql """ use ${db1} """
            pars = sql """ SHOW PARTITIONS from  ${partition_tb}; """ 
            logger.info("pars = ${pars}")
            assertTrue(pars.size() == 2)
            flag_partition_count = 0 
            pars.forEach {
                if (it[0].toString() == "country=India") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=US") {
                    flag_partition_count ++;
                } else if (it[0].toString() == "country=Canada") {
                    logger.info("exists partition canada")
                    assertTrue(false);
                }
            }
            assertTrue(flag_partition_count ==2) 
            sql """ switch ${catalog_name} """
            sql """ use ${db1} """



            sql """drop catalog if exists ${catalog_name}"""
            sql """drop catalog if exists ${catalog_name_2}"""
        } finally {
        }
    }
    }
}




