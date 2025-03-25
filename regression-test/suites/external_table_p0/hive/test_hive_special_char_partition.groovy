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

suite("test_hive_special_char_partition", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {

        setHivePrefix(hivePrefix)
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalog_name = "${hivePrefix}_test_hive_special_char_partition"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
        );"""
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use multi_catalog;"""
        qt_1 "select * from special_character_1_partition order by name"
        qt_2 "select name from special_character_1_partition where part='2023 01 01'"
        qt_3 "select name from special_character_1_partition where part='2023/01/01'"
        qt_4 "select * from special_character_1_partition where part='2023?01?01'"
        qt_5 "select * from special_character_1_partition where part='2023.01.01'"
        qt_6 "select * from special_character_1_partition where part='2023<01><01>'"
        qt_7 "select * from special_character_1_partition where part='2023:01:01'"
        qt_8 "select * from special_character_1_partition where part='2023=01=01'"
        qt_9 "select * from special_character_1_partition where part='2023\"01\"01'"
        qt_10 "select * from special_character_1_partition where part='2023\\'01\\'01'"
        qt_11 "select * from special_character_1_partition where part='2023\\\\01\\\\01'"
        qt_12 "select * from special_character_1_partition where part='2023%01%01'"
        qt_13 "select * from special_character_1_partition where part='2023#01#01'"



        // append some case.
        String table_name_1 = "partition_special_characters_1"  
        String table_name_2 = "partition_special_characters_2" 
        String table_name_3 = "partition_special_characters_3" 
    
        hive_docker """ set hive.stats.column.autogather=false """
        hive_docker """ use `default`"""
        def special_characters = [
                                    "1,1=1, 3=2+1, 1=3-2, 3/3=1, 2/2=1, 2/1=2, 2/1=2 +1 -1,2/1=2 *3 /3",
                                    " %100, @@@@@@ ,  %100 , !!asd!!, A%%Z",
                                    "abc:xyz,  1123,1222,  :::::::"
                ]


        logger.info(""" docker insert 1""")            

        hive_docker """ drop table if exists ${table_name_1} """ 
        hive_docker """ create table ${table_name_1} (id int) partitioned by (pt string) """
        special_characters.eachWithIndex { item, index ->
            hive_docker """ insert into  ${table_name_1} partition(pt="${item}")      values ("${index}"); """
            hive_docker """ insert into  ${table_name_1} partition(pt="${item}")      values ("${index*100}"); """
            println("Index: ${index}, Item: ${item}")    
        }

        
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        qt_sql1 """ select * from ${table_name_1} order by id """
        qt_sql2 """ show partitions from ${table_name_1} """

        special_characters.eachWithIndex { item, index ->
            qt_sql_where1 """ select * from  ${table_name_1} where pt = "${item}" order by id""" 
        }



        logger.info(""" docker insert 2""")            
        hive_docker """ drop table if exists ${table_name_2} """ 
        hive_docker """ create table ${table_name_2} (id int) partitioned by (pt1 string, `pt2=x!!!! **1+1/&^%3` string) """

        special_characters.eachWithIndex { outerItem, outerIndex ->
            special_characters.eachWithIndex { innerItem, innerIndex ->
                
                def insert_value = outerIndex * 100 + innerIndex;

                hive_docker """ insert into  ${table_name_2} partition(pt1="${outerItem}",`pt2=x!!!! **1+1/&^%3`="${innerItem}")   values ("${insert_value}"); """
                println("  Outer Item: ${outerItem}, Inner Item: ${innerItem}, value = ${insert_value}")
            }
        }   

        
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        qt_sql3 """ select * from ${table_name_2} order by id """
        qt_sql4 """ show partitions from ${table_name_2} """

        special_characters.eachWithIndex { item, index ->
            qt_sql_where2 """ select * from  ${table_name_2} where `pt2=x!!!! **1+1/&^%3` = "${item}"  order by id""" 
        }


        logger.info(""" docker insert 3""")            
        hive_docker """ drop table if exists ${table_name_3} """ 
        hive_docker """ create table ${table_name_3} (id int) partitioned by (pt1 string, `pt2=x!!!! **1+1/&^%3` string,pt3 string,pt4 string,pt5 string) """


        special_characters.eachWithIndex { outerItem, outerIndex ->
            special_characters.eachWithIndex { innerItem, innerIndex ->
                
                def insert_value = outerIndex * 100 + innerIndex;
                hive_docker """ insert into  ${table_name_3} partition(pt1="${outerItem}", `pt2=x!!!! **1+1/&^%3`="1", pt3="1",pt4="${innerItem}",pt5="1")   values ("${insert_value}"); """
                println("  Outer Item: ${outerItem}, Inner Item: ${innerItem}, value = ${insert_value}")
            }
        }   

                    
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        qt_sql5 """ select * from ${table_name_3} order by id """
        qt_sql6 """ show partitions from ${table_name_3} """

        special_characters.eachWithIndex { item, index ->
            qt_sql_where3 """ select * from  ${table_name_3} where pt4 = "${item}"  order by id""" 
        }






        logger.info("""  ---------- doris insert  -------------""")

        logger.info(""" doris insert 1""")            
        hive_docker """ drop table if exists ${table_name_1} """ 
        hive_docker """ create table ${table_name_1} (id int) partitioned by (pt string) """
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """
        special_characters.eachWithIndex { item, index ->
            sql """ insert into  ${table_name_1} (pt,id)  values ("${item}","${index}"); """
            sql """ insert into  ${table_name_1} (pt,id)  values ("${item}","${index*100}"); """

            println("Index: ${index}, Item: ${item}")    
        }

        
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        qt_sql1 """ select * from ${table_name_1} order by id """
        qt_sql2 """ show partitions from ${table_name_1} """

        special_characters.eachWithIndex { item, index ->
            qt_sql_where1 """ select * from  ${table_name_1} where pt = "${item}" order by id """ 
        }



        logger.info(""" doris insert 2""")            
        hive_docker """ drop table if exists ${table_name_2} """ 
        hive_docker """ create table ${table_name_2} (id int) partitioned by (pt1 string, `pt2=x!!!! **1+1/&^%3` string) """
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        special_characters.eachWithIndex { outerItem, outerIndex ->
            special_characters.eachWithIndex { innerItem, innerIndex ->
                
                def insert_value = outerIndex * 100 + innerIndex;

                sql """ insert into  ${table_name_2} (pt1,`pt2=x!!!! **1+1/&^%3`,id)   values ("${outerItem}","${innerItem}","${insert_value}"); """
                println("  Outer Item: ${outerItem}, Inner Item: ${innerItem}, value = ${insert_value}")
            }
        }   

        
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        qt_sql3 """ select * from ${table_name_2} order by id """
        qt_sql4 """ show partitions from ${table_name_2} """

        special_characters.eachWithIndex { item, index ->
            qt_sql_where2 """ select * from  ${table_name_2} where `pt2=x!!!! **1+1/&^%3` = "${item}"  order by id""" 
        }




        logger.info(""" docker insert 3""")            
        hive_docker """ drop table if exists ${table_name_3} """ 
        hive_docker """ create table ${table_name_3} (id int) partitioned by (pt1 string, `pt2=x!!!! **1+1/&^%3` string,pt3 string,pt4 string,pt5 string) """
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        special_characters.eachWithIndex { outerItem, outerIndex ->
            special_characters.eachWithIndex { innerItem, innerIndex ->
                
                def insert_value = outerIndex * 100 + innerIndex;
                sql """ insert into  ${table_name_3} (pt1, `pt2=x!!!! **1+1/&^%3`, pt3,pt4,pt5,id)   values ("${outerItem}","1","1","${innerItem}","1","${insert_value}"); """
                println("  Outer Item: ${outerItem}, Inner Item: ${innerItem}, value = ${insert_value}")
            }
        }   

                    
        sql """refresh catalog ${catalog_name} """
        sql """switch ${catalog_name} """
        sql """ use `default` """

        qt_sql5 """ select * from ${table_name_3} order by id """
        qt_sql6 """ show partitions from ${table_name_3} """

        special_characters.eachWithIndex { item, index ->
            qt_sql_where3 """ select * from  ${table_name_3} where pt4 = "${item}"  order by id""" 
        }




    }
}

