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

suite("test_hive_text_complex_type", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_hive_text_complex_type"

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
        
        sql """ use multi_catalog """ 
	
       	qt_sql1 """ select * from hive_text_complex_type order by column1; """ 

        qt_sql2 """ select * from hive_text_complex_type_delimiter order by column1; """   

        qt_filter_complex """select count(column_primitive_integer),
            count(column1_struct),
            count(column_primitive_bigint)
            from parquet_predicate_table where column_primitive_bigint = 6;"""
        qt_sql3 """ select * from hive_text_complex_type2 order by id; """   
         
        qt_sql4 """ select * from hive_text_complex_type_delimiter2 order by id; """   

        qt_sql5 """ select * from hive_text_complex_type3 order by id; """   
        
        qt_sql6 """ select * from hive_text_complex_type_delimiter3 order by id; """   
        sql """set enable_vectorized_engine = true """

        qt_sql7 """ select array_size(col2) from hive_text_complex_type2 order by id; """ 


        qt_sql8 """  select *  from hive_text_complex_type2 where array_size(col2)>2   order by id; """ 

        qt_sql9 """ select count(*) as a  from hive_text_complex_type2  group by map_size(col1) order by a;"""


        qt_sql10 """ select map_keys(col1) as a  from hive_text_complex_type2  order by id;"""

        qt_sql11 """ select id   from hive_text_complex_type2 where element_at(map_keys(col1),1)=20 order by id ;"""

        qt_sql12 """ select * from hive_text_complex_type_delimiter2  where map_keys(col5)[1]=6  order by id;"""

        qt_sql13 """ select  id,col4  from hive_text_complex_type_delimiter2  where map_keys(map_values(col4)[1])[2]=500  order by id; """ 

        qt_sql14 """ select col3 from hive_text_complex_type2 where struct_element(col3,1)=5 order by id; """ 

        qt_sql15 """select count(*) from hive_text_complex_type2 where struct_element(struct_element(col3,3),2)=0; """

        qt_sql16 """ select count(*) from hive_text_complex_type2 group by struct_element(col3,4)[1]; """ 


        sql """drop catalog ${catalog_name};"""
    }
}