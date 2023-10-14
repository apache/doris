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

suite("test_hive_parquet_alter_column", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String hms_port = context.config.otherConfigs.get("hms_port")

        String catalog_name = "test_hive_parquet_alter_column"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        String Orderby = """ order by col_int,col_smallint,col_tinyint,col_bigint,col_float,col_double,col_boolean,col_string,col_char,col_varchar,col_decimal,col_date,col_timestamp limit 7 """

        sql """ use multi_catalog """


        
        types = ["int","smallint","tinyint","bigint","float","double","boolean","string","char","varchar","date","timestamp","decimal"]
        
        for( String type1 in types) {
            qt_desc """ desc parquet_alter_column_to_${type1} ; """

            qt_show """ select * from parquet_alter_column_to_${type1} ${Orderby} """
            
            for( String type2 in types) {

                qt_order """ select col_${type2} from  parquet_alter_column_to_${type1} order by col_${type2} limit 3 """
            
            }
        }
        
    }
}
