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


suite("show_tables_cache") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "show_tables_cache"
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

        String cache_table = " internal.__internal_schema.cache_show_tables  " 

        sql """ drop table if exists ${cache_table}; """ 
        sql """ set enable_cache_show_tables = true; """ 
        
        sql """ use multi_catalog ;""";
        sql """ show tables; """

        qt_sql """ select catalog_name,database_name,table_name from ${cache_table}
                where table_name ="hive_textfile_array_all_types"; """ 

        qt_sql """ select catalog_name,database_name,table_name from ${cache_table}
                where table_name ="hive_textfile_array_delimiter"; """ 

        
        sql """ drop table ${cache_table}; """ 
        sql """ show tables; """

        qt_sql """ select catalog_name,database_name,table_name from ${cache_table}
                where table_name ="hive_textfile_array_all_types"; """ 

        qt_sql """ select catalog_name,database_name,table_name from ${cache_table}
                where table_name ="hive_textfile_array_delimiter"; """ 
        
    }
}

