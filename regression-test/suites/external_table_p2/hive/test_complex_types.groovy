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

suite("test_complex_types", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_complex_types"
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
        
        sql """ use multi_catalog """

        qt_null_struct_element """select count(struct_element(favor, 'tip')) from byd where id % 13 = 0"""

        qt_map_key_select """select id, singles["p0X72J-mkMe40O-vOa-opfI"] as map_key from byd where singles["p0X72J-mkMe40O-vOa-opfI"] is not null"""

        qt_map_keys """select map_keys(singles) from byd where id = 1077"""

        qt_map_values """select map_values(singles) from byd where id = 1433"""

        qt_map_contains_key """select * from byd where map_contains_key(singles, 'B0mXFX-QvgUgo7-Dih-6rDu') = 1"""

        qt_array_max """select count(array_max(capacity)) from byd where array_max(capacity) > 0.99"""

        qt_array_filter """select count(array_size(array_filter(i -> (i > 0.99), capacity))) from byd where array_size(array_filter(i -> (i > 0.99), capacity))"""

        qt_array_last """select max(array_last(i -> i > 0, capacity)) from byd where array_last(i -> i > 0, capacity) < 0.99"""

        qt_null_struct_element_orc """select count(struct_element(favor, 'tip')) from byd where id % 13 = 0"""

        qt_map_key_select_orc """select id, singles["p0X72J-mkMe40O-vOa-opfI"] as map_key from byd where singles["p0X72J-mkMe40O-vOa-opfI"] is not null"""

        qt_map_keys_orc """select map_keys(singles) from byd where id = 1077"""

        qt_map_values_orc """select map_values(singles) from byd where id = 1433"""

        qt_map_contains_key_orc """select * from byd where map_contains_key(singles, 'B0mXFX-QvgUgo7-Dih-6rDu') = 1"""

        qt_array_max_orc """select count(array_max(capacity)) from byd where array_max(capacity) > 0.99"""

        qt_array_filter_orc """select count(array_size(array_filter(i -> (i > 0.99), capacity))) from byd where array_size(array_filter(i -> (i > 0.99), capacity))"""

        qt_array_last_orc """select max(array_last(i -> i > 0, capacity)) from byd where array_last(i -> i > 0, capacity) < 0.99"""

        qt_offsets_check """select * from complex_offsets_check order by id"""

        qt_map_with_nullable_key """select * from parquet_all_types limit 1"""

        qt_date_dict """select max(date1), max(date2), max(date3) from date_dict"""

        sql """drop catalog ${catalog_name};"""
    }
}
