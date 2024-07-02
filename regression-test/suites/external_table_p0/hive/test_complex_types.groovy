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

suite("test_complex_types", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_complex_types"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
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

        // qt_array_filter """select count(array_size(array_filter(i -> (i > 0.99), capacity))) from byd where array_size(array_filter(i -> (i > 0.99), capacity))"""

        // qt_array_last """select max(array_last(i -> i > 0, capacity)) from byd where array_last(i -> i > 0, capacity) < 0.99"""

        qt_null_struct_element_orc """select count(struct_element(favor, 'tip')) from byd where id % 13 = 0"""

        qt_map_key_select_orc """select id, singles["p0X72J-mkMe40O-vOa-opfI"] as map_key from byd where singles["p0X72J-mkMe40O-vOa-opfI"] is not null"""

        qt_map_keys_orc """select map_keys(singles) from byd where id = 1077"""

        qt_map_values_orc """select map_values(singles) from byd where id = 1433"""

        qt_map_contains_key_orc """select * from byd where map_contains_key(singles, 'B0mXFX-QvgUgo7-Dih-6rDu') = 1"""

        qt_array_max_orc """select count(array_max(capacity)) from byd where array_max(capacity) > 0.99"""

        // qt_array_filter_orc """select count(array_size(array_filter(i -> (i > 0.99), capacity))) from byd where array_size(array_filter(i -> (i > 0.99), capacity))"""

        // qt_array_last_orc """select max(array_last(i -> i > 0, capacity)) from byd where array_last(i -> i > 0, capacity) < 0.99"""

        qt_offsets_check """select * from complex_offsets_check order by id"""

        qt_map_with_nullable_key """select * from parquet_all_types limit 1"""

        qt_date_dict """select max(date1), max(date2), max(date3) from date_dict"""

        sql """drop catalog ${catalog_name};"""
    }
}
