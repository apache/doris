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

suite("test_parquet_nested_types", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_parquet_nested_types"
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
        
        order_qt_nested_cross_page1_parquet_q1 """select array_col from nested_cross_page1_parquet where id = 1"""
        
        order_qt_nested_cross_page1_parquet_q2 """select array_col from nested_cross_page1_parquet where id = 2"""
        
        order_qt_nested_cross_page1_parquet_q3 """select array_col from nested_cross_page1_parquet where id = 3"""

        order_qt_nested_cross_page1_parquet_q4 """
            SELECT id, array_size(array_col) as arr_size
            FROM nested_cross_page1_parquet
            ORDER BY id
        """

        order_qt_nested_cross_page1_parquet_q5 """
            SELECT id, array_col[1] as first_elem, array_col[3] as third_elem
            FROM nested_cross_page1_parquet
            ORDER BY id
        """

        order_qt_nested_cross_page1_parquet_q6 """
            SELECT id, array_col
            FROM nested_cross_page1_parquet
            WHERE array_size(array_col) > 100
            ORDER BY id
        """

        order_qt_nested_cross_page1_parquet_q7 """
            SELECT id, array_col
            FROM nested_cross_page1_parquet
            WHERE array_contains(array_col, 1)
            ORDER BY id
        """

        order_qt_nested_cross_page1_parquet_q8 """
            SELECT id, array_col, description
            FROM nested_cross_page1_parquet
            WHERE id > 1 AND array_size(array_col) < 100
            ORDER BY id
        """

        order_qt_nested_cross_page1_parquet_q9 """
            SELECT
                id,
                array_min(array_col) as min_val,
                array_max(array_col) as max_val
            FROM nested_cross_page1_parquet
            ORDER BY id
        """

        order_qt_nested_cross_page1_parquet_q10 """
            SELECT id, array_col
            FROM nested_cross_page1_parquet
            WHERE description LIKE '%large array%'
            ORDER BY id
        """

        order_qt_nested_cross_page2_parquet_q1 """
            SELECT
            id,
            nested_array_col,
            array_size(nested_array_col) as outer_size
            FROM nested_cross_page2_parquet
            WHERE id = 1
        """

        order_qt_nested_cross_page2_parquet_q2 """
            SELECT
            id,
            nested_array_col,
            array_size(nested_array_col) as outer_size
            FROM nested_cross_page2_parquet
            WHERE id = 2
        """

        order_qt_nested_cross_page2_parquet_q3 """
            SELECT
            id,
            nested_array_col,
            array_size(nested_array_col) as outer_size
            FROM nested_cross_page2_parquet
            WHERE id = 3
        """

        order_qt_nested_cross_page2_parquet_q4 """
            SELECT
                id,
                array_struct_col,
                array_size(array_struct_col) as struct_arr_size
            FROM nested_cross_page2_parquet
            WHERE description LIKE '%large%'
        """

        order_qt_nested_cross_page2_parquet_q5 """
            SELECT
                id,
                STRUCT_ELEMENT(item, 'x'),
                STRUCT_ELEMENT(item, 'y')
            FROM nested_cross_page2_parquet
            LATERAL VIEW EXPLODE(array_struct_col) tmp AS item
            WHERE id = 1 AND STRUCT_ELEMENT(item, 'x') > 100
        """

        order_qt_nested_cross_page2_parquet_q6 """
            SELECT
                id,
                map_array_col,
                array_size(map_array_col) as map_size
            FROM nested_cross_page2_parquet
            WHERE id = 2
        """
        
        order_qt_nested_cross_page3_parquet_q1 """select array_col from nested_cross_page3_parquet where id = 1"""
        
        order_qt_nested_cross_page3_parquet_q2 """select array_col from nested_cross_page3_parquet where id = 2"""
        
        order_qt_nested_cross_page3_parquet_q3 """select array_col from nested_cross_page3_parquet where id = 3"""

        order_qt_nested_cross_page3_parquet_q4 """
            SELECT id, array_size(array_col) as arr_size
            FROM nested_cross_page3_parquet
            ORDER BY id
        """

        order_qt_nested_cross_page3_parquet_q5 """
            SELECT id, array_col[1] as first_elem, array_col[3] as third_elem
            FROM nested_cross_page3_parquet
            ORDER BY id
        """

        order_qt_nested_cross_page3_parquet_q6 """
            SELECT id, array_col
            FROM nested_cross_page3_parquet
            WHERE array_size(array_col) > 100
            ORDER BY id
        """

        order_qt_nested_cross_page3_parquet_q7 """
            SELECT id, array_col
            FROM nested_cross_page3_parquet
            WHERE array_contains(array_col, 1)
            ORDER BY id
        """

        order_qt_nested_cross_page3_parquet_q8 """
            SELECT id, array_col, description
            FROM nested_cross_page3_parquet
            WHERE id > 1 AND array_size(array_col) < 100
            ORDER BY id
        """

        order_qt_nested_cross_page3_parquet_q9 """
            SELECT
                id,
                array_min(array_col) as min_val,
                array_max(array_col) as max_val
            FROM nested_cross_page3_parquet
            ORDER BY id
        """

        order_qt_nested_cross_page3_parquet_q10 """
            SELECT id, array_col
            FROM nested_cross_page3_parquet
            WHERE description LIKE '%large array%'
            ORDER BY id
        """

        sql """drop catalog ${catalog_name};"""
    }
}

