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

suite("test_orc_nested_types", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_orc_nested_types"
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

        -- 简单查询
        order_qt_nested_types_q1 """select array_col from nested_types_example where id = 1"""
        order_qt_nested_types_q2 """select array_col from nested_types_example where id = 2"""
        order_qt_nested_types_q3 """select array_col from nested_types_example where id = 3"""
        
        -- 数组大小查询
        order_qt_nested_types_q4 """
            SELECT id, size(array_col) as arr_size
            FROM nested_types_example
            ORDER BY id
        """
        order_qt_nested_types_q5 """
            SELECT id, array_col
            FROM nested_types_example
            WHERE size(array_col) > 2
            ORDER BY id
        """
        
        -- 数组元素查询
        order_qt_nested_types_q6 """
            SELECT id, array_col[0] as first_elem, array_col[2] as third_elem
            FROM nested_types_example
            ORDER BY id
        """
        order_qt_nested_types_q7 """
            SELECT id, array_col
            FROM nested_types_example
            WHERE array_contains(array_col, 1)
            ORDER BY id
        """
        
        -- 复杂条件查询
        order_qt_nested_types_q8 """
            SELECT id, array_col, description
            FROM nested_types_example
            WHERE id > 1 AND size(array_col) < 3
            ORDER BY id
        """
        order_qt_nested_types_q9 """
            SELECT id, array_col, description
            FROM nested_types_example
            WHERE description LIKE '%Hello%'
            ORDER BY id
        """
        
        -- 数组统计查询
        order_qt_nested_types_q10 """
            SELECT
                id,
                array_min(array_col) as min_val,
                array_max(array_col) as max_val
            FROM nested_types_example
            ORDER BY id
        """
        
        -- 嵌套数组查询
        order_qt_nested_types_q11 """
            SELECT
                id,
                nested_array_col,
                size(nested_array_col) as outer_size
            FROM nested_types_example
            WHERE id = 1
        """
        order_qt_nested_types_q12 """
            SELECT
                id,
                nested_array_col,
                size(nested_array_col[0]) as inner_size
            FROM nested_types_example
            WHERE id = 2
        """
        
        -- 结构体数组查询
        order_qt_nested_types_q13 """
            SELECT
                id,
                array_struct_col,
                size(array_struct_col) as struct_arr_size
            FROM nested_types_example
            WHERE description LIKE '%large%'
        """
        order_qt_nested_types_q14 """
            SELECT
                id,
                item.name as name,
                item.age as age
            FROM nested_types_example
            LATERAL VIEW EXPLODE(array_struct_col) tmp AS item
            WHERE id = 1 AND item.age > 30
        """
        
        -- 映射数组查询
        order_qt_nested_types_q15 """
            SELECT
                id,
                map_array_col,
                size(map_array_col) as map_size
            FROM nested_types_example
            WHERE id = 2
        """
        order_qt_nested_types_q16 """
            SELECT
                id,
                map_array_col['a'] as a_value
            FROM nested_types_example
            WHERE id = 1
        """
        
        -- 复杂结构体查询
        order_qt_nested_types_q17 """
            SELECT
                id,
                complex_struct_col.a as array_a
            FROM nested_types_example
            WHERE id = 1
        """
        order_qt_nested_types_q18 """
            SELECT
                id,
                complex_struct_col.b as map_b
            FROM nested_types_example
            WHERE id = 2
        """
        order_qt_nested_types_q19 """
            SELECT
                id,
                complex_struct_col.c as struct_c
            FROM nested_types_example
            WHERE id = 3
        """
        
        -- 综合查询
        order_qt_nested_types_q20 """
            SELECT *
            FROM nested_types_example
            ORDER BY id
        """

        sql """drop catalog ${catalog_name};"""
    }
}

