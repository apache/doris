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

        order_qt_nested_types1_q1 """select * from nested_types1_orc where id = 1"""
        
        order_qt_nested_types1_q2 """select array_col from nested_types1_orc where id = 2"""
        
        order_qt_nested_types1_q3 """select nested_array_col from nested_types1_orc where id = 3"""
        
        
        order_qt_nested_types1_q4 """
            SELECT id, size(array_col) as arr_size
            FROM nested_types1_orc
            ORDER BY id
        """
        
        order_qt_nested_types1_q5 """
            SELECT id, array_col[0] as first_elem, array_col[2] as third_elem
            FROM nested_types1_orc
            ORDER BY id
        """
        
        order_qt_nested_types1_q6 """
            SELECT id, array_col
            FROM nested_types1_orc
            WHERE array_contains(array_col, 1)
            ORDER BY id
        """
        
        order_qt_nested_types1_q7 """
            SELECT
                id,
                array_min(array_col) as min_val,
                array_max(array_col) as max_val
            FROM nested_types1_orc
            ORDER BY id
        """
        
        order_qt_nested_types1_q8 """
            SELECT
                id,
                nested_array_col,
                size(nested_array_col) as outer_size
            FROM nested_types1_orc
            WHERE id = 1
        """
        
        order_qt_nested_types1_q9 """
            SELECT
                id,
                nested_array_col,
                size(nested_array_col[0]) as inner_size
            FROM nested_types1_orc
            WHERE id = 2
        """
        
        order_qt_nested_types1_q10 """
            SELECT
                id,
                nested_array_col[0] as first_inner_array
            FROM nested_types1_orc
            WHERE id = 3
        """
        
        order_qt_nested_types1_q11 """
            SELECT
                id,
                map_col,
                size(map_col) as map_size
            FROM nested_types1_orc
            WHERE id = 1
        """
        
        order_qt_nested_types1_q12 """
            SELECT
                id,
                map_col['a'] as a_value
            FROM nested_types1_orc
            WHERE id = 2
        """
        
        order_qt_nested_types1_q13 """
            SELECT
                id,
                nested_map_col['b'] as b_value
            FROM nested_types1_orc
            WHERE id = 3
        """
        
        order_qt_nested_types1_q14 """
            SELECT
                id,
                struct_element(struct_col, 'name') as name,
                struct_element(struct_col, 'age') as age
            FROM nested_types1_orc
            WHERE id = 1
        """
        
        order_qt_nested_types1_q15 """
            SELECT
                id,
                array_struct_col,
                size(array_struct_col) as struct_arr_size
            FROM nested_types1_orc
            WHERE id = 2
        """
        
        order_qt_nested_types1_q16 """
            SELECT
                id,
                struct_element(item, 'name') as name,
                struct_element(item, 'age') as age
            FROM nested_types1_orc
            LATERAL VIEW EXPLODE(array_struct_col) tmp AS item
            WHERE id = 1 AND struct_element(item, 'age') > 30
        """
        
        order_qt_nested_types1_q17 """
            SELECT
                id,
                struct_element(map_struct_col['a'], 'name') as name,
                struct_element(map_struct_col['a'], 'age') as age
            FROM nested_types1_orc
            WHERE id = 2
        """
        
        order_qt_nested_types1_q18 """
            SELECT
                id,
                struct_element(complex_struct_col, 'a') as array_a
            FROM nested_types1_orc
            WHERE id = 1
        """
        
        order_qt_nested_types1_q19 """
            SELECT
                id,
                struct_element(complex_struct_col, 'b') as map_b
            FROM nested_types1_orc
            WHERE id = 2
        """
        
        order_qt_nested_types1_q20 """
            SELECT
                id,
                struct_element(complex_struct_col, 'c') as struct_c
            FROM nested_types1_orc
            WHERE id = 3
        """
        
        order_qt_nested_types1_q21 """
            SELECT
                id,
                struct_element(struct_element(complex_struct_col, 'c'), 'x') as array_x
            FROM nested_types1_orc
            WHERE id = 1
        """
        
        order_qt_nested_types1_q22 """
            SELECT
                id,
                struct_element(struct_element(complex_struct_col, 'c'), 'y') as y_value
            FROM nested_types1_orc
            WHERE id = 2
        """
        
        order_qt_nested_types1_q23 """
            SELECT *
            FROM nested_types1_orc
            ORDER BY id
        """
        
        order_qt_nested_types1_q24 """
            SELECT id, array_col
            FROM nested_types1_orc
            WHERE array_contains(array_col, 1)
            ORDER BY id
        """
        
        order_qt_nested_types1_q25 """
            SELECT
                id,
                struct_element(struct_col, 'age') as age
            FROM nested_types1_orc
            WHERE struct_element(struct_col, 'name') = 'Alice'
        """
        
        order_qt_nested_types1_q26 """
            SELECT
                id,
                struct_element(struct_col, 'age') as age
            FROM nested_types1_orc
            WHERE struct_element(struct_col, 'name') LIKE '%A%'
        """

        sql """drop catalog ${catalog_name};"""
    }
}

