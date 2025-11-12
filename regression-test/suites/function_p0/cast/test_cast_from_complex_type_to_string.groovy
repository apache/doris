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

suite("test_cast_from_complex_type_to_string") {
    
    sql """
        DROP TABLE IF EXISTS complex_type_cast_to_string;
    """

    sql """
        CREATE TABLE IF NOT EXISTS complex_type_cast_to_string (
              `id` INT(11) NUll COMMENT "",
              `arr` array<INT> NUll COMMENT "",
              `mp` array<String>  NUll COMMENT "",
              `sr` STRUCT<f1:INT,f2:FLOAT,f3:STRING> NUll COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """

    sql """ 
        INSERT INTO complex_type_cast_to_string VALUES 
        (1, [1,2,3], ['a','b','c'], '{"f1":10,"f2":20.5,"f3":"hello"}'),
        (2, [4,5,6], ['d','e','f'], '{"f1":30,"f2":40.5,"f3":"world"}');
    """


    qt_select_arr """ 
        SELECT CAST(arr AS STRING) AS arr_str FROM complex_type_cast_to_string ORDER BY id;
    """


    qt_select_arr """
        select cast(array(1,2,3,4) as string);
    """

    qt_select_arr """
        select cast(array(array(1,2),array(3,4)) as string);
    """

    qt_select_map """
        select cast(map("abc",123,"def",456) as string);
    """

    qt_select_struct """
        select cast(struct(123,"abc",3.14) as string);
    """

}
