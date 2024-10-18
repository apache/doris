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


suite("array_compare") {
    def tableName = "test_array"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        `id` INT NULL,
        `c_array` ARRAY<INT> NULL
        ) DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO ${tableName} (`id`,`c_array`) VALUES
            (1, [1, 2, 3]),
            (2, [4, 5, 6]),
            (3, []),
            (4, [1, 2, 3, 4]),
            (5, NULL)
        """
    try {
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY id; """

        sql """ set enable_nereids_planner = true;"""
        sql """ set enable_fallback_to_original_planner=false;"""
        order_qt_array_compare_1 """
            SELECT * FROM ${tableName} where c_array = [1, 2, 3];
        """
        order_qt_array_compare_2 """
            SELECT * FROM ${tableName} where c_array = [];
        """
        order_qt_array_compare_3 """
            SELECT * FROM ${tableName} where c_array != [1, 2, 3, 4];
        """
        order_qt_array_compare_4 """
            SELECT * FROM ${tableName} where c_array > [1, 2, 3];
        """
        order_qt_array_compare_5 """
            SELECT * FROM ${tableName} where c_array < [1, 2, 3, 4];
        """
        order_qt_array_compare_6 """
            SELECT * FROM ${tableName} where c_array = ['1', '2', '3'];
        """
        order_qt_array_compare_7 """
            SELECT * FROM ${tableName} where c_array != ["1", "2", "3", "4"];
        """
        order_qt_array_compare_8 """
            SELECT * FROM ${tableName} where c_array > [1.0, 2.0, 3.0];
        """
        order_qt_array_compare_9 """
            SELECT * FROM ${tableName} where c_array < [1.00001, 2.00001, 3.00001, 4.00001];
        """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

    qt_sql """select [1, 2] = ["1, 2"];"""
    qt_sql """select [1, 2, 3] = ['1', '2', '3'];"""
    qt_sql """select [1, 2, 3] = ["1", "2", "3"];"""
    qt_sql """select [1, 2, 3] = [1.0, 2.0, 3.0];"""
    qt_sql """select [1, 2, 3] = [1.000000000000000001, 2.000000000000000001, 3.000000000000000001];"""

    qt_sql """select [1, 2, 3] > ['1', '2'];"""
    qt_sql """select [1, 2, 3] < ["1", "2", "3"];"""
    qt_sql """select [1, 2, 3] != [1.0, 2.0, 3.0];"""

}
