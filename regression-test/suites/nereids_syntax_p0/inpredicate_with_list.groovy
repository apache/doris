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

suite("inpredicate_with_list") {
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
            (2, [4, 5, 6])
        """
    try {
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY id; """

        sql """ set enable_nereids_planner = true;"""
        sql """ set enable_fallback_to_original_planner=false;"""
        order_qt_in_predicate_17 """
            SELECT * FROM ${tableName} where c_array in ([1,2], [1,3]);
        """
        order_qt_in_predicate_18 """
            SELECT * FROM ${tableName} where c_array in (null, [1,3]);
        """
        order_qt_in_predicate_19 """
            SELECT * FROM ${tableName} where c_array in ([1,2], [1,3], [4,5,6]);
        """
        order_qt_in_predicate_20 """
            SELECT * FROM ${tableName} where c_array in ([1,2], null, [4,5,6]);
        """
        order_qt_in_predicate_21 """
            SELECT * FROM ${tableName} where c_array in ([1,2], null, [4,5,3]);
        """
        order_qt_in_predicate_22 """
            SELECT * FROM ${tableName} where c_array in ([1,2], [6,5,4], [4,5,3]);
        """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}

