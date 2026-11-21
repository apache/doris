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

suite("limit_join_lazy_materialization") {
    sql "DROP TABLE IF EXISTS limit_join_lazy_left"
    sql "DROP TABLE IF EXISTS limit_join_lazy_right"

    sql """
        CREATE TABLE limit_join_lazy_left (id INT, payload VARCHAR(200))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE limit_join_lazy_right (id INT, flag INT, payload VARCHAR(200))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO limit_join_lazy_left VALUES (1, 'left_1'), (2, 'left_2'), (3, 'left_3')"
    sql """
        INSERT INTO limit_join_lazy_right VALUES
            (1, 1, 'right_1'), (2, 1, 'right_2a'),
            (2, 1, 'right_2b'), (3, 0, 'right_3')
    """
    sql "sync"

    sql "set limit_join_lazy_materialization_threshold = 0"
    explain {
        sql """
            SELECT l.id, l.payload, r.payload FROM limit_join_lazy_left l
            JOIN limit_join_lazy_right r ON l.id = r.id WHERE r.flag = 1 LIMIT 3
        """
        notContains("VMaterializeNode")
    }
    order_qt_limit_join_off """
        SELECT l.id, l.payload, r.payload FROM limit_join_lazy_left l
        JOIN limit_join_lazy_right r ON l.id = r.id WHERE r.flag = 1 LIMIT 3
    """

    sql "set limit_join_lazy_materialization_threshold = 3"
    explain {
        sql """
            SELECT l.id, l.payload, r.payload FROM limit_join_lazy_left l
            JOIN limit_join_lazy_right r ON l.id = r.id WHERE r.flag = 1 LIMIT 3
        """
        contains("VMaterializeNode")
    }
    order_qt_limit_join_on """
        SELECT l.id, l.payload, r.payload FROM limit_join_lazy_left l
        JOIN limit_join_lazy_right r ON l.id = r.id WHERE r.flag = 1 LIMIT 3
    """

    explain {
        sql """
            SELECT l.id, l.payload, r.payload FROM limit_join_lazy_left l
            LEFT JOIN limit_join_lazy_right r ON l.id = r.id AND r.flag = 1 LIMIT 3
        """
        notContains("VMaterializeNode")
    }
}
