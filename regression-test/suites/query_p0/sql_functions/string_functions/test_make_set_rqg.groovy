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

suite("test_make_set_rqg") {


    sql "drop table if exists test_make_set_rqg;"

    sql """
         CREATE TABLE test_make_set_rqg (id INT, col1 DOUBLE, col2 VARCHAR(50), col3 DECIMAL(10,2), col4 DATETIME, col5 INT) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """


    sql """
        INSERT INTO test_make_set_rqg (id, col1, col2, col3, col4, col5) VALUES (1, 123.456, 'test_string', 999.99, '2023-01-01 12:00:00', 100), (2, 999999999.999, 'boundary_value', 99999999.99, '9999-12-31 23:59:59', 2147483647), (3, -999999999.999, 'negative_boundary', -99999999.99, '1000-01-01 00:00:00', -2147483648), (4, 0.0, 'zero_value', 0.00, '2023-06-15 18:30:45', 0), (5, NULL, 'null_value', NULL, NULL, NULL);
    """

    qt_sql """
        SELECT id, make_set(1, col2, 'default1', 'default2') FROM test_make_set_rqg order by id;
    """

}
