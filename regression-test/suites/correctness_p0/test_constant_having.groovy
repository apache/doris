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

suite("test_constant_having") {
    sql """
        drop table if exists test_constant_having_t0;
    """
    
    sql """
        CREATE TABLE test_constant_having_t0(c0 VARCHAR(19) NOT NULL) DISTRIBUTED BY HASH (c0) PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO test_constant_having_t0 (c0) VALUES (1),(2);
    """

    qt_sql1 """
        SELECT
            CAST(DATE '1970-12-16' AS FLOAT),
            9998895.0,
            0.946221655
        FROM
            test_constant_having_t0
        GROUP BY
            test_constant_having_t0.c0
        HAVING
            (
                NOT (
                    CAST(false AS DATETIME) NOT IN (CAST(-994966193 AS DATETIME))
                )
            )
        ORDER BY
            1;
    """
    
    sql """
            drop table if exists test_constant_having_t0;
        """
}
