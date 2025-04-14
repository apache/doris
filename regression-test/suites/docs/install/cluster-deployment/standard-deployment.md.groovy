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

import org.junit.jupiter.api.Assertions;

suite("docs/install/cluster-deployment/standard-deployment.md") {
    try {
        multi_sql """
        CREATE TABLE testdb.table_hash
        (
            k1 TINYINT,
            k2 DECIMAL(10, 2) DEFAULT "10.5",
            k3 VARCHAR(10) COMMENT "string column",
            k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
        )
        COMMENT "my first table"
        DISTRIBUTED BY HASH(k1) BUCKETS 32;
        """

        multi_sql """
        INSERT INTO testdb.table_hash VALUES
        (1, 10.1, 'AAA', 10),
        (2, 10.2, 'BBB', 20),
        (3, 10.3, 'CCC', 30),
        (4, 10.4, 'DDD', 40),
        (5, 10.5, 'EEE', 50);
        """

    } catch (Throwable t) {
        Assertions.fail("examples in docs/install/cluster-deployment/standard-deployment.md failed to exec, please fix it", t)
    }
}
