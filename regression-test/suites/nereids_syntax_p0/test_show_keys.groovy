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

suite("test_show_keys") {
    sql """
        DROP TABLE IF EXISTS test_show_keys
    """

    sql """
        DROP TABLE IF EXISTS test_show_keys_v
    """

    sql """
        CREATE TABLE IF NOT EXISTS test_show_keys (
            c1 int
        ) DISTRIBUTED BY HASH(c1) PROPERTIES('replication_num'='1')
    """

    sql """
        CREATE VIEW IF NOT EXISTS test_show_keys_v AS SELECT * FROM test_show_keys
    """

    sql """
        SHOW KEYS FROM test_show_keys
    """

    sql """
        SHOW KEYS FROM test_show_keys_v
    """
}
