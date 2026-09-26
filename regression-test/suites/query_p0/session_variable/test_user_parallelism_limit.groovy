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

suite("test_user_parallelism_limit") {
    sql "DROP USER IF EXISTS 'test_user_parallelism_limit'"
    sql "CREATE USER 'test_user_parallelism_limit'"

    sql "SET PROPERTY FOR 'test_user_parallelism_limit' 'parallel_fragment_exec_instance_num' = '256'"
    qt_upper_bound "SHOW PROPERTY FOR 'test_user_parallelism_limit' LIKE 'parallel_fragment_exec_instance_num'"

    for (def value : [257, 2000, 2147483647]) {
        test {
            sql """SET PROPERTY FOR 'test_user_parallelism_limit'
                'max_user_connections' = '200', 'PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM' = '${value}'"""
            exception "parallel_fragment_exec_instance_num must be less than or equal to 256, got ${value}"
        }
    }
    qt_unchanged_parallelism "SHOW PROPERTY FOR 'test_user_parallelism_limit' LIKE 'parallel_fragment_exec_instance_num'"
    qt_unchanged_connections "SHOW PROPERTY FOR 'test_user_parallelism_limit' LIKE 'max_user_connections'"

    for (def value : [1, 0, -1, -2147483648]) {
        sql "SET PROPERTY FOR 'test_user_parallelism_limit' 'parallel_fragment_exec_instance_num' = '${value}'"
        "qt_value_${value}"("SHOW PROPERTY FOR 'test_user_parallelism_limit' LIKE 'parallel_fragment_exec_instance_num'")
    }
}
