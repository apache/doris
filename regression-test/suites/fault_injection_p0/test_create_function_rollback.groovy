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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_create_function_rollback", "docker") {
    def options = new ClusterOptions()
    options.feConfigs += [
        "enable_debug_points=true"
    ]
    options.cloudMode = false

    docker(options) {
        sql """DROP FUNCTION IF EXISTS create_fn_rollback_fn(INT)"""
        sql """DROP FUNCTION IF EXISTS create_fn_rollback_fn(BIGINT)"""
        sql """CREATE ALIAS FUNCTION create_fn_rollback_fn(INT) WITH PARAMETER(x) AS add(x, 1)"""

        try {
            GetDebugPoint().enableDebugPointForAllFEs(
                    "FunctionUtil.translateToNereidsThrows.exception", [execute: 1])
            test {
                sql """CREATE ALIAS FUNCTION create_fn_rollback_fn(BIGINT) WITH PARAMETER(x) AS add(x, 1)"""
                exception "debug point FunctionUtil.translateToNereidsThrows.exception"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("FunctionUtil.translateToNereidsThrows.exception")
        }

        test {
            sql """CREATE ALIAS FUNCTION create_fn_rollback_fn(INT) WITH PARAMETER(x) AS add(x, 1)"""
            exception "function already exists"
        }
        test {
            sql """DROP FUNCTION create_fn_rollback_fn(BIGINT)"""
            exception "function does not exist"
        }
    }
}
