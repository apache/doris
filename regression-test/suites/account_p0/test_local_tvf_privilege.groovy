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

suite("test_local_tvf_privilege", "p0") {
    def user = "local_tvf_privilege_user"
    sql "DROP USER IF EXISTS '${user}'"
    sql "CREATE USER '${user}' IDENTIFIED BY 'LocalTvf123!'"
    sql "GRANT SELECT_PRIV ON *.*.* TO '${user}'"

    if (isCloudMode()) {
        def cluster = (sql "SHOW CLUSTERS")[0][0]
        sql "GRANT USAGE_PRIV ON CLUSTER `${cluster}` TO '${user}'"
    }

    connect(user, "LocalTvf123!", context.config.jdbcUrl) {
        for (def functionName : ["LOCAL", "FILE"]) {
            // Reject the user before attempting to resolve the file or its schema.
            for (def prefix : ["", "EXPLAIN "]) {
                test {
                    sql """${prefix}SELECT * FROM ${functionName}(
                        "file_path" = "local_tvf_privilege_test.csv",
                        "backend_id" = "0",
                        "format" = "csv"
                    )"""
                    exception "ADMIN/OPERATOR"
                }
            }
        }
    }
}
