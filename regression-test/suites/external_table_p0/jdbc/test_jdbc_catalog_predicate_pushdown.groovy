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

// Regression tests for JDBC connector predicate pushdown correctness.
// Covers fixes:
//   P0-1: Partial predicate pushdown — only pushable predicates go to remote
//   Cast push toggle — date()/cast() pushed only when enabled

suite("test_jdbc_catalog_predicate_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String catalog_name = "jdbc_pred_pushdown_test"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """use ${catalog_name}.doris_test"""

        // ======================================================================
        // P0-1: Partial predicate pushdown correctness
        // When cast push is disabled, date()/cast() predicates must NOT appear
        // in the remote QUERY, while simple predicates (id=1) SHOULD be pushed.
        // ======================================================================

        sql "set enable_jdbc_cast_predicate_push_down = false;"

        // Case 1: Non-pushable predicate only
        // date(datetime_c) is NOT pushed to remote QUERY
        explain {
            sql("select * from test_cast where date(datetime_c) = '2022-01-01';")
            notContains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast` WHERE")
        }

        // Case 2: Mixed pushable + non-pushable predicates
        // Only id=1 is pushed to remote; date() stays local
        explain {
            sql("select * from test_cast where id = 1 and date(datetime_c) = '2022-01-01';")
            contains("WHERE (`id` = 1)")
            notContains("date(`datetime_c`)")
        }

        // Case 3: All predicates pushable (no cast)
        // Both predicates should be pushed to remote QUERY
        explain {
            sql("select * from test_cast where id = 1 and datetime_c != '2022-01-01 00:00:01';")
            contains("WHERE")
            contains("`id` = 1")
            contains("`datetime_c` != '2022-01-01 00:00:01'")
        }

        // Case 4: Single pushable predicate
        explain {
            sql("select * from test_cast where id = 1;")
            contains("WHERE (`id` = 1)")
        }

        // ======================================================================
        // Verify cast predicate push when enabled
        // ======================================================================
        sql "set enable_jdbc_cast_predicate_push_down = true;"

        // Case 5: With cast push enabled, date() IS pushed to remote
        explain {
            sql("select * from test_cast where date(datetime_c) = '2022-01-01';")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast` WHERE")
        }

        // Case 6: With cast push enabled, mixed predicates all pushed
        explain {
            sql("select * from test_cast where id = 1 and date(datetime_c) = '2022-01-01';")
            contains("WHERE")
            contains("`id` = 1")
        }

        // ======================================================================
        // Data correctness checks — verify query results via order_qt_
        // ======================================================================
        sql "set enable_jdbc_cast_predicate_push_down = false;"

        order_qt_jdbc_all """select * from test_cast order by id"""
        order_qt_jdbc_eq """select * from test_cast where id = 1 order by id"""
        order_qt_jdbc_mixed """select * from test_cast where id = 1 and datetime_c != '2022-01-01 00:00:01' order by id"""
        order_qt_jdbc_limit """select * from test_cast order by id limit 3"""
        order_qt_jdbc_eq_limit """select * from test_cast where id = 1 order by id limit 3"""

        sql """drop catalog if exists ${catalog_name}"""
    }
}
