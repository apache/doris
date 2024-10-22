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

suite("test_jdbc_catalog_push_cast", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists jdbc_catalog_push_cast """
        sql """create catalog if not exists jdbc_catalog_push_cast properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql "use jdbc_catalog_push_cast.doris_test"

        qt_sql """select * from test_cast where date(datetime_c) = '2022-01-01';"""

        explain {
            sql("select * from test_cast where date(datetime_c) = '2022-01-01';")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where datetime_c = now();")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where datetime_c = cast(cast('2022-01-01 00:00:01' as datetime) as string);")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast` WHERE ((`datetime_c` = '2022-01-01 00:00:01'))")
        }

        explain {
            sql("select * from test_cast where cast(datetime_c as datetime) = cast('2022-01-01 00:00:01' as datetime);")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where date_c = cast(cast('2022-01-01 00:00:01' as datetime) as date)")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where datetime_c = date_c;")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where datetime_c = '2022-01-01';")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where cast(datetime_c as string) = '2022-01-01 00:00:01';")
            contains("SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where datetime_c != '2022-01-01 00:00:01';")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast` WHERE ((`datetime_c` != '2022-01-01 00:00:01'))")
        }

        explain {
            sql("select * from test_cast where datetime_c in (cast('2022-01-01' as datetime), cast('2022-02-01' as datetime));")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast`")
        }

        explain {
            sql("select * from test_cast where date_c = cast(cast('2022-01-01 00:00:01' as datetime) as date) and id = 1;")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast` WHERE ((`id` = 1))")
        }

        sql "set enable_jdbc_cast_predicate_push_down = true;"

        explain {
            sql("select * from test_cast where cast(datetime_c as datetime) = cast('2022-01-01 00:00:01' as datetime);")
            contains("QUERY: SELECT `id`, `int_c`, `date_c`, `datetime_c` FROM `doris_test`.`test_cast` WHERE (`datetime_c` = '2022-01-01 00:00:01')")
        }

        sql """drop catalog if exists jdbc_catalog_push_cast """
    }
}
