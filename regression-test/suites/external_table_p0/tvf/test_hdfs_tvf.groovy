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

suite("test_hdfs_tvf","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hdfs_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUserName = "doris"
    def format = "csv"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def uri = ""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {

            // test csv format
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/all_types.csv"
            format = "csv"
            qt_csv_all_types """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "${format}") order by c1; """


            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/student.csv"
            format = "csv"
            qt_csv_student """ select cast(c1 as INT) as id, c2 as name, c3 as age from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "${format}") order by id; """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/array_malformat.csv"
            format = "csv"
            qt_csv_array_malformat """ select * from HDFS(
                                        "uri" = "${uri}",
                                        "hadoop.username" = "${hdfsUserName}",
                                        "format" = "${format}",
                                        "column_separator" = "|") order by c1; """

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/array_normal.csv"
            format = "csv"
            qt_csv_array_normal """ select * from HDFS("uri" = "${uri}",
                                    "hadoop.username" = "${hdfsUserName}",
                                    "format" = "${format}",
                                    "column_separator" = "|") order by c1; """

            // test csv format with compress type
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/all_types_compressed.csv.gz"
            format = "csv"
            qt_csv_with_compress_type """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "column_separator" = ",",
                        "compress_type" = "GZ") order by c1; """

            // test csv format infer compress type
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/all_types_compressed.csv.gz"
            format = "csv"
            qt_csv_infer_compress_type """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "column_separator" = ",",
                        "format" = "${format}") order by c1; """

            // test csv_with_names file format
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/student_with_names.csv"
            format = "csv_with_names"
            qt_csv_names """ select cast(id as INT) as id, name, age from HDFS(
                            "uri" = "${uri}",
                            "hadoop.username" = "${hdfsUserName}",
                            "column_separator" = ",",
                            "format" = "${format}") order by id; """

            // test csv_with_names_and_types file format
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/csv_format_test/student_with_names_and_types.csv"
            format = "csv_with_names_and_types"
            qt_csv_names_types """ select cast(id as INT) as id, name, age from HDFS(
                                    "uri" = "${uri}",
                                    "hadoop.username" = "${hdfsUserName}",
                                    "column_separator" = ",",
                                    "format" = "${format}") order by id; """


            // test parquet
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/hdfs_tvf/test_parquet.snappy.parquet"
            format = "parquet"
            qt_parquet """ select * from HDFS(
                            "uri" = "${uri}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}") order by s_suppkey limit 20; """

            // test orc
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/hdfs_tvf/test_orc.snappy.orc"
            format = "orc"
            qt_orc """ select * from HDFS(
                            "uri" = "${uri}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}") order by p_partkey limit 20; """


            // test josn format
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/simple_object_json.json"
            format = "json"
            qt_json """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true") order by id; """


           uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/simple_object_json.json"
            format = "json"
            qt_json_limit1 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true") order by id limit 100; """

           uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/one_array_json.json"
            format = "json"
            qt_json_limit2 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "true",
                        "read_json_by_line" = "false") order by id limit 100; """
           uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/nest_json.json"
            format = "json"
            qt_json_limit3 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true") order by no  limit 100; """
           uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/nest_json.json"
            format = "json"
            qt_json_limit4 """ select * from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true") order by no limit 2; """


            // test json root
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/nest_json.json"
            format = "json"
            qt_json_root """ select cast(id as INT) as id, city, cast(code as INT) as code from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true",
                        "json_root" = "\$.item") order by id; """

            // test json paths
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/simple_object_json.json"
            format = "json"
            qt_json_paths """ select cast(id as INT) as id, cast(code as INT) as code from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true",
                        "jsonpaths" = "[\\"\$.id\\", \\"\$.code\\"]") order by id; """

            // test non read_json_by_line
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/one_array_json.json"
            format = "json"
            qt_one_array """ select cast(id as INT) as id, city, cast(code as INT) as code from HDFS(
                            "uri" = "${uri}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}",
                            "strip_outer_array" = "true",
                            "read_json_by_line" = "false") order by id; """


            // test cast to int
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/simple_object_json.json"
            format = "json"
            qt_cast """ select cast(id as INT) as id, city, cast(code as INT) as code from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true") order by id; """

            // test insert into select
            def testTable = "test_hdfs_tvf"
            sql "DROP TABLE IF EXISTS ${testTable}"
            def result1 = sql """ CREATE TABLE IF NOT EXISTS ${testTable}
                (
                    id int,
                    city varchar(50),
                    code int
                )
                COMMENT "test hdfs tvf table"
                DISTRIBUTED BY HASH(id) BUCKETS 32
                PROPERTIES("replication_num" = "1"); """

            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

            uri = "${defaultFS}" + "/user/doris/preinstalled_data/json_format_test/nest_json.json"
            format = "json"
            def result2 = sql """ insert into ${testTable}(id,city,code)
                    select cast (id as INT) as id, city, cast (code as INT) as code
                    from HDFS(
                        "uri" = "${uri}",
                        "hadoop.username" = "${hdfsUserName}",
                        "format" = "${format}",
                        "strip_outer_array" = "false",
                        "read_json_by_line" = "true",
                        "json_root" = "\$.item") """
            
            sql "sync"
            assertTrue(result2[0][0] == 5, "Insert should update 12 rows")
            qt_insert """ select * from test_hdfs_tvf order by id; """

            // test desc function
            uri = "${defaultFS}" + "/user/doris/preinstalled_data/hdfs_tvf/test_parquet.snappy.parquet"
            format = "parquet"
            qt_desc """ desc function HDFS(
                            "uri" = "${uri}",
                            "hadoop.username" = "${hdfsUserName}",
                            "format" = "${format}"); """
        } finally {
        }
    }
}
