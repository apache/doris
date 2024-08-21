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

suite("hive_config_test", "p0,external,hive,external_docker,external_docker_hive") {
    String db_name = "regression_test_external_table_p0_hive"
    String internal_table = "hive_config_test"
    String catalog_name = "docker_hive"

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${internal_table} """
    sql """
    CREATE TABLE IF NOT EXISTS ${internal_table} (
        `id` INT NOT NULL,
        `name` STRING NOT NULL
        )
        DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
    """
    // insert data into interal table
    sql """ INSERT INTO ${internal_table} VALUES (1, 'doris'), (2, 'nereids'); """

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2"]) {
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
        // It's okay to use random `hdfsUser`, but can not be empty.
        def hdfsUserName = "doris"


        def test_outfile = {format, uri ->
            def res = sql """
                SELECT * FROM internal.${db_name}.${internal_table} t ORDER BY id
                INTO OUTFILE "${defaultFS}${uri}"
                FORMAT AS ${format}
                PROPERTIES (
                    "fs.defaultFS"="${defaultFS}",
                    "hadoop.username" = "${hdfsUserName}"
                );
            """

            def outfile_url = res[0][3]
            // check data correctness
            order_qt_check_outfile """ select * from hdfs(
                    "uri" = "${outfile_url}.${format}",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                """
        }


        // 1. test hive.recursive_directories table config
        test_outfile("orc", "/user/doris/suites/default/hive_recursive_directories_table/exp_")
        test_outfile("orc", "/user/doris/suites/default/hive_recursive_directories_table/1/exp_")
        test_outfile("orc", "/user/doris/suites/default/hive_recursive_directories_table/2/exp_")

        // test hive.recursive_directories_table = false
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hive.recursive_directories' = 'false'
        );"""
        sql """use `${catalog_name}`.`default`"""
        order_qt_1 """ select * from hive_recursive_directories_table order by id;"""

        // test hive.recursive_directories_table = true
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hive.recursive_directories' = 'true'
        );"""
        sql """ use `${catalog_name}`.`default` """
        order_qt_2 """ select * from hive_recursive_directories_table order by id; """

        // 2. test hive.ignore_absent_partitions-table
        test_outfile("orc", "/user/doris/suites/default/hive_ignore_absent_partitions_table/country=USA/city=NewYork/exp_")
        
        // test 'hive.ignore_absent_partitions' = 'true'
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hive.ignore_absent_partitions' = 'true'
        );"""
        sql """use `${catalog_name}`.`default`"""
        order_qt_3 """ select * from hive_ignore_absent_partitions_table order by id;"""


        // 'hive.ignore_absent_partitions' = 'false'
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hive.ignore_absent_partitions' = 'false'
        );"""
        sql """use `${catalog_name}`.`default`"""
        test {
            sql """ select * from hive_ignore_absent_partitions_table order by id;"""

            exception "Partition location does not exist"
        }
    }
}