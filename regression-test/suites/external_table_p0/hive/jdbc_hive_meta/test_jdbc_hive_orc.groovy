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

suite("test_jdbc_hive_orc", "all_types,p0,external,hive,external_docker,external_docker_hive") {
    // Ensure that all types are parsed correctly
    def select_top50 = {
        qt_select_top50 """select * from orc_all_types order by int_col desc limit 50;"""
    }

    // Ensure that the null map of all types are parsed correctly
    def count_all = {
        qt_count_all """
        select p1_col, p2_col,
        count(tinyint_col),
        count(smallint_col),
        count(int_col),
        count(bigint_col),
        count(boolean_col),
        count(float_col),
        count(double_col),
        count(string_col),
        count(binary_col),
        count(timestamp_col),
        count(decimal_col),
        count(char_col),
        count(varchar_col),
        count(date_col),
        sum(size(list_double_col)),
        sum(size(list_string_col))
        from orc_all_types group by p1_col, p2_col
        order by p1_col, p2_col;
        """
    }

    // Ensure that the SearchArgument works well: LG
    def search_lg_int = {
        qt_search_lg_int """select count(int_col) from orc_all_types where int_col > 999613702;"""
    }

    // Ensure that the SearchArgument works well: IN
    def search_in_int = {
        qt_search_in_int """select count(int_col) from orc_all_types where int_col in (999742610, 999613702);"""
    }

    // Ensure that the SearchArgument works well: MIX
    def search_mix = {
        qt_search_mix """select int_col, decimal_col, date_col from orc_all_types where int_col > 995328433 and decimal_col > 988850.7929 and date_col > date '2018-08-25';"""
    }

    // only partition column selected
    def only_partition_col = {
        qt_only_partition_col """select count(p1_col), count(p2_col) from orc_all_types;"""
    }

    // decimals
    def decimals = {
        qt_decimals1 """select * from orc_decimal_table order by id;"""
        qt_decimals2 """select * from orc_decimal_table where id = 3 order by id;"""
        qt_decimals3 """select * from orc_decimal_table where id < 3 order by id;"""
        qt_decimals4 """select * from orc_decimal_table where id > 3 order by id;"""
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_jdbc_${hivePrefix}_orc"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hive_pg_port = context.config.otherConfigs.get(hivePrefix + "PgPort")


            String s3_endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
            
            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                    'type'='hms',
                    'hive.metastore.type' = 'jdbc',
                    "user"="hive",
                    "password"="hive",
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${hive_pg_port}/metastore?useSSL=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver"
                );
            """
            sql """use `${catalog_name}`.`default`"""

            select_top50()
            count_all()
            search_lg_int()
            search_in_int()
            search_mix()
            only_partition_col()
            decimals()

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

