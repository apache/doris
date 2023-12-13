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

suite("test_hive_partitions", "p0,external,hive,external_docker,external_docker_hive") {
    def q01 = {
        qt_q01 """
        select id, data from table_with_pars where dt_par = '2023-02-01' order by id;
        """
        qt_q02 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00' order by id;
        """
        qt_q03 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00'
        and decimal_par1 = '1' order by id;
        """
        qt_q04 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00'
        and decimal_par1 = '1' and decimal_par2 = '1.2' order by id;
        """
        qt_q05 """
        select id, data from table_with_pars where dt_par = '2023-02-01' and time_par = '2023-02-01 01:30:00'
        and decimal_par1 = '1' and decimal_par2 = '1.2' and decimal_par3 = '1.22' order by id;
        """
        qt_q11 """
            show partitions from partition_table;
        """
        qt_q12 """
            show partitions from partition_table WHERE partitionName='nation=cn/city=beijing';
        """
        qt_q13 """
            show partitions from partition_table WHERE partitionName like 'nation=us/%';
        """
        qt_q14 """
            show partitions from partition_table WHERE partitionName like 'nation=%us%';
        """
        qt_q16 """
            show partitions from partition_table LIMIT 3;
        """
        qt_q17 """
            show partitions from partition_table LIMIT 3 OFFSET 2;
        """
        qt_q18 """
            show partitions from partition_table LIMIT 3 OFFSET 4;
        """
        qt_q19 """
            show partitions from partition_table ORDER BY partitionName desc LIMIT 3 OFFSET 2;
        """
        qt_q20 """
            show partitions from partition_table ORDER BY partitionName asc;
        """
        qt_q21 """
            show partitions from partition_table
                WHERE partitionName like '%X%'
                ORDER BY partitionName DESC
                LIMIT 1;
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            String catalog_name = "hive_test_partitions"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            q01()

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

