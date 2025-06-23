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

suite("test_hive_runtime_filter_partition_pruning", "p0,external,hive,external_docker,external_docker_hive") {
    def test_runtime_filter_partition_pruning = {
        qt_runtime_filter_partition_pruning_decimal1 """
            select count(*) from decimal_partition_table where partition_col =
                (select partition_col from decimal_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_decimal2 """
            select count(*) from decimal_partition_table where partition_col in
                (select partition_col from decimal_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
        qt_runtime_filter_partition_pruning_decimal3 """
            select count(*) from decimal_partition_table where abs(partition_col) =
                (select partition_col from decimal_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_int1 """
            select count(*) from int_partition_table where partition_col =
                (select partition_col from int_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_int2 """
            select count(*) from int_partition_table where partition_col in
                (select partition_col from int_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
        qt_runtime_filter_partition_pruning_int3 """
            select count(*) from int_partition_table where abs(partition_col) =
                (select partition_col from int_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_string1 """
            select count(*) from string_partition_table where partition_col =
                (select partition_col from string_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_string2 """
            select count(*) from string_partition_table where partition_col in
                (select partition_col from string_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
        qt_runtime_filter_partition_pruning_date1 """
            select count(*) from date_partition_table where partition_col =
                (select partition_col from date_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_decimal2 """
            select count(*) from date_partition_table where partition_col in
                (select partition_col from date_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_hive_runtime_filter_partition_pruning"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`partition_tables`"""

            test_runtime_filter_partition_pruning()
        
        } finally {
        }
    }
}

