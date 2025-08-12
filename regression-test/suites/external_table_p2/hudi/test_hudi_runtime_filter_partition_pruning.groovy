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

suite("test_hudi_runtime_filter_partition_pruning", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_runtime_filter_partition_pruning"
    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """

    def test_runtime_filter_partition_pruning = {
        // Test BOOLEAN partition
        qt_runtime_filter_partition_pruning_boolean_1 """
            select count(*) from boolean_partition_tb where part1 =
                (select part1 from boolean_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_boolean_2 """
            select count(*) from boolean_partition_tb where part1 in
                (select part1 from boolean_partition_tb
                group by part1 having count(*) > 0);
        """

        // Test TINYINT partition
        qt_runtime_filter_partition_pruning_tinyint_1 """
            select count(*) from tinyint_partition_tb where part1 =
                (select part1 from tinyint_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_tinyint_2 """
            select count(*) from tinyint_partition_tb where part1 in
                (select part1 from tinyint_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """

        // Test SMALLINT partition
        qt_runtime_filter_partition_pruning_smallint_1 """
            select count(*) from smallint_partition_tb where part1 =
                (select part1 from smallint_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_smallint_2 """
            select count(*) from smallint_partition_tb where part1 in
                (select part1 from smallint_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """

        // Test INT partition
        qt_runtime_filter_partition_pruning_int_1 """
            select count(*) from int_partition_tb where part1 =
                (select part1 from int_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_int_2 """
            select count(*) from int_partition_tb where part1 in
                (select part1 from int_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """
        
        qt_runtime_filter_partition_pruning_int_3 """
            select count(*) from int_partition_tb where abs(part1) =
                (select part1 from int_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """

        // Test BIGINT partition
        qt_runtime_filter_partition_pruning_bigint_1 """
            select count(*) from bigint_partition_tb where part1 =
                (select part1 from bigint_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_bigint_2 """
            select count(*) from bigint_partition_tb where part1 in
                (select part1 from bigint_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """

        // Test STRING partition
        qt_runtime_filter_partition_pruning_string_1 """
            select count(*) from string_partition_tb where part1 =
                (select part1 from string_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_string_2 """
            select count(*) from string_partition_tb where part1 in
                (select part1 from string_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """

        // Test DATE partition
        qt_runtime_filter_partition_pruning_date_1 """
            select count(*) from date_partition_tb where part1 =
                (select part1 from date_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_date_2 """
            select count(*) from date_partition_tb where part1 in
                (select part1 from date_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """

        // Test TIMESTAMP partition
        qt_runtime_filter_partition_pruning_timestamp_1 """
            select count(*) from timestamp_partition_tb where part1 =
                (select part1 from timestamp_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 1);
        """
        
        qt_runtime_filter_partition_pruning_timestamp_2 """
            select count(*) from timestamp_partition_tb where part1 in
                (select part1 from timestamp_partition_tb
                group by part1 having count(*) > 0
                order by part1 desc limit 2);
        """

        // Additional complex scenarios with multiple filters
        qt_runtime_filter_partition_pruning_complex_1 """
            select count(*) from three_partition_tb t1 
            where t1.part1 in (
                select t2.part1 from three_partition_tb t2 
                where t2.part2 = 2024 
                group by t2.part1 having count(*) > 2
            );
        """
        
        qt_runtime_filter_partition_pruning_complex_2 """
            select count(*) from two_partition_tb t1 
            where t1.part1 = 'US' and t1.part2 in (
                select t2.part2 from two_partition_tb t2 
                where t2.part1 = 'US'
                group by t2.part2 having count(*) > 1
            );
        """
    }

    try {
        // Test with runtime filter partition pruning disabled
        sql """ set enable_runtime_filter_partition_prune = false; """
        test_runtime_filter_partition_pruning()
        
        // Test with runtime filter partition pruning enabled
        sql """ set enable_runtime_filter_partition_prune = true; """
        test_runtime_filter_partition_pruning()

    } finally {
        // Restore default setting
        sql """ set enable_runtime_filter_partition_prune = true; """
    }
}
