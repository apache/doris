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

suite("test_deltalake_query", "p0,external,deltalake,external_docker,external_docker_deltalake") {
    String enabled = context.config.otherConfigs.get("enableDeltaLakeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Delta Lake test.")
        return
    }

    String hms_port = context.config.otherConfigs.get("deltalakeHmsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_deltalake_query"

    sql """drop catalog if exists ${catalog_name};"""

    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type' = 'deltalake',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            's3.endpoint' = 'http://${externalEnvIp}:19001',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.region' = 'us-east-1'
        );
    """

    sql """switch ${catalog_name};"""
    sql """set enable_fallback_to_original_planner = false;"""

    // The test expects the following pre-created tables in delta_test database:
    //   delta_test.delta_basic: id INT, name STRING, value DOUBLE
    //   delta_test.delta_partitioned: id INT, name STRING, dt STRING (partition col)
    //   delta_test.delta_with_dv: id INT, data STRING (has DV applied)

    sql """use delta_test;"""

    // Test basic scan
    order_qt_basic_select """select * from delta_basic order by id;"""

    // Test count(*) pushdown
    order_qt_count """select count(*) from delta_basic;"""

    // Test predicate pushdown
    order_qt_predicate """select * from delta_basic where id > 5 order by id;"""

    // Test partitioned table
    order_qt_partition """select * from delta_partitioned where dt = '2024-01-01' order by id;"""

    // Test table with deletion vectors
    order_qt_dv """select * from delta_with_dv order by id;"""

    // Test aggregation
    order_qt_agg """select count(*), sum(value), avg(value) from delta_basic;"""

    // Test GROUP BY
    order_qt_groupby """select name, count(*) as cnt from delta_basic group by name order by name;"""

    // Test column pruning (select subset of columns)
    order_qt_column_pruning """select id, name from delta_basic order by id limit 10;"""

    // Test IS NULL / IS NOT NULL
    order_qt_null """select count(*) from delta_basic where name is not null;"""

    // Test all data types
    order_qt_all_types """select * from delta_all_types order by col_int;"""

    // Test complex types
    order_qt_complex_types """select * from delta_complex_types order by id;"""

    // Clean up
    sql """switch internal;"""
    sql """drop catalog if exists ${catalog_name};"""
}
