import com.google.common.collect.Lists

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

suite("test_paimon_partition_table", "p0,external") {
    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog_name = "test_paimon_partition_catalog"
    String db_name = "test_paimon_partition"
    List<String> tableList = new ArrayList<>(Arrays.asList("sales_by_date","sales_by_region",
            "sales_by_date_region","events_by_hour","logs_by_date_hierarchy"))
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    try {
        sql """drop catalog if exists ${catalog_name}"""

        sql """
                CREATE CATALOG ${catalog_name} PROPERTIES (
                        'type' = 'paimon',
                        'warehouse' = 's3://warehouse/wh',
                        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                        's3.access_key' = 'admin',
                        's3.secret_key' = 'password',
                        's3.path.style.access' = 'true'
                );
            """
        sql """switch `${catalog_name}`"""

        sql """use ${db_name}"""
        sql """refresh database ${db_name}"""
       tableList.eachWithIndex { String tableName, int index ->
           logger.info("show partitions command ${index + 1}: ${tableName}")
           String baseQueryName = "qt_show_partition_${tableName}"
           "$baseQueryName" """show partitions from ${tableName};"""
       }

        sql """set force_jni_scanner=true"""

        def salesByDateRows = sql """select sale_date from sales_by_date order by sale_date"""
        assertEquals(
                ["2024-01-15", "2024-01-15", "2024-01-16", "2024-01-16", "2024-01-17"],
                salesByDateRows.collect { row -> row[0].toString() })

        def salesByDateFilteredRows = sql """
                select sale_date
                from sales_by_date
                where sale_date = '2024-01-16'
                order by sale_date
        """
        assertEquals(
                ["2024-01-16", "2024-01-16"],
                salesByDateFilteredRows.collect { row -> row[0].toString() })

        def salesByRegionRows = sql """select region from sales_by_region order by region"""
        assertEquals(
                ["China-Beijing", "Japan-Tokyo", "USA-California"],
                salesByRegionRows.collect { row -> row[0].toString() })

        def salesByDateRegionRows = sql """
                select sale_date, region
                from sales_by_date_region
                order by sale_date, region
        """
        assertEquals(
                [
                        ["2024-01-15", "China-Beijing"],
                        ["2024-01-15", "Japan-Tokyo"],
                        ["2024-01-15", "USA-California"],
                        ["2024-01-16", "China-Shanghai"],
                        ["2024-01-16", "Japan-Osaka"],
                        ["2024-01-16", "USA-New York"],
                ],
                salesByDateRegionRows.collect { row -> [row[0].toString(), row[1].toString()] })

        def eventsByHourRows = sql """
                select hour_partition
                from events_by_hour
                where hour_partition in ('2024-01-15-10', '2024-01-15-14')
                order by hour_partition
        """
        assertEquals(
                ["2024-01-15-10", "2024-01-15-10", "2024-01-15-14", "2024-01-15-14"],
                eventsByHourRows.collect { row -> row[0].toString() })

        def logsByDateHierarchyRows = sql """
                select year_val, month_val, day_val
                from logs_by_date_hierarchy
                where year_val = 2024 and month_val = 1
                order by day_val, year_val, month_val
        """
        assertEquals(
                [
                        [2024, 1, 15],
                        [2024, 1, 15],
                        [2024, 1, 16],
                        [2024, 1, 16],
                        [2024, 1, 17],
                ],
                logsByDateHierarchyRows.collect { row ->
                    [(row[0] as Integer), (row[1] as Integer), (row[2] as Integer)]
                })

        sql """set force_jni_scanner=false"""
/*
mysql> show partitions from sales_by_date;
+----------------------+--------------+-------------+-----------------+-----------+
| Partition            | PartitionKey | RecordCount | FileSizeInBytes | FileCount |
+----------------------+--------------+-------------+-----------------+-----------+
| sale_date=2024-01-15 | sale_date    | 2           | 2051            | 1         |
| sale_date=2024-01-16 | sale_date    | 2           | 3899            | 2         |
| sale_date=2024-01-17 | sale_date    | 1           | 1959            | 1         |
+----------------------+--------------+-------------+-----------------+-----------+
3 rows in set (0.01 sec)
FileSizeInBytes maybe changed, when upgrade paimon version.
*/
    } finally {
         sql """set force_jni_scanner=false"""
         sql """drop catalog if exists ${catalog_name}"""
    }
}
