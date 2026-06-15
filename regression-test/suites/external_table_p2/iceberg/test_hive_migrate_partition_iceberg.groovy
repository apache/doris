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

suite("test_hive_migrate_partition_iceberg",
        "p2,external,iceberg,external_remote,external_remote_iceberg") {

    String enabled = context.config.otherConfigs.get("enableExternalIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test enableExternalIcebergTest = false")
        return
    }

    enabled = context.config.otherConfigs.get("enableExternalEmrTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test enableExternalEmrTest = false")
        return
    }

    String props = context.config.otherConfigs.get("emrCatalogCommonProp")
    String catalogName = "test_hive_migrate_partition_iceberg"
    sql """drop catalog if exists ${catalogName}"""
    sql """create catalog if not exists ${catalogName} properties (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hms",
        "hive.version" = "3.1.3",
        ${props}
    )"""
    sql """switch ${catalogName}"""
    sql """use regression_iceberg"""

    def expectedRows = [
            [1, "a", "2024-01-01"],
            [2, "b", "2024-01-01"],
            [3, "c", "2024-01-02"],
            [4, "d", "2024-01-02"],
            [5, "e", "2024-01-03"],
    ]

    def verifyMigratedPartitionTable = { String tableName ->
        def rows = sql """select id, new_name, dt from ${tableName} order by id"""
        def actualRows = rows.collect { row ->
            [(row[0] as Integer), row[1].toString(), row[2].toString()]
        }
        assertEquals(expectedRows, actualRows)

        def partitionRows = sql """
                select id, new_name
                from ${tableName}
                where dt = '2024-01-02'
                order by id
        """
        def actualPartitionRows = partitionRows.collect { row ->
            [(row[0] as Integer), row[1].toString()]
        }
        assertEquals([[3, "c"], [4, "d"]], actualPartitionRows)

        def partitionSingleRows = sql """
                select id, new_name
                from ${tableName}
                where dt = '2024-01-03'
                order by id
        """
        def actualPartitionSingleRows = partitionSingleRows.collect { row ->
            [(row[0] as Integer), row[1].toString()]
        }
        assertEquals([[5, "e"]], actualPartitionSingleRows)

        def partitionRangeRows = sql """
                select id, dt
                from ${tableName}
                where dt >= '2024-01-02'
                order by id
        """
        def actualPartitionRangeRows = partitionRangeRows.collect { row ->
            [(row[0] as Integer), row[1].toString()]
        }
        assertEquals([
                [3, "2024-01-02"],
                [4, "2024-01-02"],
                [5, "2024-01-03"],
        ], actualPartitionRangeRows)

        def partitionInRows = sql """
                select id, dt
                from ${tableName}
                where dt in ('2024-01-01', '2024-01-03')
                order by id
        """
        def actualPartitionInRows = partitionInRows.collect { row ->
            [(row[0] as Integer), row[1].toString()]
        }
        assertEquals([
                [1, "2024-01-01"],
                [2, "2024-01-01"],
                [5, "2024-01-03"],
        ], actualPartitionInRows)

        def partitionAggRows = sql """
                select dt, count(*)
                from ${tableName}
                group by dt
                order by dt
        """
        def actualPartitionAggRows = partitionAggRows.collect { row ->
            [row[0].toString(), (row[1] as Long)]
        }
        assertEquals([
                ["2024-01-01", 2L],
                ["2024-01-02", 2L],
                ["2024-01-03", 1L],
        ], actualPartitionAggRows)

        def descRows = sql """desc ${tableName}"""
        def columnNames = descRows.collect { row -> row[0].toString().toLowerCase() }
        assertTrue(columnNames.contains("new_name"))
        assertTrue(!columnNames.contains("name"))
        assertTrue(columnNames.contains("dt"))
    }

    try {
        for (String tableName : ["hive_migrate_partition_parquet", "hive_migrate_partition_orc"]) {
            verifyMigratedPartitionTable(tableName)
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}

/*
Prepare data on EMR/Hive side before running this regression_iceberg case.

PARQUET:

create database regression_iceberg;
use regression_iceberg;
drop table if exists hive_migrate_partition_parquet;
create table hive_migrate_partition_parquet (
    id int,
    name string
) partitioned by (dt string)
stored as parquet;

insert into hive_migrate_partition_parquet partition (dt='2024-01-01') values
    (1, 'a'),
    (2, 'b');

insert into hive_migrate_partition_parquet partition (dt='2024-01-02') values
    (3, 'c');

call iceberg_emr.system.migrate(
    schema_name => 'regression_iceberg',
    table_name => 'hive_migrate_partition_parquet');

insert into iceberg_emr.regression_iceberg.hive_migrate_partition_parquet values
    (4, 'd', '2024-01-02');

alter table iceberg_emr.regression_iceberg.hive_migrate_partition_parquet
    rename column name to new_name;

insert into iceberg_emr.regression_iceberg.hive_migrate_partition_parquet values
    (5, 'e', '2024-01-03');

ORC:
use regression_iceberg;
drop table if exists hive_migrate_partition_orc;
create table hive_migrate_partition_orc (
    id int,
    name string
) partitioned by (dt string)
stored as orc;

insert into hive_migrate_partition_orc partition (dt='2024-01-01') values
    (1, 'a'),
    (2, 'b');

insert into hive_migrate_partition_orc partition (dt='2024-01-02') values
    (3, 'c');

call iceberg_emr.system.migrate(
    schema_name => 'regression_iceberg',
    table_name => 'hive_migrate_partition_orc');

insert into iceberg_emr.regression_iceberg.hive_migrate_partition_orc values
    (4, 'd', '2024-01-02');

alter table iceberg_emr.regression_iceberg.hive_migrate_partition_orc
    rename column name to new_name;

insert into iceberg_emr.regression_iceberg.hive_migrate_partition_orc values
    (5, 'e', '2024-01-03');
*/
