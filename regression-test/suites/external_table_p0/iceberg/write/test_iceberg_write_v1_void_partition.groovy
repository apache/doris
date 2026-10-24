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

// A format-v1 Iceberg table replaces every partition field it drops with a void transform, and
// the source column of that field may be dropped afterwards. Doris must write such tables under
// the current spec, also after an upgrade to format v2 for row-level DML, and Spark must read
// what Doris wrote.
suite("test_iceberg_write_v1_void_partition",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_iceberg_write_v1_void_partition"
    String dbName = "iceberg_write_v1_void_partition_db"
    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """
    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    def checkRows = { String table, String columns, String predicate = "true" ->
        sql """refresh table ${dbName}.${table}"""
        spark_iceberg """refresh table demo.${dbName}.${table}"""
        assertSparkDorisResultEquals(
                spark_iceberg("""
                    select ${columns} from demo.${dbName}.${table} where ${predicate} order by id
                """),
                sql("""select ${columns} from ${table} where ${predicate} order by id"""))
    }

    def upgradeToMergeOnReadV2 = { String table ->
        spark_iceberg """
            alter table demo.${dbName}.${table} set tblproperties (
                'format-version' = '2',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read')
        """
        sql """refresh table ${dbName}.${table}"""
    }

    // Dropping the only partition field leaves spec 1 = [void(region)].
    spark_iceberg """drop table if exists demo.${dbName}.void_only"""
    spark_iceberg """
        create table demo.${dbName}.void_only (id int, region string, metric int)
        using iceberg partitioned by (region)
        tblproperties ('format-version' = '1', 'write.format.default' = 'parquet')
    """
    spark_iceberg """insert into demo.${dbName}.void_only values (1, 'a', 10), (2, 'b', 20)"""
    spark_iceberg """alter table demo.${dbName}.void_only drop partition field region"""
    sql """refresh database ${dbName}"""
    sql """insert into void_only values (3, 'a', 30), (4, 'c', 40)"""
    spark_iceberg """refresh table demo.${dbName}.void_only"""
    spark_iceberg """insert into demo.${dbName}.void_only values (5, 'd', 50)"""
    checkRows("void_only", "id, region, metric")
    order_qt_void_only_insert """select id, region, metric from void_only"""

    // A bare INSERT OVERWRITE replaces the whole table, so every remaining file uses spec 1.
    sql """insert overwrite table void_only select id, region, metric + 1 from void_only"""
    checkRows("void_only", "id, region, metric")
    order_qt_void_only_overwrite """select id, region, metric from void_only"""
    order_qt_void_only_overwrite_specs """select distinct spec_id from void_only\$data_files"""

    upgradeToMergeOnReadV2("void_only")
    sql """delete from void_only where id = 1"""
    sql """update void_only set metric = metric + 100 where id = 3"""
    sql """
        merge into void_only t
        using (select 4 as id, 'c' as region, 400 as metric union all select 6, 'f', 60) s
        on t.id = s.id
        when matched then update set metric = s.metric
        when not matched then insert (id, region, metric) values (s.id, s.region, s.metric)
    """
    checkRows("void_only", "id, region, metric")
    order_qt_void_only_dml """select id, region, metric from void_only"""
    order_qt_void_only_delete_specs """select distinct spec_id from void_only\$delete_files"""

    // The void field keeps referencing the dropped column region.
    spark_iceberg """drop table if exists demo.${dbName}.void_only_dropped_source"""
    spark_iceberg """
        create table demo.${dbName}.void_only_dropped_source (id int, region string, metric int)
        using iceberg partitioned by (region)
        tblproperties ('format-version' = '1', 'write.format.default' = 'orc')
    """
    spark_iceberg """
        insert into demo.${dbName}.void_only_dropped_source values (1, 'a', 10), (2, 'b', 20)
    """
    spark_iceberg """alter table demo.${dbName}.void_only_dropped_source drop partition field region"""
    spark_iceberg """alter table demo.${dbName}.void_only_dropped_source drop column region"""
    sql """refresh database ${dbName}"""
    sql """insert into void_only_dropped_source values (3, 30)"""
    spark_iceberg """refresh table demo.${dbName}.void_only_dropped_source"""
    spark_iceberg """insert into demo.${dbName}.void_only_dropped_source values (4, 40)"""
    checkRows("void_only_dropped_source", "id, metric")
    order_qt_void_only_dropped_source_insert """select id, metric from void_only_dropped_source"""

    // Rewriting the historical files under spec 1 also clears their non-null region values, which
    // row-level DML could not carry into delete files once the column is gone.
    sql """insert overwrite table void_only_dropped_source select id, metric + 1 from void_only_dropped_source"""
    checkRows("void_only_dropped_source", "id, metric")
    order_qt_void_only_dropped_source_overwrite """select id, metric from void_only_dropped_source"""

    upgradeToMergeOnReadV2("void_only_dropped_source")
    sql """delete from void_only_dropped_source where id = 1"""
    sql """update void_only_dropped_source set metric = metric + 100 where id = 3"""
    sql """
        merge into void_only_dropped_source t
        using (select 4 as id, 400 as metric union all select 6, 60) s
        on t.id = s.id
        when matched then update set metric = s.metric
        when not matched then insert (id, metric) values (s.id, s.metric)
    """
    checkRows("void_only_dropped_source", "id, metric")
    order_qt_void_only_dropped_source_dml """select id, metric from void_only_dropped_source"""

    // Spec 1 = [void(region), identity(category)] after region is dropped: new files must carry
    // a null region and their category value. Spark cannot write this table once region is gone
    // (its partition key needs an accessor for every source field), but it reads what Doris writes.
    spark_iceberg """drop table if exists demo.${dbName}.void_and_identity"""
    spark_iceberg """
        create table demo.${dbName}.void_and_identity (
            id int, region string, category string, metric int)
        using iceberg partitioned by (region, category)
        tblproperties ('format-version' = '1', 'write.format.default' = 'parquet')
    """
    spark_iceberg """
        insert into demo.${dbName}.void_and_identity values (1, 'a', 'x', 10), (2, 'b', 'y', 20)
    """
    spark_iceberg """alter table demo.${dbName}.void_and_identity drop partition field region"""
    spark_iceberg """alter table demo.${dbName}.void_and_identity drop column region"""
    sql """refresh database ${dbName}"""
    sql """insert into void_and_identity values (3, 'x', 30), (4, 'z', 40), (5, 'z', 50)"""
    checkRows("void_and_identity", "id, category, metric")
    // Spark prunes on the identity field, so it only finds Doris rows with the right partition value.
    checkRows("void_and_identity", "id, category, metric", "category = 'z'")
    order_qt_void_and_identity_insert """select id, category, metric from void_and_identity"""

    sql """insert overwrite table void_and_identity select id, category, metric + 1 from void_and_identity"""
    checkRows("void_and_identity", "id, category, metric")
    checkRows("void_and_identity", "id, category, metric", "category = 'x'")
    order_qt_void_and_identity_overwrite """select id, category, metric from void_and_identity"""

    upgradeToMergeOnReadV2("void_and_identity")
    sql """delete from void_and_identity where id = 1"""
    sql """update void_and_identity set metric = metric + 100 where category = 'z'"""
    sql """
        merge into void_and_identity t
        using (select 2 as id, 'y' as category, 200 as metric
               union all select 6, 'w', 60) s
        on t.id = s.id
        when matched then update set metric = s.metric
        when not matched then insert (id, category, metric) values (s.id, s.category, s.metric)
    """
    checkRows("void_and_identity", "id, category, metric")
    checkRows("void_and_identity", "id, category, metric", "category = 'w'")
    order_qt_void_and_identity_dml """select id, category, metric from void_and_identity"""
    order_qt_void_and_identity_delete_specs """select distinct spec_id from void_and_identity\$delete_files"""
}
