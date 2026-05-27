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

import java.util.Random

suite("test_iceberg_v3_row_lineage_random_cross_engine", "p2,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String randomEnabled = context.config.otherConfigs.get("enableIcebergRowLineageRandomCross")
    if (randomEnabled == null || !randomEnabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg row lineage random cross-engine test is disabled")
        return
    }

    String catalogName = "test_iceberg_v3_row_lineage_random_cross_engine"
    String dbName = "test_row_lineage_random_cross_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String endpoint = "http://${externalEnvIp}:${minioPort}"

    int rounds = (context.config.otherConfigs.get("icebergRowLineageRandomRounds") ?: "20").toInteger()
    long seed = (context.config.otherConfigs.get("icebergRowLineageRandomSeed") ?: "61398").toLong()
    def formats = ["parquet", "orc"]

    def hasSparkIcebergJdbc = {
        try {
            spark_iceberg_jdbc """select 1"""
            return true
        } catch (Exception e) {
            logger.info("Check spark-iceberg JDBC failed: ${e.message}")
            return false
        }
    }

    def dorisChecksum = { tableName ->
        def rows = sql("""
            select count(*), coalesce(sum(id), 0), coalesce(sum(score), 0)
            from ${tableName}
        """)
        return rows[0].collect { it.toString() }.join(",")
    }

    def sparkChecksum = { tableName ->
        def rows = spark_iceberg_jdbc("""
            select concat_ws(',', cast(count(*) as string),
                cast(coalesce(sum(id), 0) as string),
                cast(coalesce(sum(score), 0) as string))
            from demo.${dbName}.${tableName}
        """)
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    def assertDorisSparkBusinessEqual = { tableName, step ->
        String doris = dorisChecksum(tableName)
        String spark = sparkChecksum(tableName)
        logger.info("Random cross step ${step} checksum for ${tableName}: Doris=${doris}, Spark=${spark}")
        assertEquals(doris, spark)
    }

    def assertLineageReadable = { tableName, step ->
        def rows = sql("""
            select count(*), count(_row_id), count(_last_updated_sequence_number)
            from ${tableName}
        """)
        logger.info("Random cross step ${step} lineage counts for ${tableName}: ${rows}")
        assertEquals(rows[0][0].toString().toLong(), rows[0][1].toString().toLong())
        assertEquals(rows[0][0].toString().toLong(), rows[0][2].toString().toLong())
    }

    def logSnapshots = { tableName, step ->
        def snapshots = sql("""
            select snapshot_id, operation
            from ${tableName}\$snapshots
            order by committed_at
        """)
        logger.info("Random cross step ${step} snapshots for ${tableName}: ${snapshots}")
    }

    def rowValues = { id, prefix, score ->
        int monthDay = (id % 9) + 1
        return "${id}, '${prefix}_${id}', ${score}, date '2024-10-${String.format('%02d', monthDay)}'"
    }

    def executeDorisStep = { tableName, op, id, step ->
        String sqlText
        if (op == 0) {
            sqlText = "insert into ${tableName} values (${rowValues(id, "doris_i_${step}", id * 10 + step)})"
        } else if (op == 1) {
            sqlText = "update ${tableName} set name = concat(name, '_du${step}'), score = score + 1 where id = ${id}"
        } else if (op == 2) {
            sqlText = "delete from ${tableName} where id = ${id}"
        } else if (op == 3) {
            int updateId = id
            int deleteId = ((id + 1) % 30) + 1
            int insertId = 10000 + step
            sqlText = """
                merge into ${tableName} t
                using (
                    select ${updateId} as id, 'doris_mu_${step}' as name, ${updateId * 10 + step} as score,
                        date '2024-10-${String.format('%02d', (updateId % 9) + 1)}' as dt, 'U' as flag
                    union all
                    select ${deleteId}, 'unused', 0,
                        date '2024-10-${String.format('%02d', (deleteId % 9) + 1)}', 'D'
                    union all
                    select ${insertId}, 'doris_mi_${step}', ${insertId + step},
                        date '2024-10-${String.format('%02d', (insertId % 9) + 1)}', 'I'
                ) s
                on t.id = s.id
                when matched and s.flag = 'D' then delete
                when matched then update set name = s.name, score = s.score
                when not matched then insert (id, name, score, dt)
                values (s.id, s.name, s.score, s.dt)
            """
        } else {
            sqlText = """
                alter table ${catalogName}.${dbName}.${tableName}
                execute rewrite_data_files(
                    "target-file-size-bytes" = "10485760",
                    "min-input-files" = "1"
                )
            """
        }
        logger.info("Random cross step ${step} Doris SQL for ${tableName}: ${sqlText}")
        sql sqlText
    }

    def executeSparkStep = { tableName, op, id, step ->
        String sqlText
        if (op == 0) {
            sqlText = "insert into demo.${dbName}.${tableName} values (${rowValues(id, "spark_i_${step}", id * 10 + step)})"
        } else if (op == 1) {
            sqlText = "update demo.${dbName}.${tableName} set name = concat(name, '_su${step}'), score = score + 1 where id = ${id}"
        } else if (op == 2) {
            sqlText = "delete from demo.${dbName}.${tableName} where id = ${id}"
        } else if (op == 3) {
            int updateId = id
            int deleteId = ((id + 1) % 30) + 1
            int insertId = 20000 + step
            sqlText = """
                merge into demo.${dbName}.${tableName} t
                using (
                    select ${updateId} as id, 'spark_mu_${step}' as name, ${updateId * 10 + step} as score,
                        date '2024-10-${String.format('%02d', (updateId % 9) + 1)}' as dt, 'U' as flag
                    union all
                    select ${deleteId}, 'unused', 0,
                        date '2024-10-${String.format('%02d', (deleteId % 9) + 1)}', 'D'
                    union all
                    select ${insertId}, 'spark_mi_${step}', ${insertId + step},
                        date '2024-10-${String.format('%02d', (insertId % 9) + 1)}', 'I'
                ) s
                on t.id = s.id
                when matched and s.flag = 'D' then delete
                when matched then update set name = s.name, score = s.score
                when not matched then insert (id, name, score, dt)
                values (s.id, s.name, s.score, s.dt)
            """
        } else {
            sqlText = """
                call demo.system.rewrite_data_files(
                    table => '${dbName}.${tableName}',
                    options => map('target-file-size-bytes', '10485760', 'min-input-files', '1')
                )
            """
        }
        logger.info("Random cross step ${step} Spark SQL for ${tableName}: ${sqlText}")
        spark_iceberg_jdbc sqlText
    }

    if (!hasSparkIcebergJdbc()) {
        logger.info("spark-iceberg JDBC is unavailable, skip random cross-engine branch")
        return
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "us-east-1"
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""
    sql """set show_hidden_columns = false"""

    try {
        spark_iceberg_jdbc """create database if not exists demo.${dbName}"""

        formats.each { format ->
            String tableName = "random_cross_${format}"
            Random random = new Random(seed + format.hashCode())
            try {
                spark_iceberg_jdbc_multi """
                    drop table if exists demo.${dbName}.${tableName};
                    create table demo.${dbName}.${tableName} (
                        id int,
                        name string,
                        score int,
                        dt date
                    ) using iceberg
                    partitioned by (days(dt))
                    tblproperties (
                        'format-version' = '3',
                        'write.format.default' = '${format}',
                        'write.delete.mode' = 'merge-on-read',
                        'write.update.mode' = 'merge-on-read',
                        'write.merge.mode' = 'merge-on-read'
                    );
                    insert into demo.${dbName}.${tableName} values
                    (1, 'base_1', 10, date '2024-10-01'),
                    (2, 'base_2', 20, date '2024-10-02'),
                    (3, 'base_3', 30, date '2024-10-03'),
                    (4, 'base_4', 40, date '2024-10-04'),
                    (5, 'base_5', 50, date '2024-10-05');
                """
                sql """refresh table ${dbName}.${tableName}"""
                assertDorisSparkBusinessEqual(tableName, "initial")
                assertLineageReadable(tableName, "initial")

                for (int step = 1; step <= rounds; step++) {
                    boolean useDoris = random.nextBoolean()
                    int op = random.nextInt(5)
                    int id = random.nextInt(30) + 1
                    if (useDoris) {
                        executeDorisStep(tableName, op, id, step)
                    } else {
                        executeSparkStep(tableName, op, id, step)
                    }
                    sql """refresh table ${dbName}.${tableName}"""
                    assertDorisSparkBusinessEqual(tableName, step)
                    assertLineageReadable(tableName, step)
                    logSnapshots(tableName, step)
                }
            } finally {
                sql """drop table if exists ${tableName}"""
            }
        }
    } finally {
        sql """drop database if exists ${dbName} force"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
