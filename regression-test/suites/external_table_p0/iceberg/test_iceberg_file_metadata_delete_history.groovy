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

suite("test_iceberg_file_metadata_delete_history",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("Iceberg test is disabled")
        return
    }

    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog = "iceberg_file_metadata_delete_history"
    String database = "iceberg_file_metadata_delete_history_db"
    def lazyThreshold = sql("SELECT @@topn_lazy_materialization_threshold")[0][0]

    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'rest',
        'uri' = 'http://${host}:${restPort}',
        's3.endpoint' = 'http://${host}:${minioPort}',
        's3.access_key' = 'admin', 's3.secret_key' = 'password',
        's3.region' = 'us-east-1')"""
    sql "SWITCH ${catalog}"
    sql "CREATE DATABASE IF NOT EXISTS ${database}"
    sql "USE ${database}"

    try {
        for (int version : [2, 3]) {
            for (String format : ["parquet", "orc"]) {
                String table = "metadata_v${version}_${format}"
                spark_iceberg "DROP TABLE IF EXISTS demo.${database}.${table}"
                spark_iceberg """CREATE TABLE demo.${database}.${table} (
                    id INT, batch INT, payload STRING) USING iceberg PARTITIONED BY (batch)
                    TBLPROPERTIES ('format-version' = '${version}',
                        'write.format.default' = '${format}',
                        'write.delete.mode' = 'merge-on-read',
                        'write.distribution-mode' = 'none')"""
                // A single writer keeps multiple rows in each file, so deleting two rows really
                // exercises holes in physical positions rather than removing one-row files.
                spark_iceberg """INSERT INTO demo.${database}.${table}
                    SELECT /*+ COALESCE(1) */ id, batch, payload FROM VALUES
                    (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd') AS v(id, batch, payload)"""
                spark_iceberg """INSERT INTO demo.${database}.${table}
                    SELECT /*+ COALESCE(1) */ id, batch, payload FROM VALUES
                    (5, 2, 'e'), (6, 2, 'f'), (7, 2, 'g'), (8, 2, 'h') AS v(id, batch, payload)"""
                sql "REFRESH DATABASE ${database}"
                sql "SET topn_lazy_materialization_threshold = -1"
                def before = sql("""SELECT id, batch, payload
                    FROM ${table} ORDER BY id""")
                def snapshot = sql("""SELECT snapshot_id FROM ${table}\$refs WHERE name = 'main'""")[0][0]
                sql "ALTER TABLE ${table} CREATE TAG before_delete AS OF VERSION ${snapshot}"

                // The release branch does not expose physical metadata columns; compare surviving
                // values against the same historical snapshot across both file formats.
                sql "DELETE FROM ${table} WHERE id IN (2, 4)"
                def survivors = before.findAll { !(it[0].toString().toInteger() in [2, 4]) }
                assertEquals(survivors, sql("""SELECT id, batch, payload
                    FROM ${table} ORDER BY id"""))
                spark_iceberg "REFRESH TABLE demo.${database}.${table}"
                assertSparkDorisResultEquals(
                        spark_iceberg("SELECT id, batch, payload FROM demo.${database}.${table} ORDER BY id"),
                        sql("SELECT id, batch, payload FROM ${table} ORDER BY id"))

                // Reusing the current scan's delete state for a historical relation loses rows.
                for (String relation : ["${table} FOR VERSION AS OF ${snapshot}",
                        "${table}@tag(before_delete)"]) {
                    assertEquals(before, sql("""SELECT id, batch, payload
                        FROM ${relation} ORDER BY id"""))
                }

                // Exercise late materialization after deletes when the predicate column is
                // absent from the final projection.
                for (String projection : ["payload", "batch", "payload, batch"]) {
                    String query = """SELECT ${projection} FROM ${table}
                        WHERE id > 1 ORDER BY id LIMIT 3"""
                    sql "SET topn_lazy_materialization_threshold = -1"
                    def eager = sql(query)
                    sql "SET topn_lazy_materialization_threshold = 10"
                    assertEquals(eager, sql(query))
                }

                // A partition predicate must preserve the delete mask for that partition.
                sql "SET topn_lazy_materialization_threshold = -1"
                assertEquals(survivors.findAll { it[1].toString() == "1" },
                        sql("""SELECT id, batch, payload
                            FROM ${table} WHERE batch = 1 ORDER BY id"""))
            }
        }
    } finally {
        sql "SET topn_lazy_materialization_threshold = ${lazyThreshold}"
    }
}
