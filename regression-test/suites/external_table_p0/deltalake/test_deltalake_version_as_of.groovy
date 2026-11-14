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

import org.apache.doris.regression.util.JdbcUtils

import java.sql.DriverManager
import java.sql.Types

suite("test_deltalake_version_as_of", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String sparkPort = context.config.otherConfigs.get("deltalakeSparkPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    String table = "delta_version_as_of.`history`"
    Map<String, String> catalogs = [
            filesystem: "'type'='deltalake', 'deltalake.catalog.type'='filesystem', 'warehouse'='${warehouse}'",
            hms: "'type'='deltalake', 'deltalake.catalog.type'='hms', 'hive.metastore.uris'='thrift://${host}:${hmsPort}'",
            gateway: "'type'='hms', 'hive.metastore.uris'='thrift://${host}:${hmsPort}'",
    ]
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    def spark = DriverManager.getConnection("jdbc:hive2://${host}:${sparkPort}/default", "hadoop", "hadoop")
    try {
        def sparkSql = { String statement -> JdbcUtils.executeToList(spark, statement) }
        def version = { String name = table -> sparkSql("describe history ${name} limit 1")[0][0][0].toLong() }

        // Spark enforces retention on time travel. Create the commits for this run and keep them for inspection.
        sparkSql("create database if not exists delta_version_as_of location '${warehouse}/delta_version_as_of'")
        sparkSql("drop table if exists ${table}")
        sparkSql("""create table ${table} (id bigint, payload string, part string)
            using delta partitioned by (part)
            tblproperties ('delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'='true')""")
        sparkSql("""insert into ${table} select /*+ COALESCE(1) */ id, concat('row-', id),
            case when id=0 then 'gone' when id=7 then null else 'a/b' end from range(20)""")
        long inserted = version()

        // Reusing a logical name assigns a new column ID. Keep the original Parquet file across the change.
        String idMapped = "delta_version_as_of.readded_id"
        sparkSql("drop table if exists ${idMapped}")
        sparkSql("""create table ${idMapped} (id bigint, payload string) using delta
            tblproperties ('delta.columnMapping.mode'='id', 'delta.enableDeletionVectors'='false')""")
        sparkSql("insert into ${idMapped} values (1, 'old-value')")
        long originalColumn = version(idMapped)
        sparkSql("alter table ${idMapped} drop column payload")
        long droppedColumn = version(idMapped)
        sparkSql("alter table ${idMapped} add columns (payload bigint)")
        sparkSql("insert into ${idMapped} values (2, 10000000000)")
        long newColumn = version(idMapped)

        sql "set enable_sql_cache=false"
        catalogs.each { source, properties ->
            String catalog = "test_deltalake_version_as_of_${source}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties (
                ${properties}, 'meta.cache.deltalake.latest.ttl-second'='-1',
                's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
                's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
            )"""
            "qt_${source}_warm_latest"("select count(*) from ${catalog}.${table}")
        }

        sparkSql("delete from ${table} where id in (2,3)")
        long deleted = version()
        // Both deleted rows share a partially retained data file. Check that Spark actually wrote a DV.
        String commit = String.format("%020d.json", deleted)
        def dvFiles = sparkSql("""select count(*) from json.`${warehouse}/delta_version_as_of/history/_delta_log/${commit}`
            where add.deletionVector.cardinality > 0""")[0]
        assertTrue(dvFiles[0][0].toLong() > 0, "Spark did not write a deletion vector at version ${deleted}")
        sparkSql("delete from ${table} where part='gone'")
        sparkSql("alter table ${table} rename column payload to label")
        sparkSql("alter table ${table} rename column part to segment")
        sparkSql("insert into ${table} values (20, 'new-20', 'new'), (21, 'new-21', 'new')")
        long evolved = version()

        // Both drivers expose BIGINT directly. Hive STRING and Doris STRING can use different JDBC character types.
        def schema = { meta ->
            (1..meta.columnCount).collect { i ->
                int type = meta.getColumnType(i)
                [meta.getColumnLabel(i).tokenize('.').last(),
                        type in [Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR] ? Types.VARCHAR : type]
            }
        }
        def compareWithSpark = { String goldenTag = "", String comparedTable = table, Closure query ->
            def (expected, sparkMeta) = sparkSql(query(comparedTable, "version as of"))
            catalogs.each { source, ignored ->
                String name = "test_deltalake_version_as_of_${source}.${comparedTable}"
                String statement = query(name, "for version as of")
                def (actual, dorisMeta) = JdbcUtils.executeToList(context.getConnection(), statement)
                assertEquals(schema(sparkMeta), schema(dorisMeta), statement)
                assertSparkDorisResultEquals(expected, actual)
                if (goldenTag) {
                    quickTest("${source}_${goldenTag}", statement)
                }
            }
        }

        [original: originalColumn, dropped: droppedColumn, readded: newColumn].each { stage, snapshot ->
            compareWithSpark("id_${stage}", idMapped) { name, at ->
                "select * from ${name} ${at} ${snapshot} order by id"
            }
        }
        for (long snapshot : [0L, inserted, deleted, evolved]) {
            compareWithSpark { name, at -> "select * from ${name} ${at} ${snapshot} order by id" }
        }
        compareWithSpark("dropped_partition") { name, at ->
            "select id, payload from ${name} ${at} ${inserted} where part='gone' order by id"
        }
        compareWithSpark { name, at ->
            "select id, label from ${name} ${at} ${evolved} where segment is null or segment='new' order by id"
        }
        compareWithSpark { name, at -> """select o.id, o.payload, n.label
            from ${name} ${at} ${inserted} o left join ${name} ${at} ${evolved} n on o.id=n.id
            where o.id <= 4 order by o.id""" }
        // COUNT with and without a filter exercises metadata counting and decoding rows masked by the DV.
        for (boolean streaming : [false, true]) {
            sql "set enable_external_table_batch_mode=${streaming}"
            compareWithSpark(streaming ? "" : "dv_counts") { name, at ->
                """select 'before' as phase, count(*) as n from ${name} ${at} ${inserted}
                union all select 'after', count(*) from ${name} ${at} ${deleted}
                union all select 'masked', count(*) from ${name} ${at} ${deleted} where id in (2,3)
                order by phase""" }
        }

        catalogs.each { source, ignored ->
            String name = "test_deltalake_version_as_of_${source}.${table}"
            // Explicit lookups above succeeded for versions newer than the warmed, non-expiring latest pin.
            "qt_${source}_cached_latest"("select count(*) from ${name}")
            "qt_${source}_mixed_latest"("""select a.payload, b.label from ${name} a
                left join ${name} for version as of '${evolved}' b on a.id=b.id where a.id=2""")
            test {
                sql "select label from ${name} for version as of ${inserted}"
                exception "Unknown column 'label'"
            }
            test {
                sql "select payload from ${name} for version as of ${evolved}"
                exception "Unknown column 'payload'"
            }
            long future = evolved + 1
            test {
                sql "select * from ${name} for version as of ${future}"
                exception "Cannot load table version ${future} as it does not exist"
            }
            for (String badVersion : ["not_a_version", "-1", "9223372036854775808"]) {
                test {
                    sql "select * from ${name} for version as of '${badVersion}'"
                    exception "Delta Lake FOR VERSION AS OF requires a non-negative 64-bit integer"
                }
            }
            sql "refresh table ${name}"
            "qt_${source}_refreshed_latest"("select count(*) from ${name}")
        }
    } finally {
        spark.close()
    }
}
