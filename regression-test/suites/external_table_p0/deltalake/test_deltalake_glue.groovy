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

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.doris.regression.util.JdbcUtils

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger

suite("test_deltalake_glue", "p0,external,deltalake,external_docker") {
    def cfg = context.config.otherConfigs
    if (!"true".equalsIgnoreCase(cfg.get("enableDeltaLakeTest"))) {
        return
    }
    String host = cfg.get("externalEnvIp")
    String warehouse = cfg.get("deltalakeWarehouse")
    String changes = warehouse + "/delta_glue_p0/data_" + UUID.randomUUID().toString().replace("-", "")
    String account = "123456789012"
    String access = "GLUE_TEST_ACCESS_MARKER"
    String secret = "GLUE_TEST_SECRET_MARKER"
    String token = "GLUE_TEST_SESSION_MARKER"
    def tables = [
        events: [Parameters: ["SPARK.SQL.SOURCES.PROVIDER": "DeLtA"],
                 StorageDescriptor: [Location: warehouse + "/events-__PLACEHOLDER__", SerdeInfo: [Parameters: [path: changes]]]],
        mapped: [Parameters: [Table_Type: "DELTA"], StorageDescriptor: [Location: warehouse + "/delta_m1/mapping_name"]],
        deleted: [Parameters: [Classification: "delta"], StorageDescriptor: [Location: warehouse + "/delta_m1/dv"]],
        manifest: [Parameters: [table_type: "DELTA"], StorageDescriptor: [Location: changes + "/_symlink_format_manifest"]],
        missing_root: [Parameters: [classification: "delta"], StorageDescriptor: [Location: "-__PLACEHOLDER__"]],
        iceberg_table: [Parameters: [table_type: "ICEBERG"], StorageDescriptor: [Location: changes]],
        hive_table: [Parameters: [classification: "parquet"], StorageDescriptor: [Location: changes]]
    ].collectEntries { name, definition -> [(name): [Name: name, DatabaseName: "glue_db"] + definition] }

    sql "drop materialized view if exists delta_glue_mtmv"
    def calls = new AtomicInteger()
    HttpServer glue = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)
    glue.createContext("/", { HttpExchange exchange ->
        try {
            calls.incrementAndGet()
            String target = exchange.requestHeaders.getFirst("X-Amz-Target")
            String signature = exchange.requestHeaders.getFirst("Authorization")
            def request = new JsonSlurper().parse(exchange.requestBody)
            def reply
            int status = 200
            if (!signature?.contains("Credential=" + access + "/")
                    || exchange.requestHeaders.getFirst("X-Amz-Security-Token") != token || request.CatalogId != account) {
                reply = [__type: "AccessDeniedException", Message: "Expected the configured Glue identity and catalog"]
                status = 400
            } else if (target == "AWSGlue.GetDatabases") {
                reply = request.NextToken == null ? [DatabaseList: [[Name: "glue_db"]], NextToken: "db-2"] :
                        [DatabaseList: [[Name: "glue_extra"]]]
            } else if (target == "AWSGlue.GetTables" && request.DatabaseName == "glue_db") {
                reply = request.NextToken == null ?
                        [TableList: [tables.events, tables.hive_table, tables.mapped], NextToken: "tables-2"] :
                        [TableList: [tables.deleted, tables.iceberg_table, tables.manifest, tables.missing_root]]
            } else if (target == "AWSGlue.GetTable" && request.DatabaseName == "glue_db" && tables.containsKey(request.Name)) {
                reply = [Table: tables[request.Name]]
            } else {
                reply = [__type: "EntityNotFoundException", Message: "No such Glue registration"]
                status = 400
            }
            byte[] bytes = JsonOutput.toJson(reply).getBytes(StandardCharsets.UTF_8)
            exchange.responseHeaders.set("Content-Type", "application/x-amz-json-1.1")
            if (status != 200) {
                exchange.responseHeaders.set("X-Amzn-Errortype", reply.__type.toString())
            }
            exchange.sendResponseHeaders(status, bytes.length)
            exchange.responseBody.write(bytes)
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    glue.start()
    try {
        String glueHost = cfg.get("httpTvfHost") ?: host
        String endpoint = "http://${glueHost}:${glue.address.port}"
        String storage = warehouse.startsWith("file:") ? "" : """,
            's3.endpoint'='http://${host}:${cfg.get("deltalakeMinioPort")}','s3.region'='us-east-1',
            's3.access_key'='admin','s3.secret_key'='password','use_path_style'='true'"""
        for (boolean aliases in [false, true]) {
            String catalog = aliases ? "test_deltalake_glue_aliases" : "test_deltalake_glue"
            String ep = aliases ? "aws.endpoint" : "glue.endpoint"
            String region = aliases ? "aws.glue.region" : "glue.region"
            String ak = aliases ? "aws.glue.access-key" : "glue.access_key"
            String sk = aliases ? "aws.glue.secret-key" : "glue.secret_key"
            String st = aliases ? "aws.glue.session-token" : "glue.session_token"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties ('type'='deltalake','deltalake.catalog.type'='glue',
                '${ep}'='${endpoint}','${region}'='us-east-1','glue.catalog_id'='${account}',
                '${ak}'='${access}','${sk}'='${secret}','${st}'='${token}' ${storage})"""
            String ddl = sql("show create catalog ${catalog}")[0][1].toString()
            [ak, sk, st].each { key -> assertTrue(ddl.contains('"' + key + '" = "*XXX"'), key) }
            [access, secret, token].each { marker -> assertFalse(ddl.contains(marker)) }
        }
        assertEquals(0, calls.get())
        order_qt_databases "show databases from test_deltalake_glue like 'glue_%'"
        order_qt_tables "show tables from test_deltalake_glue.glue_db"
        order_qt_aliases "show databases from test_deltalake_glue_aliases like 'glue_%'"

        Class.forName("org.apache.hive.jdbc.HiveDriver")
        def spark = DriverManager.getConnection("jdbc:hive2://${host}:${cfg.get('deltalakeSparkPort')}/default", "hadoop", "hadoop")
        try {
            def write = { String statement -> JdbcUtils.executeToList(spark, statement) }
            write("set spark.sql.session.timeZone=UTC")
            write("create database if not exists delta_glue_p0 location '${warehouse}/delta_glue_p0'")
            write("drop table if exists delta_glue_p0.events")
            write("""create table delta_glue_p0.events(id bigint,label string,p string) using delta partitioned by(p)
                location '${changes}' tblproperties('delta.columnMapping.mode'='name','delta.enableDeletionVectors'='true',
                'delta.enableInCommitTimestamps'='true','stage'='before')""")
            write("""insert into delta_glue_p0.events select /*+ COALESCE(1) */ * from values
                (1L,'one','a'),(2L,'removed','a'),(3L,'three','b') as t(id,label,p)""")
            def history = spark.createStatement().withCloseable { stmt ->
                stmt.executeQuery("describe history delta_glue_p0.events limit 1").withCloseable { rs ->
                    assertTrue(rs.next())
                    [rs.getLong("version"), rs.getString("timestamp")]
                }
            }
            long oldVersion = history[0]
            // Read Spark's UTC display directly so Hive JDBC never reinterprets it in the runner's time zone.
            String oldTime = LocalDateTime.parse(history[1].replace(" ", "T")).toInstant(ZoneOffset.UTC).toString()
            sql "set time_zone='UTC'"
            qt_schema "desc test_deltalake_glue.glue_db.events"
            qt_before "select * from test_deltalake_glue.glue_db.events order by id"
            qt_mapped 'select numFiles > 0, name from test_deltalake_glue.glue_db.`mapped$detail`'
            qt_mapped_rows "select renamed,old,partition_renamed from test_deltalake_glue_aliases.glue_db.mapped order by renamed"
            qt_existing_dv 'select count(*) from test_deltalake_glue.glue_db.deleted'

            write("delete from delta_glue_p0.events where id=2")
            write("alter table delta_glue_p0.events rename column label to note")
            write("alter table delta_glue_p0.events set tblproperties ('stage'='after')")
            write("insert into delta_glue_p0.events values (4L,'four','a')")
            sql "refresh table test_deltalake_glue.glue_db.events"
            qt_latest "select id,note,p from test_deltalake_glue.glue_db.events order by id"
            qt_version """select id,label,p from test_deltalake_glue.glue_db.events for version as of ${oldVersion} order by id"""
            qt_time """select id,label,p from test_deltalake_glue.glue_db.events for time as of '${oldTime}' order by id"""
            qt_detail 'select format,name,to_json(partitionColumns) from test_deltalake_glue.glue_db.`events$detail`'
            qt_files '''select sum(num_records),sum(deletion_vector_cardinality),sum(cast(has_deletion_vector as int)) > 0
                       from test_deltalake_glue.glue_db.`events$files`'''
            qt_old_files """select sum(num_records),sum(deletion_vector_cardinality)
                            from test_deltalake_glue.glue_db.`events\$files` for time as of '${oldTime}'"""
            qt_history '''select min(version),max(version)>1,sum(if(operation='DELETE',1,0))
                          from test_deltalake_glue.glue_db.`events$history`'''
            qt_properties """select `value` from test_deltalake_glue.glue_db.`events\$properties`
                             for version as of ${oldVersion} where `key`='stage'"""
            qt_partitions '''select `partition`['p'] as p,record_count,deleted_record_count
                             from test_deltalake_glue.glue_db.`events$partitions` order by p'''
            sql "analyze table test_deltalake_glue.glue_db.events (id) with sync"
            def cid = get_catalog_id("test_deltalake_glue")
            def dbid = get_database_id("test_deltalake_glue", "glue_db")
            def tid = get_table_id("test_deltalake_glue", "glue_db", "events")
            qt_stats """select col_id,count,ndv,null_count,min,max from internal.__internal_schema.column_statistics
                        where catalog_id='${cid}' and db_id='${dbid}' and tbl_id='${tid}' and col_id='id'
                          and idx_id='-1' and part_id is null order by col_id"""

            sql """create materialized view delta_glue_mtmv build deferred refresh auto on manual partition by(p)
                distributed by random buckets 1 properties ('replication_num'='1')
                as select p,id from test_deltalake_glue.glue_db.events"""
            sql "refresh materialized view delta_glue_mtmv auto"
            waitingMTMVTaskFinishedByMvName("delta_glue_mtmv")
            write("insert into delta_glue_p0.events values (5L,'five','b')")
            sql "refresh table test_deltalake_glue.glue_db.events"
            sql "refresh materialized view delta_glue_mtmv auto"
            waitingMTMVTaskFinishedByMvName("delta_glue_mtmv")
            qt_glue_auto """select RefreshMode,NeedRefreshPartitions,CompletedPartitions from tasks('type'='mv')
                where MvDatabaseName='${context.dbName}' and MvName='delta_glue_mtmv'
                order by CreateTime desc,TaskId desc limit 1"""
            qt_glue_mv "select p,id from delta_glue_mtmv order by p,id"
        } finally {
            spark.close()
        }
        for (String name in ["hive_table", "iceberg_table", "absent"]) {
            test {
                sql "desc test_deltalake_glue.glue_db.${name}"
                exception "Unknown table '${name}'"
            }
        }
        test {
            sql "select * from test_deltalake_glue.glue_db.manifest"
            exception "Symlink manifest"
        }
        test {
            sql "select * from test_deltalake_glue.glue_db.missing_root"
            exception "Missing storage location"
        }
    } finally {
        glue.stop(0)
    }
}
