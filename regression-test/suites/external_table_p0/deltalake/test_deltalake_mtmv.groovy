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

suite("test_deltalake_mtmv", "p0,external,deltalake,external_docker") {
    def cfg = context.config.otherConfigs
    if (!"true".equalsIgnoreCase(cfg.get("enableDeltaLakeTest"))) {
        return
    }
    String host = cfg.get("externalEnvIp")
    String warehouse = cfg.get("deltalakeWarehouse")
    String storage = warehouse.startsWith("s3") ? """, 's3.endpoint'='http://${host}:${cfg.get("deltalakeMinioPort")}',
        's3.region'='us-east-1','s3.access_key'='admin','s3.secret_key'='password','use_path_style'='true'""" : ""
    def createCatalog = { String name ->
        sql "drop catalog if exists ${name}"
        sql """create catalog ${name} properties ('type'='deltalake', 'warehouse'='${warehouse}'${storage})"""
    }
    sql "drop materialized view if exists delta_mtmv"
    createCatalog("test_deltalake_mtmv")
    String source = "delta_mtmv_db.events"
    String table = "test_deltalake_mtmv.${source}"
    String history = 'test_deltalake_mtmv.delta_mtmv_db.`events$history`'
    String files = 'test_deltalake_mtmv.delta_mtmv_db.`events$files`'
    def refresh = { String tag ->
        sql "refresh table ${table}"
        sql "refresh materialized view delta_mtmv auto"
        waitingMTMVTaskFinishedByMvName("delta_mtmv")
        quickTest("${tag}_task", """select RefreshMode,
            array_sort(cast(NeedRefreshPartitions as array<string>)),
            array_sort(cast(CompletedPartitions as array<string>))
            from tasks('type'='mv') where MvDatabaseName='${context.dbName}' and MvName='delta_mtmv'
            order by CreateTime desc, TaskId desc limit 1""")
        def actual = sql("select p,id from delta_mtmv order by p,id")
        def base = sql("select /*+ SET_VAR(enable_materialized_view_rewrite=false) */ p,id from ${table} order by p,id")
        assertEquals(base, actual)
        quickTest("${tag}_rows", "select p,id from delta_mtmv order by p,id")
    }

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection("jdbc:hive2://${host}:${cfg.get('deltalakeSparkPort')}/default",
            "hadoop", "hadoop").withCloseable { spark ->
        def write = { String statement -> JdbcUtils.executeToList(spark, statement) }
        write("create database if not exists delta_mtmv_db location '${warehouse}/delta_mtmv_db'")
        write("drop table if exists ${source}")
        write("""create table ${source}(id int,p int) using delta partitioned by(p)
            tblproperties ('delta.enableDeletionVectors'='true')""")
        write("""insert into ${source} select /*+ COALESCE(1) */ * from values
            (11,10),(12,10),(13,10),(21,20),(22,20) as t(id,p)""")
        sql """create materialized view delta_mtmv build deferred refresh auto on manual partition by(p)
            distributed by random buckets 1 properties ('replication_num'='1') as select p,id from ${table}"""
        refresh("initial")
        String oldestFile = sql("select path from ${files} where partition_values['p']='10'").first()[0]
        write("insert into ${source} select /*+ COALESCE(1) */ * from values (14,10),(15,10) as t(id,p)")
        refresh("append")
        write("delete from ${source} where p=10 and id=14")
        refresh("dv")
        qt_dv_proof """select cast(operationMetrics['numDeletionVectorsAdded'] as bigint)>0
            from ${history} where operation='DELETE' order by version desc limit 1"""
        write("delete from ${source} where p=10 and id between 11 and 13")
        refresh("remove_file")
        qt_old_file_gone "select count(*) from ${files} where path='${oldestFile}'"
        write("insert into ${source} values (31,30)")
        refresh("add_partition")
        write("delete from ${source} where p=20")
        refresh("drop_partition")
        refresh("unchanged")

        write("insert into ${source} values (16,10)")
        write("insert into ${source} values (17,10)")
        refresh("before_optimize")
        write("optimize ${source} where p=10")
        refresh("optimize")
        qt_compaction_proof """select cast(operationMetrics['numRemovedFiles'] as bigint)>0
            from ${history} where operation='OPTIMIZE' order by version desc limit 1"""

        // A fresh catalog has no MTMV candidates. Its partition freshness cache must stay cold even when
        // the FE builds, validates and invalidates a SQL result cache entry for an ordinary native scan.
        createCatalog("test_deltalake_mtmv_cache")
        String cacheTable = "test_deltalake_mtmv_cache.${source}"
        String query = "select p,id from ${cacheTable} order by p,id"
        sql "set enable_sql_cache=true"
        sql "set enable_hive_sql_cache=true"
        long quietSeconds = sql("admin show frontend config like 'cache_last_version_interval_second'")[0][1] as long
        sleep((quietSeconds + 2) * 1000L)
        qt_cache_rows query
        explain {
            sql("physical plan ${query}")
            contains("PhysicalSqlCache")
        }
        write("insert into ${source} values (32,30)")
        sql "refresh table ${cacheTable}"
        explain {
            sql("physical plan ${query}")
            notContains("PhysicalSqlCache")
        }
        qt_cache_after_write query
        qt_native_partition_enumeration """select sum(request_count) from internal.information_schema.catalog_meta_cache_statistics
            where catalog_name='test_deltalake_mtmv_cache' and entry_name='deltalake-partition-freshness'"""
    }
}
