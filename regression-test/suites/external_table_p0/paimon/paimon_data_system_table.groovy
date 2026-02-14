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

suite("paimon_data_system_table", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalogName = "paimon_data_system_table"
    String dbName = "test_paimon_spark"
    String tableName = "data_sys_table"
    String nativeTableName = "data_sys_table_native"
    String appendTableName = "data_sys_table_append"
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    try {
        sql """drop catalog if exists ${catalogName}"""
        sql """CREATE CATALOG ${catalogName} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.region" = "us-east-1"
            );"""

        logger.info("catalog " + catalogName + " created")
        sql """switch ${catalogName};"""
        logger.info("switched to catalog " + catalogName)
        sql """use ${dbName};"""
        logger.info("use " + dbName)

        List<List<Object>> paimonTableList = sql """show tables;"""
        boolean targetTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(tableName)
        }
        assertTrue(targetTableExists, "Target table '${tableName}' not found in database '${dbName}'")
        boolean appendTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(appendTableName)
        }
        assertTrue(appendTableExists, "Target table '${appendTableName}' not found in database '${dbName}'")
        boolean nativeTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(nativeTableName)
        }
        assertTrue(nativeTableExists, "Target table '${nativeTableName}' not found in database '${dbName}'")

        qt_base_table_count """select count(*) from ${tableName}"""
        qt_base_table_rows """select id, name from ${tableName} order by id"""

        qt_audit_log_count """select count(*) from ${tableName}\$audit_log"""
        qt_audit_log_rows """select rowkind, id, name from ${tableName}\$audit_log order by id"""
        qt_audit_log_rowkind_stats """select rowkind, count(*) from ${tableName}\$audit_log group by rowkind order by rowkind"""
        qt_binlog_count """select count(*) from ${tableName}\$binlog"""
        qt_binlog_rows """select rowkind, id[1], id[2], name[1], name[2] from ${tableName}\$binlog order by id[1]"""
        qt_binlog_rowkind_stats """select rowkind, count(*) from ${tableName}\$binlog group by rowkind order by rowkind"""
        qt_desc_base_table """desc ${tableName}"""
        qt_desc_base_table_binlog """desc ${tableName}\$binlog"""
        qt_desc_base_table_audit_log """desc ${tableName}\$audit_log"""
        qt_desc_base_table_ro """desc ${tableName}\$ro"""

        qt_native_base_table_count """select count(*) from ${nativeTableName}"""
        qt_native_base_table_rows """select id, name from ${nativeTableName} order by id"""
        qt_native_audit_log_count """select count(*) from ${nativeTableName}\$audit_log"""
        qt_native_audit_log_rows """select rowkind, id, name from ${nativeTableName}\$audit_log order by id"""
        qt_native_audit_log_rowkind_stats """select rowkind, count(*) from ${nativeTableName}\$audit_log group by rowkind order by rowkind"""
        qt_native_binlog_count """select count(*) from ${nativeTableName}\$binlog"""
        qt_native_binlog_rows """select rowkind, id[1], id[2], name[1], name[2] from ${nativeTableName}\$binlog order by id[1]"""
        qt_native_binlog_rowkind_stats """select rowkind, count(*) from ${nativeTableName}\$binlog group by rowkind order by rowkind"""
        qt_native_ro_table_count """select count(*) from ${nativeTableName}\$ro"""
        qt_native_ro_table_rows """select id, name from ${nativeTableName}\$ro order by id"""
        qt_desc_native_table """desc ${nativeTableName}"""
        qt_desc_native_table_binlog """desc ${nativeTableName}\$binlog"""
        qt_desc_native_table_audit_log """desc ${nativeTableName}\$audit_log"""
        qt_desc_native_table_ro """desc ${nativeTableName}\$ro"""

        qt_append_table_count """select count(*) from ${appendTableName}"""
        qt_append_table_rows """select id, name from ${appendTableName} order by id"""

        qt_append_ro_table_count """select count(*) from ${appendTableName}\$ro"""
        qt_append_ro_table_rows """select id, name from ${appendTableName}\$ro order by id"""
        // We intentionally do not run table-read.sequence-number.enabled regression checks here.
        // Because Paimon also does not allow setting this option via SQL hints.
        // This logic is covered in FE UT (PaimonUtilTest).

        def getExplainText = { String explainSql ->
            List<List<Object>> explainRows = sql(explainSql)
            return explainRows.collect { row -> row[0].toString() }.join("\n")
        }

        def assertNativePath = { String querySql, String tableLabel ->
            String explainText = getExplainText("explain verbose ${querySql}")
            def splitMatcher = (explainText =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
            assertTrue(splitMatcher.find(), "Expected paimonNativeReadSplits in explain for ${tableLabel}")
            long nativeSplits = Long.parseLong(splitMatcher.group(1))
            long totalSplits = Long.parseLong(splitMatcher.group(2))
            assertTrue(totalSplits > 0, "Expected total splits > 0 for ${tableLabel}")
            assertTrue(nativeSplits > 0,
                    "Expected native splits > 0 for ${tableLabel}, native=${nativeSplits}, total=${totalSplits}")
            assertTrue(explainText.contains("SplitStat [type=NATIVE"),
                    "Expected NATIVE split stats in explain for ${tableLabel}")
        }

        def assertJniPath = { String querySql, String tableLabel ->
            String explainText = getExplainText("explain verbose ${querySql}")
            def splitMatcher = (explainText =~ /paimonNativeReadSplits=(\d+)\/(\d+)/)
            assertTrue(splitMatcher.find(), "Expected paimonNativeReadSplits in explain for ${tableLabel}")
            long nativeSplits = Long.parseLong(splitMatcher.group(1))
            long totalSplits = Long.parseLong(splitMatcher.group(2))
            assertTrue(totalSplits > 0, "Expected total splits > 0 for ${tableLabel}")
            assertTrue(nativeSplits == 0,
                    "Expected native splits == 0 for JNI path ${tableLabel}, native=${nativeSplits}, total=${totalSplits}")
            assertTrue(explainText.contains("SplitStat [type=JNI"),
                    "Expected JNI split stats in explain for ${tableLabel}")
        }

        def assertCountStarPushdown = { String querySql, String tableLabel ->
            String explainText = getExplainText("explain ${querySql}")
            assertTrue(explainText.contains("pushdown agg=COUNT"),
                    "Expected count(*) pushdown in explain for ${tableLabel}")
        }

        sql """set force_jni_scanner=false"""
        // Paimon data system tables need Paimon-side semantics:
        // - binlog: pack/merge + array materialization
        // - audit_log: rowkind / sequence-number projection
        // TODO: switch these assertions back to native once Doris native reader supports the same semantics.
        assertJniPath("select rowkind, id[1], name[1] from ${tableName}\$binlog", "${tableName}\$binlog")
        assertJniPath("select rowkind, id[1], name[1] from ${nativeTableName}\$binlog", "${nativeTableName}\$binlog")
        assertJniPath("select rowkind, id, name from ${tableName}\$audit_log", "${tableName}\$audit_log")
        assertJniPath("select rowkind, id, name from ${nativeTableName}\$audit_log", "${nativeTableName}\$audit_log")

        assertCountStarPushdown("select count(*) from ${tableName}\$binlog", "${tableName}\$binlog")
        assertCountStarPushdown("select count(*) from ${tableName}\$audit_log", "${tableName}\$audit_log")
        assertCountStarPushdown("select count(*) from ${nativeTableName}\$binlog", "${nativeTableName}\$binlog")
        assertCountStarPushdown("select count(*) from ${nativeTableName}\$audit_log", "${nativeTableName}\$audit_log")

        sql """set force_jni_scanner=true"""
        assertJniPath("select rowkind, id[1], name[1] from ${tableName}\$binlog", "${tableName}\$binlog")
        assertJniPath("select rowkind, id, name from ${tableName}\$audit_log", "${tableName}\$audit_log")
        assertJniPath("select rowkind, id[1], name[1] from ${nativeTableName}\$binlog", "${nativeTableName}\$binlog")
        assertJniPath("select rowkind, id, name from ${nativeTableName}\$audit_log", "${nativeTableName}\$audit_log")

        qt_jni_binlog_rows """select rowkind, id[1], id[2], name[1], name[2] from ${tableName}\$binlog order by id[1]"""
        qt_jni_audit_log_rows """select rowkind, id, name from ${tableName}\$audit_log order by id"""
        qt_jni_native_binlog_rows """select rowkind, id[1], id[2], name[1], name[2] from ${nativeTableName}\$binlog order by id[1]"""
        qt_jni_native_audit_log_rows """select rowkind, id, name from ${nativeTableName}\$audit_log order by id"""
    } finally {
        sql """set force_jni_scanner=false"""
    }
}
