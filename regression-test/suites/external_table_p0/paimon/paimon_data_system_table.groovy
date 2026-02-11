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
    String tableName = "data_sys_table_seq_off"
    String appendTableName = "data_sys_table_append"
    String seqOnTableName = "data_sys_table_seq_on"
    String seqOffTableName = "data_sys_table_seq_off"
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
        boolean seqOnTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(seqOnTableName)
        }
        assertTrue(seqOnTableExists, "Target table '${seqOnTableName}' not found in database '${dbName}'")
        boolean seqOffTableExists = paimonTableList.any { row ->
            row.size() > 0 && row[0].toString().equals(seqOffTableName)
        }
        assertTrue(seqOffTableExists, "Target table '${seqOffTableName}' not found in database '${dbName}'")

        qt_base_table_count """select count(*) from ${tableName}"""
        qt_base_table_rows """select id, name from ${tableName} order by id"""

        qt_audit_log_count """select count(*) from ${tableName}\$audit_log"""
        qt_audit_log_rows """select rowkind, id, name from ${tableName}\$audit_log order by id"""
        qt_audit_log_rowkind_stats """select rowkind, count(*) from ${tableName}\$audit_log group by rowkind order by rowkind"""
        qt_binlog_count """select count(*) from ${tableName}\$binlog"""
        qt_binlog_rows """select rowkind, id[1], id[2], name[1], name[2] from ${tableName}\$binlog order by id[1]"""
        qt_binlog_rowkind_stats """select rowkind, count(*) from ${tableName}\$binlog group by rowkind order by rowkind"""

        qt_append_table_count """select count(*) from ${appendTableName}"""
        qt_append_table_rows """select id, name from ${appendTableName} order by id"""

        qt_append_ro_table_count """select count(*) from ${appendTableName}\$ro"""
        qt_append_ro_table_rows """select id, name from ${appendTableName}\$ro order by id"""

        // 1. cover table-read.sequence-number.enabled=true
        List<List<Object>> seqOnBinlogDesc = sql """desc ${seqOnTableName}\$binlog"""
        List<String> seqOnBinlogColumns = seqOnBinlogDesc.collect { it[0].toString().toLowerCase() }
        assertTrue(seqOnBinlogColumns.contains("_sequence_number"),
                "Expected _SEQUENCE_NUMBER in ${seqOnTableName}\$binlog schema")

        List<List<Object>> seqOnAuditLogDesc = sql """desc ${seqOnTableName}\$audit_log"""
        List<String> seqOnAuditLogColumns = seqOnAuditLogDesc.collect { it[0].toString().toLowerCase() }
        assertTrue(seqOnAuditLogColumns.contains("_sequence_number"),
                "Expected _SEQUENCE_NUMBER in ${seqOnTableName}\$audit_log schema")

        List<List<Object>> seqOnBinlogRows = sql """
            select rowkind, _SEQUENCE_NUMBER, id[1], name[1]
            from ${seqOnTableName}\$binlog
            order by id[1], _SEQUENCE_NUMBER
        """
        assertTrue(seqOnBinlogRows.size() > 0, "Expected rows in ${seqOnTableName}\$binlog")
        assertTrue(seqOnBinlogRows.every { row -> row[1] != null },
                "Expected non-null _SEQUENCE_NUMBER in ${seqOnTableName}\$binlog")

        // 2. cover table-read.sequence-number.enabled=false(default)
        List<List<Object>> seqOffBinlogDesc = sql """desc ${seqOffTableName}\$binlog"""
        List<String> seqOffBinlogColumns = seqOffBinlogDesc.collect { it[0].toString().toLowerCase() }
        assertTrue(!seqOffBinlogColumns.contains("_sequence_number"),
                "Unexpected _SEQUENCE_NUMBER in ${seqOffTableName}\$binlog schema")

        List<List<Object>> seqOffAuditLogDesc = sql """desc ${seqOffTableName}\$audit_log"""
        List<String> seqOffAuditLogColumns = seqOffAuditLogDesc.collect { it[0].toString().toLowerCase() }
        assertTrue(!seqOffAuditLogColumns.contains("_sequence_number"),
                "Unexpected _SEQUENCE_NUMBER in ${seqOffTableName}\$audit_log schema")

        List<List<Object>> seqOffBinlogRows = sql """
            select rowkind, id[1], name[1]
            from ${seqOffTableName}\$binlog
            order by id[1]
        """
        assertTrue(seqOffBinlogRows.size() > 0, "Expected rows in ${seqOffTableName}\$binlog")
    } finally {
    }
}
