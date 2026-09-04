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

// WHY: a COMMENT on a field nested inside a STRUCT column must survive Doris CREATE TABLE on a paimon
// catalog (write path: PaimonTypeMapping.toPaimonRowType passes the comment into the 4-arg paimon
// DataField), land in the on-disk paimon schema ($schemas.fields), and round-trip back through the read
// path (PaimonTypeMapping.toStructType + fe-core ConnectorColumnConverter.convertStructType) so that
// SHOW CREATE TABLE reports it. Before the fix the nested comment was dropped on both paths. (DESC is not
// asserted: its Type column is a bare type signature that omits nested field comments for every table.)
suite("test_paimon_nested_struct_comment", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    String defaultFs = "hdfs://${externalEnvIp}:${hdfsPort}"
    String warehouse = "${defaultFs}/warehouse"

    String catalogName = "test_paimon_nested_struct_comment"
    String dbName = "test_nested_struct_comment_db"
    String tableName = "t_nested_struct_comment"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'paimon.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
            'warehouse' = '${warehouse}'
        );
    """

    try {
        sql """switch ${catalogName}"""
        sql """create database if not exists ${dbName}"""
        sql """use ${dbName}"""
        sql """drop table if exists ${tableName}"""

        // A comment on a top-level column AND on a field nested inside a STRUCT column.
        sql """
            CREATE TABLE ${tableName} (
                id int COMMENT 'the id',
                s STRUCT<a:int COMMENT 'note_a', b:string> COMMENT 'the struct'
            ) engine=paimon;
        """

        // Force a fresh schema load from the paimon catalog so the read-back exercises the read path
        // rather than any cached ConnectorType from the create.
        sql """refresh catalog ${catalogName}"""
        sql """use ${dbName}"""

        // 1) Read path: SHOW CREATE TABLE must render the nested struct field comment. Column comments
        //    already worked; the nested 'note_a' is the fix under test.
        def showCreate = sql """show create table ${tableName}"""
        String createStr = showCreate[0][1].toString()
        logger.info("SHOW CREATE TABLE ${tableName}:\n" + createStr)
        assertTrue(createStr.contains("note_a"),
                "SHOW CREATE TABLE must render the nested struct field comment 'note_a', got: " + createStr)

        // NOTE: DESC is intentionally NOT asserted here. Its Type column is a pure type signature rendered by
        // Type.hideVersionForVersionColumn (struct branch emits name:type only), which by design omits nested
        // struct field comments for every table (internal olap and external alike). The read path is already
        // proven by the SHOW CREATE TABLE assertion above; making DESC surface nested comments would be a
        // separate, branch-wide product change, out of scope for this paimon fix.

        // 2) Write path: the comment must have landed in the on-disk paimon schema metadata. The
        //    $schemas system table exposes the raw RowType (fields) string, bypassing the read mapping,
        //    so it independently proves the write path carried the comment.
        def schemas = sql """select fields from ${tableName}\$schemas"""
        String fieldsStr = schemas.toString()
        logger.info("paimon \$schemas fields of ${tableName}:\n" + fieldsStr)
        assertTrue(fieldsStr.contains("note_a"),
                "paimon \$schemas.fields must contain the nested struct field comment 'note_a' (write path), got: "
                        + fieldsStr)
    } finally {
        sql """drop table if exists ${catalogName}.${dbName}.${tableName}"""
        sql """drop database if exists ${catalogName}.${dbName}"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
