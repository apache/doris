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

package org.apache.doris.catalog;

import org.apache.doris.analysis.SchemaTableType;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.thrift.TSchemaTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Doris's representation of MySQL information schema table metadata.
 */
public class SchemaTable extends Table {
    private static final int FN_REFLEN = 512;
    private static final int NAME_CHAR_LEN = 64;
    private static final int MY_CS_NAME_SIZE = 32;
    private static final int GRANTEE_len = 81;
    private static final int PRIVILEGE_TYPE_LEN = 64;
    private static final int IS_GRANTABLE_LEN = 3;

    // Now we just mock tables, table_privileges, referential_constraints, key_column_usage and routines table
    // Because in MySQL ODBC, these tables are used.
    // TODO(zhaochun): Review some commercial BI to check if we need support where clause in show statement
    // like 'show table where_clause'. If we decide to support it, we must mock these related table here.
    public static Map<String, Table> TABLE_MAP = ImmutableMap.<String, Table>builder().put("tables",
                    new SchemaTable(SystemIdGenerator.getNextId(), "tables", TableType.SCHEMA,
                            builder().column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("ENGINE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("ROW_FORMAT", ScalarType.createVarchar(10))
                                    .column("TABLE_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("AVG_ROW_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("MAX_DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("INDEX_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DATA_FREE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("AUTO_INCREMENT", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("UPDATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("CHECK_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("TABLE_COLLATION", ScalarType.createVarchar(MY_CS_NAME_SIZE))
                                    .column("CHECKSUM", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CREATE_OPTIONS", ScalarType.createVarchar(255))
                                    .column("TABLE_COMMENT", ScalarType.createVarchar(2048)).build()))
            .put("table_privileges",
                    new SchemaTable(SystemIdGenerator.getNextId(), "table_privileges", TableType.SCHEMA,
                            builder().column("GRANTEE", ScalarType.createVarchar(GRANTEE_len))
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("PRIVILEGE_TYPE", ScalarType.createVarchar(PRIVILEGE_TYPE_LEN))
                                    .column("IS_GRANTABLE", ScalarType.createVarchar(IS_GRANTABLE_LEN)).build()))
            .put("schema_privileges",
                    new SchemaTable(SystemIdGenerator.getNextId(), "schema_privileges", TableType.SCHEMA,
                            builder().column("GRANTEE", ScalarType.createVarchar(GRANTEE_len))
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("PRIVILEGE_TYPE", ScalarType.createVarchar(PRIVILEGE_TYPE_LEN))
                                    .column("IS_GRANTABLE", ScalarType.createVarchar(IS_GRANTABLE_LEN)).build()))
            .put("user_privileges", new SchemaTable(SystemIdGenerator.getNextId(), "user_privileges", TableType.SCHEMA,
                    builder().column("GRANTEE", ScalarType.createVarchar(GRANTEE_len))
                            .column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
                            .column("PRIVILEGE_TYPE", ScalarType.createVarchar(PRIVILEGE_TYPE_LEN))
                            .column("IS_GRANTABLE", ScalarType.createVarchar(IS_GRANTABLE_LEN)).build()))
            .put("referential_constraints",
                    new SchemaTable(SystemIdGenerator.getNextId(), "referential_constraints", TableType.SCHEMA,
                            builder().column("CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("MATCH_OPTION", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UPDATE_RULE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("DELETE_RULE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("REFERENCED_TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN)).build()))
            .put("key_column_usage",
                    new SchemaTable(SystemIdGenerator.getNextId(), "key_column_usage", TableType.SCHEMA,
                            builder().column("CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("COLUMN_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("ORDINAL_POSITION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("POSITION_IN_UNIQUE_CONSTRAINT",
                                            ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("REFERENCED_TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("REFERENCED_TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("REFERENCED_COLUMN_NAME", ScalarType.createVarchar(64)).build()))
            .put("routines", new SchemaTable(SystemIdGenerator.getNextId(), "routines", TableType.SCHEMA,
                    builder().column("SPECIFIC_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("DTD_IDENTIFIER", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_BODY", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_DEFINITION", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("EXTERNAL_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("EXTERNAL_LANGUAGE", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("PARAMETER_STYLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("IS_DETERMINISTIC", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("SQL_DATA_ACCESS", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("SQL_PATH", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("SECURITY_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("CREATED", ScalarType.createType(PrimitiveType.DATETIME))
                            .column("LAST_ALTERED", ScalarType.createType(PrimitiveType.DATETIME))
                            .column("SQL_MODE", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("ROUTINE_COMMENT", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("DEFINER", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("COLLATION_CONNECTION", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("DATABASE_COLLATION", ScalarType.createVarchar(NAME_CHAR_LEN)).build()))
            .put("schemata", new SchemaTable(SystemIdGenerator.getNextId(), "schemata", TableType.SCHEMA,
                    builder().column("CATALOG_NAME", ScalarType.createVarchar(512))
                            .column("SCHEMA_NAME", ScalarType.createVarchar(32))
                            .column("DEFAULT_CHARACTER_SET_NAME", ScalarType.createVarchar(32))
                            .column("DEFAULT_COLLATION_NAME", ScalarType.createVarchar(32))
                            .column("SQL_PATH", ScalarType.createVarchar(512))
                            .column("DEFAULT_ENCRYPTION", ScalarType.createVarchar(3)).build()))
            .put("session_variables",
                    new SchemaTable(SystemIdGenerator.getNextId(), "session_variables", TableType.SCHEMA,
                            builder().column("VARIABLE_NAME", ScalarType.createVarchar(64))
                                    .column("VARIABLE_VALUE", ScalarType.createVarchar(1024))
                                    .column("DEFAULT_VALUE", ScalarType.createVarchar(1024))
                                    .column("CHANGED", ScalarType.createVarchar(4))
                                    .build()))
            .put("global_variables",
                    new SchemaTable(SystemIdGenerator.getNextId(), "global_variables", TableType.SCHEMA,
                            builder().column("VARIABLE_NAME", ScalarType.createVarchar(64))
                                    .column("VARIABLE_VALUE", ScalarType.createVarchar(1024))
                                    .column("DEFAULT_VALUE", ScalarType.createVarchar(1024))
                                    .column("CHANGED", ScalarType.createVarchar(4)).build()))
            .put("columns",
                    new SchemaTable(SystemIdGenerator.getNextId(), "columns", TableType.SCHEMA,
                            builder().column("TABLE_CATALOG", ScalarType.createVarchar(512))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("COLUMN_NAME", ScalarType.createVarchar(64))
                                    .column("ORDINAL_POSITION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("COLUMN_DEFAULT", ScalarType.createVarchar(1024))
                                    .column("IS_NULLABLE", ScalarType.createVarchar(3))
                                    .column("DATA_TYPE", ScalarType.createVarchar(64))
                                    .column("CHARACTER_MAXIMUM_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CHARACTER_OCTET_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("NUMERIC_PRECISION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("NUMERIC_SCALE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DATETIME_PRECISION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CHARACTER_SET_NAME", ScalarType.createVarchar(32))
                                    .column("COLLATION_NAME", ScalarType.createVarchar(32))
                                    .column("COLUMN_TYPE", ScalarType.createVarchar(32))
                                    .column("COLUMN_KEY", ScalarType.createVarchar(3))
                                    .column("EXTRA", ScalarType.createVarchar(27))
                                    .column("PRIVILEGES", ScalarType.createVarchar(80))
                                    .column("COLUMN_COMMENT", ScalarType.createVarchar(255))
                                    .column("COLUMN_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DECIMAL_DIGITS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("GENERATION_EXPRESSION", ScalarType.createVarchar(64))
                                    .column("SRS_ID", ScalarType.createType(PrimitiveType.BIGINT)).build()))
            .put("character_sets", new SchemaTable(SystemIdGenerator.getNextId(), "character_sets", TableType.SCHEMA,
                    builder().column("CHARACTER_SET_NAME", ScalarType.createVarchar(512))
                            .column("DEFAULT_COLLATE_NAME", ScalarType.createVarchar(64))
                            .column("DESCRIPTION", ScalarType.createVarchar(64))
                            .column("MAXLEN", ScalarType.createType(PrimitiveType.BIGINT)).build()))
            .put("collations",
                    new SchemaTable(SystemIdGenerator.getNextId(), "collations", TableType.SCHEMA,
                            builder().column("COLLATION_NAME", ScalarType.createVarchar(512))
                                    .column("CHARACTER_SET_NAME", ScalarType.createVarchar(64))
                                    .column("ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("IS_DEFAULT", ScalarType.createVarchar(64))
                                    .column("IS_COMPILED", ScalarType.createVarchar(64))
                                    .column("SORTLEN", ScalarType.createType(PrimitiveType.BIGINT)).build()))
            .put("table_constraints",
                    new SchemaTable(SystemIdGenerator.getNextId(), "table_constraints", TableType.SCHEMA,
                            builder().column("CONSTRAINT_CATALOG", ScalarType.createVarchar(512))
                                    .column("CONSTRAINT_SCHEMA", ScalarType.createVarchar(64))
                                    .column("CONSTRAINT_NAME", ScalarType.createVarchar(64))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("CONSTRAINT_TYPE", ScalarType.createVarchar(64)).build()))
            .put("engines",
                    new SchemaTable(SystemIdGenerator.getNextId(), "engines", TableType.SCHEMA,
                            builder().column("ENGINE", ScalarType.createVarchar(64))
                                    .column("SUPPORT", ScalarType.createVarchar(8))
                                    .column("COMMENT", ScalarType.createVarchar(80))
                                    .column("TRANSACTIONS", ScalarType.createVarchar(3))
                                    .column("XA", ScalarType.createVarchar(3))
                                    .column("SAVEPOINTS", ScalarType.createVarchar(3)).build()))
            .put("views",
                    new SchemaTable(SystemIdGenerator.getNextId(), "views", TableType.SCHEMA,
                            builder().column("TABLE_CATALOG", ScalarType.createVarchar(512))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("VIEW_DEFINITION", ScalarType.createVarchar(8096))
                                    .column("CHECK_OPTION", ScalarType.createVarchar(8))
                                    .column("IS_UPDATABLE", ScalarType.createVarchar(3))
                                    .column("DEFINER", ScalarType.createVarchar(77))
                                    .column("SECURITY_TYPE", ScalarType.createVarchar(7))
                                    .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                                    .column("COLLATION_CONNECTION", ScalarType.createVarchar(32)).build()))
            // statistics is table provides information about table indexes in mysql: 5.7
            .put("statistics", new SchemaTable(SystemIdGenerator.getNextId(), "statistics", TableType.SCHEMA,
                    builder().column("TABLE_CATALOG", ScalarType.createVarchar(512))
                            .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                            .column("TABLE_NAME", ScalarType.createVarchar(64))
                            .column("NON_UNIQUE", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("INDEX_SCHEMA", ScalarType.createVarchar(64))
                            .column("INDEX_NAME", ScalarType.createVarchar(64))
                            .column("SEQ_IN_INDEX", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("COLUMN_NAME", ScalarType.createVarchar(64))
                            .column("COLLATION", ScalarType.createVarchar(1))
                            .column("CARDINALITY", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("SUB_PART", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("PACKED", ScalarType.createVarchar(10))
                            .column("NULLABLE", ScalarType.createVarchar(3))
                            .column("INDEX_TYPE", ScalarType.createVarchar(16))
                            .column("COMMENT", ScalarType.createVarchar(16))
                            // for datagrip
                            .column("INDEX_COMMENT", ScalarType.createVarchar(1024)).build()))
            // Compatible with mysql for mysqldump
            .put("column_statistics",
                    new SchemaTable(SystemIdGenerator.getNextId(), "column_statistics", TableType.SCHEMA,
                            builder().column("SCHEMA_NAME", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("COLUMN_NAME", ScalarType.createVarchar(64))
                                    .column("HISTOGRAM", ScalarType.createJsonbType()).build()))
            .put("files",
                    new SchemaTable(SystemIdGenerator.getNextId(), "files", TableType.SCHEMA,
                            builder().column("FILE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("FILE_NAME", ScalarType.createStringType())
                                    .column("FILE_TYPE", ScalarType.createVarchar(256))
                                    .column("TABLESPACE_NAME", ScalarType.createVarchar(256))
                                    .column("TABLE_CATALOG", ScalarType.createCharType(16))
                                    .column("TABLE_SCHEMA", ScalarType.createStringType())
                                    .column("TABLE_NAME", ScalarType.createStringType())
                                    .column("LOGFILE_GROUP_NAME", ScalarType.createVarchar(256))
                                    .column("LOGFILE_GROUP_NUMBER", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("ENGINE", ScalarType.createVarchar(64))
                                    .column("FULLTEXT_KEYS", ScalarType.createStringType())
                                    .column("DELETED_ROWS", ScalarType.createStringType())
                                    .column("UPDATE_COUNT", ScalarType.createStringType())
                                    .column("FREE_EXTENTS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("TOTAL_EXTENTS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("EXTENT_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("INITIAL_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("MAXIMUM_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("AUTOEXTEND_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CREATION_TIME", ScalarType.createStringType())
                                    .column("LAST_UPDATE_TIME", ScalarType.createStringType())
                                    .column("LAST_ACCESS_TIME", ScalarType.createStringType())
                                    .column("RECOVER_TIME", ScalarType.createStringType())
                                    .column("TRANSACTION_COUNTER", ScalarType.createStringType())
                                    .column("VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("ROW_FORMAT", ScalarType.createVarchar(256))
                                    .column("TABLE_ROWS", ScalarType.createStringType())
                                    .column("AVG_ROW_LENGTH", ScalarType.createStringType())
                                    .column("DATA_LENGTH", ScalarType.createStringType())
                                    .column("MAX_DATA_LENGTH", ScalarType.createStringType())
                                    .column("INDEX_LENGTH", ScalarType.createStringType())
                                    .column("DATA_FREE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CREATE_TIME", ScalarType.createStringType())
                                    .column("UPDATE_TIME", ScalarType.createStringType())
                                    .column("CHECK_TIME", ScalarType.createStringType())
                                    .column("CHECKSUM", ScalarType.createStringType())
                                    .column("STATUS", ScalarType.createVarchar(256))
                                    .column("EXTRA", ScalarType.createVarchar(256)).build()))
            .put("partitions",
                    new SchemaTable(SystemIdGenerator.getNextId(), "partitions", TableType.SCHEMA,
                            builder().column("TABLE_CATALOG", ScalarType.createVarchar(64))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("PARTITION_NAME", ScalarType.createVarchar(64))
                                    .column("SUBPARTITION_NAME", ScalarType.createVarchar(64))
                                    .column("PARTITION_ORDINAL_POSITION", ScalarType.createType(PrimitiveType.INT))
                                    .column("SUBPARTITION_ORDINAL_POSITION",
                                            ScalarType.createType(PrimitiveType.INT))
                                    .column("PARTITION_METHOD", ScalarType.createVarchar(13))
                                    .column("SUBPARTITION_METHOD", ScalarType.createVarchar(13))
                                    .column("PARTITION_EXPRESSION", ScalarType.createVarchar(2048))
                                    .column("SUBPARTITION_EXPRESSION", ScalarType.createVarchar(2048))
                                    .column("PARTITION_DESCRIPTION", ScalarType.createStringType())
                                    .column("TABLE_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("AVG_ROW_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("MAX_DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("INDEX_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DATA_FREE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CREATE_TIME", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("UPDATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("CHECK_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("CHECKSUM", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("PARTITION_COMMENT", ScalarType.createStringType())
                                    .column("NODEGROUP", ScalarType.createVarchar(256))
                                    .column("TABLESPACE_NAME", ScalarType.createVarchar(268)).build()))
            // Compatible with Datagrip
            .put("column_privileges",
                    new SchemaTable(SystemIdGenerator.getNextId(), "column_privileges", TableType.SCHEMA,
                            builder().column("GRANTEE", ScalarType.createVarchar(128))
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(512))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("COLUMN_NAME", ScalarType.createVarchar(64))
                                    .column("PRIVILEGE_TYPE", ScalarType.createVarchar(64))
                                    .column("IS_GRANTABLE", ScalarType.createVarchar(3)).build()))
            // Compatible with Datagrip
            .put("triggers",
                    new SchemaTable(SystemIdGenerator.getNextId(), "triggers", TableType.SCHEMA,
                            builder().column("TRIGGER_CATALOG", ScalarType.createVarchar(512))
                                    .column("TRIGGER_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TRIGGER_NAME", ScalarType.createVarchar(64))
                                    .column("EVENT_MANIPULATION", ScalarType.createVarchar(6))
                                    .column("EVENT_OBJECT_CATALOG", ScalarType.createVarchar(512))
                                    .column("EVENT_OBJECT_SCHEMA", ScalarType.createVarchar(64))
                                    .column("EVENT_OBJECT_TABLE", ScalarType.createVarchar(64))
                                    .column("ACTION_ORDER", ScalarType.createVarchar(4))
                                    .column("ACTION_CONDITION", ScalarType.createVarchar(512))
                                    .column("ACTION_STATEMENT", ScalarType.createVarchar(512))
                                    .column("ACTION_ORIENTATION", ScalarType.createVarchar(9))
                                    .column("ACTION_TIMING", ScalarType.createVarchar(6))
                                    .column("ACTION_REFERENCE_OLD_TABLE", ScalarType.createVarchar(64))
                                    .column("ACTION_REFERENCE_NEW_TABLE", ScalarType.createVarchar(64))
                                    .column("ACTION_REFERENCE_OLD_ROW", ScalarType.createVarchar(3))
                                    .column("ACTION_REFERENCE_NEW_ROW", ScalarType.createVarchar(3))
                                    .column("CREATED", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("SQL_MODE", ScalarType.createVarchar(8192))
                                    .column("DEFINER", ScalarType.createVarchar(77))
                                    .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                                    .column("COLLATION_CONNECTION", ScalarType.createVarchar(32))
                                    .column("DATABASE_COLLATION", ScalarType.createVarchar(32)).build()))
            // Compatible with Datagrip
            .put("events",
                    new SchemaTable(SystemIdGenerator.getNextId(), "events", TableType.SCHEMA,
                            builder().column("EVENT_CATALOG", ScalarType.createVarchar(64))
                                    .column("EVENT_SCHEMA", ScalarType.createVarchar(64))
                                    .column("EVENT_NAME", ScalarType.createVarchar(64))
                                    .column("DEFINER", ScalarType.createVarchar(77))
                                    .column("TIME_ZONE", ScalarType.createVarchar(64))
                                    .column("EVENT_BODY", ScalarType.createVarchar(8))
                                    .column("EVENT_DEFINITION", ScalarType.createVarchar(512))
                                    .column("EVENT_TYPE", ScalarType.createVarchar(9))
                                    .column("EXECUTE_AT", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("INTERVAL_VALUE", ScalarType.createVarchar(256))
                                    .column("INTERVAL_FIELD", ScalarType.createVarchar(18))
                                    .column("SQL_MODE", ScalarType.createVarchar(8192))
                                    .column("STARTS", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("ENDS", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("STATUS", ScalarType.createVarchar(18))
                                    .column("ON_COMPLETION", ScalarType.createVarchar(12))
                                    .column("CREATED", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("LAST_ALTERED", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("LAST_EXECUTED", ScalarType.createType(PrimitiveType.DATETIME))
                                    .column("EVENT_COMMENT", ScalarType.createVarchar(64))
                                    .column("ORIGINATOR", ScalarType.createType(PrimitiveType.INT))
                                    .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                                    .column("COLLATION_CONNECTION", ScalarType.createVarchar(32))
                                    .column("DATABASE_COLLATION", ScalarType.createVarchar(32)).build()))
            .put("rowsets", new SchemaTable(SystemIdGenerator.getNextId(), "rowsets", TableType.SCHEMA,
                    builder().column("BACKEND_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("ROWSET_ID", ScalarType.createVarchar(64))
                            .column("TABLET_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("ROWSET_NUM_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("TXN_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("NUM_SEGMENTS", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("START_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("END_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("INDEX_DISK_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("DATA_DISK_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("CREATION_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                            .column("NEWEST_WRITE_TIMESTAMP", ScalarType.createType(PrimitiveType.DATETIME))
                            .column("SCHEMA_VERSION", ScalarType.createType(PrimitiveType.INT))
                            .build()))
            .put("parameters", new SchemaTable(SystemIdGenerator.getNextId(), "parameters", TableType.SCHEMA,
                    builder().column("SPECIFIC_CATALOG", ScalarType.createVarchar(64))
                            .column("SPECIFIC_SCHEMA", ScalarType.createVarchar(64))
                            .column("SPECIFIC_NAME", ScalarType.createVarchar(64))
                            .column("ORDINAL_POSITION", ScalarType.createVarchar(77))
                            .column("PARAMETER_MODE", ScalarType.createVarchar(77))
                            .column("PARAMETER_NAME", ScalarType.createVarchar(77))
                            .column("DATA_TYPE", ScalarType.createVarchar(64))
                            .column("CHARACTER_OCTET_LENGTH", ScalarType.createVarchar(64))
                            .column("NUMERIC_PRECISION", ScalarType.createVarchar(512))
                            .column("NUMERIC_SCALE", ScalarType.createVarchar(64))
                            .column("DATETIME_PRECISION", ScalarType.createVarchar(64))
                            .column("CHARACTER_SET_NAME", ScalarType.createVarchar(256))
                            .column("COLLATION_NAME", ScalarType.createVarchar(64))
                            .column("DTD_IDENTIFIER", ScalarType.createVarchar(64))
                            .column("ROUTINE_TYPE", ScalarType.createVarchar(64))
                            .column("DATA_TYPEDTD_IDENDS", ScalarType.createVarchar(64))
                            .build()))
            .put("metadata_name_ids", new SchemaTable(SystemIdGenerator.getNextId(),
                        "metadata_name_ids", TableType.SCHEMA,
                    builder().column("CATALOG_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("CATALOG_NAME", ScalarType.createVarchar(FN_REFLEN))
                            .column("DATABASE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("DATABASE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .column("TABLE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                            .build()))
            .put("profiling", new SchemaTable(SystemIdGenerator.getNextId(), "profiling", TableType.SCHEMA,
                    builder().column("QUERY_ID", ScalarType.createType(PrimitiveType.INT))
                            .column("SEQ", ScalarType.createType(PrimitiveType.INT))
                            .column("STATE", ScalarType.createVarchar(30))
                            .column("DURATION", ScalarType.createType(PrimitiveType.DOUBLE))
                            .column("CPU_USER", ScalarType.createType(PrimitiveType.DOUBLE))
                            .column("CPU_SYSTEM", ScalarType.createType(PrimitiveType.DOUBLE))
                            .column("CONTEXT_VOLUNTARY", ScalarType.createType(PrimitiveType.INT))
                            .column("CONTEXT_INVOLUNTARY", ScalarType.createType(PrimitiveType.INT))
                            .column("BLOCK_OPS_IN", ScalarType.createType(PrimitiveType.INT))
                            .column("BLOCK_OPS_OUT", ScalarType.createType(PrimitiveType.INT))
                            .column("MESSAGES_SENT", ScalarType.createType(PrimitiveType.INT))
                            .column("MESSAGES_RECEIVED", ScalarType.createType(PrimitiveType.INT))
                            .column("PAGE_FAULTS_MAJOR", ScalarType.createType(PrimitiveType.INT))
                            .column("PAGE_FAULTS_MINOR", ScalarType.createType(PrimitiveType.INT))
                            .column("SWAPS", ScalarType.createType(PrimitiveType.INT))
                            .column("SOURCE_FUNCTION", ScalarType.createVarchar(30))
                            .column("SOURCE_FILE", ScalarType.createVarchar(20))
                            .column("SOURCE_LINE", ScalarType.createType(PrimitiveType.INT))
                            .build()))
            .put("backend_active_tasks",
                    new SchemaTable(SystemIdGenerator.getNextId(), "backend_active_tasks", TableType.SCHEMA,
                            builder().column("BE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("FE_HOST", ScalarType.createVarchar(256))
                                    .column("QUERY_ID", ScalarType.createVarchar(256))
                                    .column("TASK_TIME_MS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("TASK_CPU_TIME_MS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("SCAN_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("SCAN_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("BE_PEAK_MEMORY_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CURRENT_USED_MEMORY_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("SHUFFLE_SEND_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("SHUFFLE_SEND_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("QUERY_TYPE",  ScalarType.createVarchar(256))
                                    .build()))
            .put("active_queries", new SchemaTable(SystemIdGenerator.getNextId(), "active_queries", TableType.SCHEMA,
                    builder().column("QUERY_ID", ScalarType.createVarchar(256))
                            .column("QUERY_START_TIME", ScalarType.createVarchar(256))
                            .column("QUERY_TIME_MS", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("WORKLOAD_GROUP_ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("DATABASE", ScalarType.createVarchar(256))
                            .column("FRONTEND_INSTANCE", ScalarType.createVarchar(256))
                            .column("QUEUE_START_TIME", ScalarType.createVarchar(256))
                            .column("QUEUE_END_TIME", ScalarType.createVarchar(256))
                            .column("QUERY_STATUS", ScalarType.createVarchar(256))
                            .column("SQL", ScalarType.createStringType())
                            .build()))
            .put("workload_groups", new SchemaTable(SystemIdGenerator.getNextId(), "workload_groups", TableType.SCHEMA,
                    builder().column("ID", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("NAME", ScalarType.createVarchar(256))
                            .column("CPU_SHARE", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("MEMORY_LIMIT", ScalarType.createVarchar(256))
                            .column("ENABLE_MEMORY_OVERCOMMIT", ScalarType.createVarchar(256))
                            .column("MAX_CONCURRENCY", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("MAX_QUEUE_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("QUEUE_TIMEOUT", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("CPU_HARD_LIMIT", ScalarType.createVarchar(256))
                            .column("SCAN_THREAD_NUM", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("MAX_REMOTE_SCAN_THREAD_NUM", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("MIN_REMOTE_SCAN_THREAD_NUM", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("SPILL_THRESHOLD_LOW_WATERMARK", ScalarType.createVarchar(256))
                            .column("SPILL_THRESHOLD_HIGH_WATERMARK", ScalarType.createVarchar(256))
                            .column("TAG", ScalarType.createVarchar(256))
                            .column("READ_BYTES_PER_SECOND", ScalarType.createType(PrimitiveType.BIGINT))
                            .column("REMOTE_READ_BYTES_PER_SECOND", ScalarType.createType(PrimitiveType.BIGINT))
                            .build()))
            .put("processlist",
                    new SchemaTable(SystemIdGenerator.getNextId(), "processlist", TableType.SCHEMA,
                            builder().column("CURRENT_CONNECTED", ScalarType.createVarchar(16))
                                    .column("ID", ScalarType.createType(PrimitiveType.LARGEINT))
                                    .column("USER", ScalarType.createVarchar(32))
                                    .column("HOST", ScalarType.createVarchar(261))
                                    .column("LOGIN_TIME", ScalarType.createType(PrimitiveType.DATETIMEV2))
                                    .column("CATALOG", ScalarType.createVarchar(64))
                                    .column("DB", ScalarType.createVarchar(64))
                                    .column("COMMAND", ScalarType.createVarchar(16))
                                    .column("TIME", ScalarType.createType(PrimitiveType.INT))
                                    .column("STATE", ScalarType.createVarchar(64))
                                    .column("QUERY_ID", ScalarType.createVarchar(256))
                                    .column("INFO", ScalarType.createVarchar(ScalarType.MAX_VARCHAR_LENGTH))
                                    .column("FE",
                                            ScalarType.createVarchar(64))
                                    .column("CLOUD_CLUSTER", ScalarType.createVarchar(64)).build(), true))
            .put("workload_policy",
                    new SchemaTable(SystemIdGenerator.getNextId(), "workload_policy", TableType.SCHEMA,
                            builder().column("ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("NAME", ScalarType.createVarchar(256))
                                    .column("CONDITION", ScalarType.createStringType())
                                    .column("ACTION", ScalarType.createStringType())
                                    .column("PRIORITY", ScalarType.createType(PrimitiveType.INT))
                                    .column("ENABLED", ScalarType.createType(PrimitiveType.BOOLEAN))
                                    .column("VERSION", ScalarType.createType(PrimitiveType.INT))
                                    .column("WORKLOAD_GROUP", ScalarType.createStringType()).build()))
            .put("table_options",
                    new SchemaTable(SystemIdGenerator.getNextId(), "table_options", TableType.SCHEMA,
                            builder().column("TABLE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_MODEL", ScalarType.createStringType())
                                    .column("TABLE_MODEL_KEY", ScalarType.createStringType())
                                    .column("DISTRIBUTE_KEY", ScalarType.createStringType())
                                    .column("DISTRIBUTE_TYPE", ScalarType.createStringType())
                                    .column("BUCKETS_NUM", ScalarType.createType(PrimitiveType.INT))
                                    .column("PARTITION_NUM", ScalarType.createType(PrimitiveType.INT))
                                    .build()))
            .put("workload_group_privileges",
                    new SchemaTable(SystemIdGenerator.getNextId(), "workload_group_privileges", TableType.SCHEMA,
                            builder().column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("WORKLOAD_GROUP_NAME", ScalarType.createVarchar(256))
                                    .column("PRIVILEGE_TYPE", ScalarType.createVarchar(PRIVILEGE_TYPE_LEN))
                                    .column("IS_GRANTABLE", ScalarType.createVarchar(IS_GRANTABLE_LEN))
                                    .build()))
            .put("table_properties",
                    new SchemaTable(SystemIdGenerator.getNextId(), "table_properties", TableType.SCHEMA,
                            builder().column("TABLE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("PROPERTY_NAME", ScalarType.createStringType())
                                    .column("PROPERTY_VALUE", ScalarType.createStringType())
                                    .build())
            )
            .put("workload_group_resource_usage",
                    new SchemaTable(SystemIdGenerator.getNextId(), "workload_group_resource_usage", TableType.SCHEMA,
                            builder().column("BE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("WORKLOAD_GROUP_ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("MEMORY_USAGE_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CPU_USAGE_PERCENT", ScalarType.createType(PrimitiveType.DOUBLE))
                                    .column("LOCAL_SCAN_BYTES_PER_SECOND", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("REMOTE_SCAN_BYTES_PER_SECOND", ScalarType.createType(PrimitiveType.BIGINT))
                                    .build())
            )
            .put("catalog_meta_cache_statistics",
                    new SchemaTable(SystemIdGenerator.getNextId(), "catalog_meta_cache_statistics", TableType.SCHEMA,
                            builder().column("CATALOG_NAME", ScalarType.createStringType())
                                    .column("CACHE_NAME", ScalarType.createStringType())
                                    .column("METRIC_NAME", ScalarType.createStringType())
                                    .column("METRIC_VALUE", ScalarType.createStringType())
                                    .build())
            )
            .build();

    private boolean fetchAllFe = false;

    protected SchemaTable(long id, String name, TableType type, List<Column> baseSchema) {
        super(id, name, type, baseSchema);
    }

    protected SchemaTable(long id, String name, TableType type, List<Column> baseSchema, boolean fetchAllFe) {
        this(id, name, type, baseSchema);
        this.fetchAllFe = fetchAllFe;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("Do not allow to write SchemaTable to image.");
    }

    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Do not allow read SchemaTable from image.");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static boolean isShouldFetchAllFe(String schemaTableName) {
        Table table = TABLE_MAP.get(schemaTableName);
        if (table != null && table instanceof SchemaTable) {
            return ((SchemaTable) table).fetchAllFe;
        }
        return false;
    }

    /**
     * For TABLE_MAP.
     **/
    public static class Builder {
        List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public Builder column(String name, ScalarType type) {
            columns.add(new Column(name, type, true));
            return this;
        }

        public List<Column> build() {
            return columns;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        TSchemaTable tSchemaTable = new TSchemaTable(SchemaTableType.getThriftType(this.name));
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                TABLE_MAP.get(this.name).getBaseSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }
}
