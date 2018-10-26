// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.SchemaTableType;
import com.baidu.palo.common.SystemIdGenerator;
import com.baidu.palo.thrift.TSchemaTable;
import com.baidu.palo.thrift.TTableDescriptor;
import com.baidu.palo.thrift.TTableType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Palo representation of MySQL information schema table metadata,
 */
public class SchemaTable extends Table {
    private final static int FN_REFLEN               = 512;
    private final static int NAME_CHAR_LEN           = 64;
    private final static int MAX_FIELD_VARCHARLENGTH = 65535;
    private final static int MY_CS_NAME_SIZE         = 32;
    private SchemaTableType schemaTableType;

//    static {
//        tableMap = Maps.newHashMap();
//        List<Column> columns;
//        int pos = 0;
//        // AUTHORS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("Name", ColumnType.createCharType(50), pos++));
//        columns.add(new Column("Location", ColumnType.createCharType(50), pos++));
//        columns.add(new Column("Comment", ColumnType.createCharType(50), pos++));
//        tableMap.put("authors", columns);
//        // COLUMNS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TABLE_CATALOG", ColumnType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COLUMN_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ORDINAL_POSITION", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("COLUMN_DEFAULT", ColumnType.createCharType(MAX_FIELD_VARCHARLENGTH), pos++));
//        columns.add(new Column("IS_NULLABLE", ColumnType.createCharType(3), pos++));
//        columns.add(new Column("DATA_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_MAXIMUM_LENGTH", ColumnType.createType(PrimitiveType.BIGINT),
//            pos++));
//        columns.add(
//          new Column("CHARACTER_OCTET_LENGTH", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("NUMERIC_PRECISION", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("NUMERIC_SCALE", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_NAME", ColumnType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(
//          new Column("COLLATION_NAME", ColumnType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(new Column("COLUMN_TYPE", ColumnType.createCharType(65535), pos++));
//        columns.add(new Column("COLUMN_KEY", ColumnType.createCharType(3), pos++));
//        columns.add(new Column("EXTRA", ColumnType.createCharType(27), pos++));
//        columns.add(new Column("PRIVILEGES", ColumnType.createCharType(80), pos++));
//        columns.add(new Column("COLUMN_COMMENT", ColumnType.createCharType(255), pos++));
//        tableMap.put("columns", columns);
//        // create table
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("Catalog", ColumnType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("Schema", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("Table", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("Create Table", ColumnType.createCharType(65535), pos++));
//        tableMap.put("create_table", columns);
//        // ENGINES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("ENGINE", ColumnType.createCharType(64), pos++));
//        columns.add(new Column("SUPPORT", ColumnType.createCharType(8), pos++));
//        columns.add(new Column("COMMENT", ColumnType.createCharType(80), pos++));
//        columns.add(new Column("TRANSACTIONS", ColumnType.createCharType(3), pos++));
//        columns.add(new Column("XA", ColumnType.createCharType(3), pos++));
//        columns.add(new Column("SAVEPOINTS", ColumnType.createCharType(3), pos++));
//        tableMap.put("engines", columns);
//        // EVENTS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("EVENT_CATALOG", ColumnType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("EVENT_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("EVENT_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DEFINER", ColumnType.createCharType(77), pos++));
//        columns.add(new Column("TIME_ZONE", ColumnType.createCharType(64), pos++));
//        columns.add(new Column("EVENT_BODY", ColumnType.createCharType(8), pos++));
//        columns.add(new Column("EVENT_DEFINITION", ColumnType.createCharType(65535), pos++));
//        columns.add(new Column("EVENT_TYPE", ColumnType.createCharType(9), pos++));
//        columns.add(
//          new Column("EXECUTE_AT", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("INTERVAL_VALUE", ColumnType.createCharType(256), pos++));
//        columns.add(new Column("INTERVAL_FIELD", ColumnType.createCharType(18), pos++));
//        columns.add(new Column("SQL_MODE", ColumnType.createCharType(32 * 256), pos++));
//        columns.add(new Column("STARTS", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("ENDS", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("STATUS", ColumnType.createCharType(18), pos++));
//        columns.add(new Column("ON_COMPLETION", ColumnType.createCharType(12), pos++));
//        columns.add(new Column("CREATED", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(
//          new Column("LAST_ALTERED", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(
//          new Column("LAST_EXECUTED", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("EVENT_COMMENT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ORIGINATOR", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_CLIENT", ColumnType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(
//          new Column("COLLATION_CONNECTION", ColumnType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(
//          new Column("DATABASE_COLLATION", ColumnType.createCharType(MY_CS_NAME_SIZE), pos++));
//        tableMap.put("events", columns);
//
//        // OPEN_TABLES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("Database", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("Table", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("In_use", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("Name_locked", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        tableMap.put("open_tables", columns);
//        // TABLE_NAMES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TABLE_CATALOG", ColumnType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("table_names", columns);
//        // PLUGINS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("PLUGIN_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PLUGIN_VERSION", ColumnType.createCharType(20), pos++));
//        columns.add(new Column("PLUGIN_STATUS", ColumnType.createCharType(10), pos++));
//        columns.add(new Column("PLUGIN_TYPE", ColumnType.createCharType(80), pos++));
//        columns.add(new Column("PLUGIN_TYPE_VERSION", ColumnType.createCharType(20), pos++));
//        columns.add(new Column("PLUGIN_LIBRARY", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PLUGIN_LIBRARY_VERSION", ColumnType.createCharType(20), pos++));
//        columns.add(new Column("PLUGIN_AUTHOR", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PLUGIN_DESCRIPTION", ColumnType.createCharType(65535), pos++));
//        columns.add(new Column("PLUGIN_LICENSE", ColumnType.createCharType(80), pos++));
//        tableMap.put("plugins", columns);
//        // PROCESSLIST
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("ID", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("USER", ColumnType.createCharType(16), pos++));
//        columns.add(new Column("HOST", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DB", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COMMAND", ColumnType.createCharType(16), pos++));
//        columns.add(new Column("TIME", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("STATE", ColumnType.createCharType(64), pos++));
//        columns.add(new Column("INFO", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("processlist", columns);
//        // PROFILING
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("QUERY_ID", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("SEQ", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("STATE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DURATION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CPU_USER", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CPU_SYSTEM", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CONTEXT_VOLUNTARY", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("CONTEXT_INVOLUNTARY", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("BLOCK_OPS_IN", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("BLOCK_OPS_OUT", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("MESSAGES_SENT", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("MESSAGES_RECEIVED", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("PAGE_FAULTS_MAJOR", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("PAGE_FAULTS_MINOR", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("SWAPS", ColumnType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("SOURCE_FUNCTION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SOURCE_FILE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SOURCE_LINE", ColumnType.createType(PrimitiveType.INT), pos++));
//        tableMap.put("profiling", columns);
//        // TABLE PRIVILEGES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("GRANTEE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PRIVILEGE_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("IS_GRANTABLE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("table_privileges", columns);
//
//        // TABLE CONSTRAINTS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(
//          new Column("CONSTRAINT_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CONSTRAINT_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CONSTRAINT_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CONSTRAINT_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("table_constraints", columns);
//
//        // REFERENTIAL_CONSTRAINTS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(
//          new Column("CONSTRAINT_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CONSTRAINT_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CONSTRAINT_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("UNIQUE_CONSTRAINT_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("UNIQUE_CONSTRAINT_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("UNIQUE_CONSTRAINT_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("MATCH_OPTION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("UPDATE_RULE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DELETE_RULE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("REFERENCED_TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("referential_constraints", columns);
//
//        // VARIABLES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("VARIABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("VARIABLE_VALUE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("variables", columns);
//
//        // ROUNTINE
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("SPECIFIC_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DTD_IDENTIFIER", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_BODY", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ROUTINE_DEFINITION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("EXTERNAL_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EXTERNAL_LANGUAGE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PARAMETER_STYLE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("IS_DETERMINISTIC", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SQL_DATA_ACCESS", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SQL_PATH", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SECURITY_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CREATED", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(
//          new Column("LAST_ALTERED", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("SQL_MODE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_COMMENT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DEFINER", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_CLIENT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("COLLATION_CONNECTION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("DATABASE_COLLATION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("routines", columns);
//
//        // STATISTICS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TABLE_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("NON_UNIQUE", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("INDEX_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("INDEX_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SEQ_IN_INDEX", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("COLUMN_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COLLATION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CARDINALITY", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("SUB_PART", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("PACKED", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("NULLABLE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("INDEX_TYPE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COMMENT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("statistics", columns);
//
//        // STATUS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("VARIABLE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("VARIABLE_VALUE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("status", columns);
//
//        // TRIGGERS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TRIGGER_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TRIGGER_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TRIGGER_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_MANIPULATION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_OBJECT_CATALOG", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_OBJECT_SCHEMA", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_OBJECT_TABLE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ACTION_ORDER", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("ACTION_CONDITION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_STATEMENT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_ORIENTATION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ACTION_TIMING", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_OLD_TABLE", ColumnType.createCharType(NAME_CHAR_LEN),
//            pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_NEW_TABLE", ColumnType.createCharType(NAME_CHAR_LEN),
//            pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_OLD_ROW", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_NEW_ROW", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CREATED", ColumnType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("SQL_MODE", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DEFINER", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_CLIENT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("COLLATION_CONNECTION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("DATABASE_COLLATION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("triggers", columns);
//
//      /* COLLATION */
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("COLLATION_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ID", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("IS_DEFAULT", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("IS_COMPILED", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SORTLEN", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        tableMap.put("collations", columns);
//      /* CHARSETS */
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(
//          new Column("CHARACTER_SET_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("DEFAULT_COLLATE_NAME", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DESCRIPTION", ColumnType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("MAXLEN", ColumnType.createType(PrimitiveType.BIGINT), pos++));
//        tableMap.put("character_sets", columns);
//    }

    protected SchemaTable(long id, String name, TableType type, List<Column> baseSchema) {
        super(id, name, type, baseSchema);
    }

    protected SchemaTable(long id, String name, SchemaTableType type) {
        super(TableType.SCHEMA);
        schemaTableType = type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("Do not allow to write SchemaTable to image.");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Do not allow read SchemaTable from image.");
    }

    public static Builder builder() {
        return new Builder();
    }

    // Now we just mock tables, table_privileges, referential_constraints, key_column_usage and routines table
    // Because in MySQL ODBC, these tables are used.
    // TODO(zhaochun): Review some commercial BI to check if we need support where clause in show statement
    // like 'show table where_clause'. If we decide to support it, we must mock these related table here.
    public static Map<String, Table> TABLE_MAP =
            ImmutableMap
                    .<String, Table> builder()
                    .put("tables", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "tables",
                            TableType.SCHEMA,
                            builder()
                                    .column("TABLE_CATALOG", ColumnType.createVarchar(FN_REFLEN))
                                    .column("TABLE_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_TYPE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ENGINE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("VERSION", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("ROW_FORMAT", ColumnType.createVarchar(10))
                                    .column("TABLE_ROWS", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("AVG_ROW_LENGTH", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("DATA_LENGTH", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("MAX_DATA_LENGTH", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("INDEX_LENGTH", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("DATA_FREE", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("AUTO_INCREMENT", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("CREATE_TIME", ColumnType.createType(PrimitiveType.DATETIME))
                                    .column("UPDATE_TIME", ColumnType.createType(PrimitiveType.DATETIME))
                                    .column("CHECK_TIME", ColumnType.createType(PrimitiveType.DATETIME))
                                    .column("TABLE_COLLATION", ColumnType.createVarchar(MY_CS_NAME_SIZE))
                                    .column("CHECKSUM", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("CREATE_OPTIONS", ColumnType.createVarchar(255))
                                    .column("TABLE_COMMENT", ColumnType.createVarchar(2048))
                                    .build()))
                    .put("table_privileges", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "table_privileges",
                            TableType.SCHEMA,
                            builder()
                                    .column("GRANTEE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_CATALOG", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("PRIVILEGE_TYPE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("IS_GRANTABLE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .build()))
                    .put("referential_constraints", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "referential_constraints",
                            TableType.SCHEMA,
                            builder()
                                    .column("CONSTRAINT_CATALOG", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_CATALOG", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("MATCH_OPTION", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("UPDATE_RULE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("DELETE_RULE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("REFERENCED_TABLE_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .build()))
                    .put("key_column_usage", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "key_column_usage",
                            TableType.SCHEMA,
                            builder()
                                    .column("CONSTRAINT_CATALOG", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_CATALOG", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("COLUMN_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ORDINAL_POSITION", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("POSITION_IN_UNIQUE_CONSTRAINT",
                                        ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("REFERENCED_TABLE_SCHEMA", ColumnType.createVarchar(64))
                                    .column("REFERENCED_TABLE_NAME", ColumnType.createVarchar(64))
                                    .column("REFERENCED_COLUMN_NAME", ColumnType.createVarchar(64))
                                    .build()))
                    .put("routines", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "routines",
                            TableType.SCHEMA,
                            builder()
                                    .column("SPECIFIC_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_CATALOG", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_SCHEMA", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_TYPE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("DTD_IDENTIFIER", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_BODY", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_DEFINITION", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("EXTERNAL_NAME", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("EXTERNAL_LANGUAGE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("PARAMETER_STYLE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("IS_DETERMINISTIC", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("SQL_DATA_ACCESS", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("SQL_PATH", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("SECURITY_TYPE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("CREATED", ColumnType.createType(PrimitiveType.DATETIME))
                                    .column("LAST_ALTERED", ColumnType.createType(PrimitiveType.DATETIME))
                                    .column("SQL_MODE", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("ROUTINE_COMMENT", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("DEFINER", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("CHARACTER_SET_CLIENT", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("COLLATION_CONNECTION", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .column("DATABASE_COLLATION", ColumnType.createVarchar(NAME_CHAR_LEN))
                                    .build()))
                    .put("schemata", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "schemata",
                            TableType.SCHEMA,
                            builder()
                                    .column("CATALOG_NAME", ColumnType.createVarchar(512))
                                    .column("SCHEMA_NAME", ColumnType.createVarchar(32))
                                    .column("DEFAULT_CHARACTER_SET_NAME", ColumnType.createVarchar(32))
                                    .column("DEFAULT_COLLATION_NAME", ColumnType.createVarchar(32))
                                    .column("SQL_PATH", ColumnType.createVarchar(512))
                                    .build()))
                    .put("session_variables", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "session_variables",
                            TableType.SCHEMA,
                            builder()
                                    .column("VARIABLE_NAME", ColumnType.createVarchar(64))
                                    .column("VARIABLE_VALUE", ColumnType.createVarchar(1024))
                                    .build()))
                    .put("global_variables", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "global_variables",
                            TableType.SCHEMA,
                            builder()
                                    .column("VARIABLE_NAME", ColumnType.createVarchar(64))
                                    .column("VARIABLE_VALUE", ColumnType.createVarchar(1024))
                                    .build()))
                    .put("columns", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "columns",
                            TableType.SCHEMA,
                            builder()
                                    .column("TABLE_CATALOG", ColumnType.createVarchar(512))
                                    .column("TABLE_SCHEMA", ColumnType.createVarchar(64))
                                    .column("TABLE_NAME", ColumnType.createVarchar(64))
                                    .column("COLUMN_NAME", ColumnType.createVarchar(64))
                                    .column("ORDINAL_POSITION", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("COLUMN_DEFAULT", ColumnType.createVarchar(1024))
                                    .column("IS_NULLABLE", ColumnType.createVarchar(3))
                                    .column("DATA_TYPE", ColumnType.createVarchar(64))
                                    .column("CHARACTER_MAXIMUM_LENGTH",
                                            ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("CHARACTER_OCTET_LENGTH",
                                            ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("NUMERIC_PRECISION", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("NUMERIC_SCALE", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("CHARACTER_SET_NAME", ColumnType.createVarchar(32))
                                    .column("COLLATION_NAME", ColumnType.createVarchar(32))
                                    .column("COLUMN_TYPE", ColumnType.createVarchar(32))
                                    .column("COLUMN_KEY", ColumnType.createVarchar(3))
                                    .column("EXTRA", ColumnType.createVarchar(27))
                                    .column("PRIVILEGES", ColumnType.createVarchar(80))
                                    .column("COLUMN_COMMENT", ColumnType.createVarchar(255))
                                    .column("COLUMN_SIZE", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("DECIMAL_DIGITS", ColumnType.createType(PrimitiveType.BIGINT))
                                    .build()))
                    .put("character_sets", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "character_sets",
                            TableType.SCHEMA,
                            builder()
                                    .column("CHARACTER_SET_NAME", ColumnType.createVarchar(512))
                                    .column("DEFAULT_COLLATE_NAME", ColumnType.createVarchar(64))
                                    .column("DESCRIPTION", ColumnType.createVarchar(64))
                                    .column("MAXLEN", ColumnType.createType(PrimitiveType.BIGINT))
                                    .build()))
                    .put("collations", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "collations",
                            TableType.SCHEMA,
                            builder()
                                    .column("COLLATION_NAME", ColumnType.createVarchar(512))
                                    .column("CHARACTER_SET_NAME", ColumnType.createVarchar(64))
                                    .column("ID", ColumnType.createType(PrimitiveType.BIGINT))
                                    .column("IS_DEFAULT", ColumnType.createVarchar(64))
                                    .column("IS_COMPILED", ColumnType.createVarchar(64))
                                    .column("SORTLEN", ColumnType.createType(PrimitiveType.BIGINT))
                                    .build()))
                    .put("table_constraints", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "table_constraints",
                            TableType.SCHEMA,
                            builder()
                                    .column("CONSTRAINT_CATALOG", ColumnType.createVarchar(512))
                                    .column("CONSTRAINT_SCHEMA", ColumnType.createVarchar(64))
                                    .column("CONSTRAINT_NAME", ColumnType.createVarchar(64))
                                    .column("TABLE_SCHEMA", ColumnType.createVarchar(64))
                                    .column("TABLE_NAME", ColumnType.createVarchar(64))
                                    .column("CONSTRAINT_TYPE", ColumnType.createVarchar(64))
                                    .build()))
                    .put("engines",
                         new SchemaTable(
                                         SystemIdGenerator.getNextId(),
                                         "engines",
                                         TableType.SCHEMA,
                                         builder()
                                                 .column("ENGINE", ColumnType.createVarchar(64))
                                                 .column("SUPPORT", ColumnType.createVarchar(8))
                                                 .column("COMMENT", ColumnType.createVarchar(80))
                                                 .column("TRANSACTIONS", ColumnType.createVarchar(3))
                                                 .column("XA", ColumnType.createVarchar(3))
                                                 .column("SAVEPOINTS", ColumnType.createVarchar(3))
                                                 .build()))
                    .build();

    public static class Builder {
        List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public Builder column(String name, ColumnType type) {
            columns.add(new Column(name, type.getType(), true));
            return this;
        }

        public List<Column> build() {
            return columns;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        TSchemaTable tSchemaTable = new TSchemaTable(SchemaTableType.getThriftType(this.name));
        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                TABLE_MAP.get(this.name).getBaseSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }
}
