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
//        columns.add(new Column("Name", ScalarType.createCharType(50), pos++));
//        columns.add(new Column("Location", ScalarType.createCharType(50), pos++));
//        columns.add(new Column("Comment", ScalarType.createCharType(50), pos++));
//        tableMap.put("authors", columns);
//        // COLUMNS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TABLE_CATALOG", ScalarType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COLUMN_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ORDINAL_POSITION", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("COLUMN_DEFAULT", ScalarType.createCharType(MAX_FIELD_VARCHARLENGTH), pos++));
//        columns.add(new Column("IS_NULLABLE", ScalarType.createCharType(3), pos++));
//        columns.add(new Column("DATA_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_MAXIMUM_LENGTH", ScalarType.createType(PrimitiveType.BIGINT),
//            pos++));
//        columns.add(
//          new Column("CHARACTER_OCTET_LENGTH", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("NUMERIC_PRECISION", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("NUMERIC_SCALE", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_NAME", ScalarType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(
//          new Column("COLLATION_NAME", ScalarType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(new Column("COLUMN_TYPE", ScalarType.createCharType(65535), pos++));
//        columns.add(new Column("COLUMN_KEY", ScalarType.createCharType(3), pos++));
//        columns.add(new Column("EXTRA", ScalarType.createCharType(27), pos++));
//        columns.add(new Column("PRIVILEGES", ScalarType.createCharType(80), pos++));
//        columns.add(new Column("COLUMN_COMMENT", ScalarType.createCharType(255), pos++));
//        tableMap.put("columns", columns);
//        // create table
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("Catalog", ScalarType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("Schema", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("Table", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("Create Table", ScalarType.createCharType(65535), pos++));
//        tableMap.put("create_table", columns);
//        // ENGINES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("ENGINE", ScalarType.createCharType(64), pos++));
//        columns.add(new Column("SUPPORT", ScalarType.createCharType(8), pos++));
//        columns.add(new Column("COMMENT", ScalarType.createCharType(80), pos++));
//        columns.add(new Column("TRANSACTIONS", ScalarType.createCharType(3), pos++));
//        columns.add(new Column("XA", ScalarType.createCharType(3), pos++));
//        columns.add(new Column("SAVEPOINTS", ScalarType.createCharType(3), pos++));
//        tableMap.put("engines", columns);
//        // EVENTS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("EVENT_CATALOG", ScalarType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("EVENT_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("EVENT_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DEFINER", ScalarType.createCharType(77), pos++));
//        columns.add(new Column("TIME_ZONE", ScalarType.createCharType(64), pos++));
//        columns.add(new Column("EVENT_BODY", ScalarType.createCharType(8), pos++));
//        columns.add(new Column("EVENT_DEFINITION", ScalarType.createCharType(65535), pos++));
//        columns.add(new Column("EVENT_TYPE", ScalarType.createCharType(9), pos++));
//        columns.add(
//          new Column("EXECUTE_AT", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("INTERVAL_VALUE", ScalarType.createCharType(256), pos++));
//        columns.add(new Column("INTERVAL_FIELD", ScalarType.createCharType(18), pos++));
//        columns.add(new Column("SQL_MODE", ScalarType.createCharType(32 * 256), pos++));
//        columns.add(new Column("STARTS", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("ENDS", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("STATUS", ScalarType.createCharType(18), pos++));
//        columns.add(new Column("ON_COMPLETION", ScalarType.createCharType(12), pos++));
//        columns.add(new Column("CREATED", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(
//          new Column("LAST_ALTERED", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(
//          new Column("LAST_EXECUTED", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("EVENT_COMMENT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ORIGINATOR", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_CLIENT", ScalarType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(
//          new Column("COLLATION_CONNECTION", ScalarType.createCharType(MY_CS_NAME_SIZE), pos++));
//        columns.add(
//          new Column("DATABASE_COLLATION", ScalarType.createCharType(MY_CS_NAME_SIZE), pos++));
//        tableMap.put("events", columns);
//
//        // OPEN_TABLES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("Database", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("Table", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("In_use", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("Name_locked", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        tableMap.put("open_tables", columns);
//        // TABLE_NAMES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TABLE_CATALOG", ScalarType.createCharType(FN_REFLEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("table_names", columns);
//        // PLUGINS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("PLUGIN_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PLUGIN_VERSION", ScalarType.createCharType(20), pos++));
//        columns.add(new Column("PLUGIN_STATUS", ScalarType.createCharType(10), pos++));
//        columns.add(new Column("PLUGIN_TYPE", ScalarType.createCharType(80), pos++));
//        columns.add(new Column("PLUGIN_TYPE_VERSION", ScalarType.createCharType(20), pos++));
//        columns.add(new Column("PLUGIN_LIBRARY", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PLUGIN_LIBRARY_VERSION", ScalarType.createCharType(20), pos++));
//        columns.add(new Column("PLUGIN_AUTHOR", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PLUGIN_DESCRIPTION", ScalarType.createCharType(65535), pos++));
//        columns.add(new Column("PLUGIN_LICENSE", ScalarType.createCharType(80), pos++));
//        tableMap.put("plugins", columns);
//        // PROCESSLIST
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("ID", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("USER", ScalarType.createCharType(16), pos++));
//        columns.add(new Column("HOST", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DB", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COMMAND", ScalarType.createCharType(16), pos++));
//        columns.add(new Column("TIME", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("STATE", ScalarType.createCharType(64), pos++));
//        columns.add(new Column("INFO", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("processlist", columns);
//        // PROFILING
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("QUERY_ID", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("SEQ", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("STATE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DURATION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CPU_USER", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CPU_SYSTEM", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CONTEXT_VOLUNTARY", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("CONTEXT_INVOLUNTARY", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("BLOCK_OPS_IN", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("BLOCK_OPS_OUT", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("MESSAGES_SENT", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("MESSAGES_RECEIVED", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("PAGE_FAULTS_MAJOR", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(
//          new Column("PAGE_FAULTS_MINOR", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("SWAPS", ScalarType.createType(PrimitiveType.INT), pos++));
//        columns.add(new Column("SOURCE_FUNCTION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SOURCE_FILE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SOURCE_LINE", ScalarType.createType(PrimitiveType.INT), pos++));
//        tableMap.put("profiling", columns);
//        // TABLE PRIVILEGES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("GRANTEE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PRIVILEGE_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("IS_GRANTABLE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("table_privileges", columns);
//
//        // TABLE CONSTRAINTS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(
//          new Column("CONSTRAINT_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CONSTRAINT_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CONSTRAINT_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CONSTRAINT_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("table_constraints", columns);
//
//        // REFERENTIAL_CONSTRAINTS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(
//          new Column("CONSTRAINT_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CONSTRAINT_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CONSTRAINT_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("UNIQUE_CONSTRAINT_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("UNIQUE_CONSTRAINT_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("UNIQUE_CONSTRAINT_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("MATCH_OPTION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("UPDATE_RULE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DELETE_RULE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("REFERENCED_TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("referential_constraints", columns);
//
//        // VARIABLES
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("VARIABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("VARIABLE_VALUE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("variables", columns);
//
//        // ROUNTINE
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("SPECIFIC_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DTD_IDENTIFIER", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_BODY", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ROUTINE_DEFINITION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("EXTERNAL_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EXTERNAL_LANGUAGE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("PARAMETER_STYLE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("IS_DETERMINISTIC", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SQL_DATA_ACCESS", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SQL_PATH", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SECURITY_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CREATED", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(
//          new Column("LAST_ALTERED", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("SQL_MODE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ROUTINE_COMMENT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DEFINER", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_CLIENT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("COLLATION_CONNECTION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("DATABASE_COLLATION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("routines", columns);
//
//        // STATISTICS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TABLE_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("NON_UNIQUE", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("INDEX_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("INDEX_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SEQ_IN_INDEX", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("COLUMN_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COLLATION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CARDINALITY", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("SUB_PART", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("PACKED", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("NULLABLE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("INDEX_TYPE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("COMMENT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("statistics", columns);
//
//        // STATUS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("VARIABLE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("VARIABLE_VALUE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("status", columns);
//
//        // TRIGGERS
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("TRIGGER_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TRIGGER_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("TRIGGER_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_MANIPULATION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_OBJECT_CATALOG", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_OBJECT_SCHEMA", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("EVENT_OBJECT_TABLE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ACTION_ORDER", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(
//          new Column("ACTION_CONDITION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_STATEMENT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_ORIENTATION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ACTION_TIMING", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_OLD_TABLE", ScalarType.createCharType(NAME_CHAR_LEN),
//            pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_NEW_TABLE", ScalarType.createCharType(NAME_CHAR_LEN),
//            pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_OLD_ROW", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("ACTION_REFERENCE_NEW_ROW", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("CREATED", ScalarType.createType(PrimitiveType.TIMESTAMP), pos++));
//        columns.add(new Column("SQL_MODE", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DEFINER", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_CLIENT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("COLLATION_CONNECTION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("DATABASE_COLLATION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        tableMap.put("triggers", columns);
//
//      /* COLLATION */
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(new Column("COLLATION_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("CHARACTER_SET_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("ID", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        columns.add(new Column("IS_DEFAULT", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("IS_COMPILED", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("SORTLEN", ScalarType.createType(PrimitiveType.BIGINT), pos++));
//        tableMap.put("collations", columns);
//      /* CHARSETS */
//        pos = 0;
//        columns = new ArrayList();
//        columns.add(
//          new Column("CHARACTER_SET_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(
//          new Column("DEFAULT_COLLATE_NAME", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("DESCRIPTION", ScalarType.createCharType(NAME_CHAR_LEN), pos++));
//        columns.add(new Column("MAXLEN", ScalarType.createType(PrimitiveType.BIGINT), pos++));
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
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
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
                                    .column("TABLE_COMMENT", ScalarType.createVarchar(2048))
                                    .build()))
                    .put("table_privileges", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "table_privileges",
                            TableType.SCHEMA,
                            builder()
                                    .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("PRIVILEGE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("IS_GRANTABLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .build()))
                    .put("referential_constraints", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "referential_constraints",
                            TableType.SCHEMA,
                            builder()
                                    .column("CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UNIQUE_CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("MATCH_OPTION", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("UPDATE_RULE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("DELETE_RULE", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .column("REFERENCED_TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .build()))
                    .put("key_column_usage", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "key_column_usage",
                            TableType.SCHEMA,
                            builder()
                                    .column("CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
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
                                    .column("REFERENCED_COLUMN_NAME", ScalarType.createVarchar(64))
                                    .build()))
                    .put("routines", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "routines",
                            TableType.SCHEMA,
                            builder()
                                    .column("SPECIFIC_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
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
                                    .column("DATABASE_COLLATION", ScalarType.createVarchar(NAME_CHAR_LEN))
                                    .build()))
                    .put("schemata", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "schemata",
                            TableType.SCHEMA,
                            builder()
                                    .column("CATALOG_NAME", ScalarType.createVarchar(512))
                                    .column("SCHEMA_NAME", ScalarType.createVarchar(32))
                                    .column("DEFAULT_CHARACTER_SET_NAME", ScalarType.createVarchar(32))
                                    .column("DEFAULT_COLLATION_NAME", ScalarType.createVarchar(32))
                                    .column("SQL_PATH", ScalarType.createVarchar(512))
                                    .build()))
                    .put("session_variables", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "session_variables",
                            TableType.SCHEMA,
                            builder()
                                    .column("VARIABLE_NAME", ScalarType.createVarchar(64))
                                    .column("VARIABLE_VALUE", ScalarType.createVarchar(1024))
                                    .build()))
                    .put("global_variables", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "global_variables",
                            TableType.SCHEMA,
                            builder()
                                    .column("VARIABLE_NAME", ScalarType.createVarchar(64))
                                    .column("VARIABLE_VALUE", ScalarType.createVarchar(1024))
                                    .build()))
                    .put("columns", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "columns",
                            TableType.SCHEMA,
                            builder()
                                    .column("TABLE_CATALOG", ScalarType.createVarchar(512))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("COLUMN_NAME", ScalarType.createVarchar(64))
                                    .column("ORDINAL_POSITION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("COLUMN_DEFAULT", ScalarType.createVarchar(1024))
                                    .column("IS_NULLABLE", ScalarType.createVarchar(3))
                                    .column("DATA_TYPE", ScalarType.createVarchar(64))
                                    .column("CHARACTER_MAXIMUM_LENGTH",
                                            ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CHARACTER_OCTET_LENGTH",
                                            ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("NUMERIC_PRECISION", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("NUMERIC_SCALE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("CHARACTER_SET_NAME", ScalarType.createVarchar(32))
                                    .column("COLLATION_NAME", ScalarType.createVarchar(32))
                                    .column("COLUMN_TYPE", ScalarType.createVarchar(32))
                                    .column("COLUMN_KEY", ScalarType.createVarchar(3))
                                    .column("EXTRA", ScalarType.createVarchar(27))
                                    .column("PRIVILEGES", ScalarType.createVarchar(80))
                                    .column("COLUMN_COMMENT", ScalarType.createVarchar(255))
                                    .column("COLUMN_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("DECIMAL_DIGITS", ScalarType.createType(PrimitiveType.BIGINT))
                                    .build()))
                    .put("character_sets", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "character_sets",
                            TableType.SCHEMA,
                            builder()
                                    .column("CHARACTER_SET_NAME", ScalarType.createVarchar(512))
                                    .column("DEFAULT_COLLATE_NAME", ScalarType.createVarchar(64))
                                    .column("DESCRIPTION", ScalarType.createVarchar(64))
                                    .column("MAXLEN", ScalarType.createType(PrimitiveType.BIGINT))
                                    .build()))
                    .put("collations", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "collations",
                            TableType.SCHEMA,
                            builder()
                                    .column("COLLATION_NAME", ScalarType.createVarchar(512))
                                    .column("CHARACTER_SET_NAME", ScalarType.createVarchar(64))
                                    .column("ID", ScalarType.createType(PrimitiveType.BIGINT))
                                    .column("IS_DEFAULT", ScalarType.createVarchar(64))
                                    .column("IS_COMPILED", ScalarType.createVarchar(64))
                                    .column("SORTLEN", ScalarType.createType(PrimitiveType.BIGINT))
                                    .build()))
                    .put("table_constraints", new SchemaTable(
                            SystemIdGenerator.getNextId(),
                            "table_constraints",
                            TableType.SCHEMA,
                            builder()
                                    .column("CONSTRAINT_CATALOG", ScalarType.createVarchar(512))
                                    .column("CONSTRAINT_SCHEMA", ScalarType.createVarchar(64))
                                    .column("CONSTRAINT_NAME", ScalarType.createVarchar(64))
                                    .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                                    .column("TABLE_NAME", ScalarType.createVarchar(64))
                                    .column("CONSTRAINT_TYPE", ScalarType.createVarchar(64))
                                    .build()))
                    .put("engines",
                         new SchemaTable(
                                         SystemIdGenerator.getNextId(),
                                         "engines",
                                         TableType.SCHEMA,
                                         builder()
                                                 .column("ENGINE", ScalarType.createVarchar(64))
                                                 .column("SUPPORT", ScalarType.createVarchar(8))
                                                 .column("COMMENT", ScalarType.createVarchar(80))
                                                 .column("TRANSACTIONS", ScalarType.createVarchar(3))
                                                 .column("XA", ScalarType.createVarchar(3))
                                                 .column("SAVEPOINTS", ScalarType.createVarchar(3))
                                                 .build()))
                    .build();

    public static class Builder {
        List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public Builder column(String name, ScalarType type) {
            columns.add(new Column(name, type.getPrimitiveType(), true));
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
