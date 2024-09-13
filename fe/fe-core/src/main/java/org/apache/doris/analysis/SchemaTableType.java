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

package org.apache.doris.analysis;

import org.apache.doris.thrift.TSchemaTableType;

public enum SchemaTableType {
    SCH_AUTHORS("AUTHORS", "AUTHORS", TSchemaTableType.SCH_AUTHORS),
    SCH_CHARSETS("CHARSETS", "CHARACTER_SETS", TSchemaTableType.SCH_CHARSETS),
    SCH_COLLATIONS("COLLATIONS", "COLLATIONS", TSchemaTableType.SCH_COLLATIONS),
    SCH_COLLATION_CHARACTER_SET_APPLICABILITY("COLLATION_CHARACTER_SET_APPLICABILITY",
            "COLLATION_CHARACTER_SET_APPLICABILITY",
            TSchemaTableType.SCH_COLLATION_CHARACTER_SET_APPLICABILITY),
    SCH_COLUMNS("COLUMNS", "COLUMNS", TSchemaTableType.SCH_COLUMNS),
    SCH_COLUMN_PRIVILEGES("COLUMN_PRIVILEGES", "COLUMN_PRIVILEGES",
            TSchemaTableType.SCH_COLUMN_PRIVILEGES),
    SCH_ENGINES("ENGINES", "ENGINES", TSchemaTableType.SCH_ENGINES),
    SCH_EVENTS("EVENTS", "EVENTS", TSchemaTableType.SCH_EVENTS),
    SCH_FILES("FILES", "FILES", TSchemaTableType.SCH_FILES),
    SCH_GLOBAL_STATUS("GLOBAL_STATUS", "GLOBAL_STATUS", TSchemaTableType.SCH_GLOBAL_STATUS),
    SCH_GLOBAL_VARIABLES("GLOBAL_VARIABLES", "GLOBAL_VARIABLES",
            TSchemaTableType.SCH_GLOBAL_VARIABLES),
    SCH_KEY_COLUMN_USAGE("KEY_COLUMN_USAGE", "KEY_COLUMN_USAGE",
            TSchemaTableType.SCH_KEY_COLUMN_USAGE),
    SCH_OPEN_TABLES("OPEN_TABLES", "OPEN_TABLES", TSchemaTableType.SCH_OPEN_TABLES),
    SCH_PARTITIONS("PARTITIONS", "PARTITIONS", TSchemaTableType.SCH_PARTITIONS),
    SCH_PLUGINS("PLUGINS", "PLUGINS", TSchemaTableType.SCH_PLUGINS),
    SCH_PROCESSLIST("PROCESSLIST", "PROCESSLIST", TSchemaTableType.SCH_PROCESSLIST),
    SCH_PROFILES("PROFILES", "PROFILES", TSchemaTableType.SCH_PROFILES),
    SCH_REFERENTIAL_CONSTRAINTS("REFERENTIAL_CONSTRAINTS", "REFERENTIAL_CONSTRAINTS",
            TSchemaTableType.SCH_REFERENTIAL_CONSTRAINTS),
    SCH_PROCEDURES("ROUTINES", "ROUTINES", TSchemaTableType.SCH_PROCEDURES),
    SCH_SCHEMATA("SCHEMATA", "SCHEMATA", TSchemaTableType.SCH_SCHEMATA),
    SCH_SCHEMA_PRIVILEGES("SCHEMA_PRIVILEGES", "SCHEMA_PRIVILEGES",
            TSchemaTableType.SCH_SCHEMA_PRIVILEGES),
    SCH_SESSION_STATUS("SESSION_STATUS", "SESSION_STATUS", TSchemaTableType.SCH_SESSION_STATUS),
    SCH_SESSION_VARIABLES("SESSION_VARIABLES", "SESSION_VARIABLES",
            TSchemaTableType.SCH_SESSION_VARIABLES),
    SCH_STATISTICS("STATISTICS", "STATISTICS", TSchemaTableType.SCH_STATISTICS),
    SCH_COLUMN_STATISTICS("COLUMN_STATISTICS", "COLUMN_STATISTICS", TSchemaTableType.SCH_COLUMN_STATISTICS),
    SCH_STATUS("STATUS", "STATUS", TSchemaTableType.SCH_STATUS),
    SCH_TABLES("TABLES", "TABLES", TSchemaTableType.SCH_TABLES),
    SCH_TABLE_CONSTRAINTS("TABLE_CONSTRAINTS", "TABLE_CONSTRAINTS",
            TSchemaTableType.SCH_TABLE_CONSTRAINTS),
    SCH_TABLE_NAMES("TABLE_NAMES", "TABLE_NAMES", TSchemaTableType.SCH_TABLE_NAMES),
    SCH_TABLE_PRIVILEGES("TABLE_PRIVILEGES", "TABLE_PRIVILEGES",
            TSchemaTableType.SCH_TABLE_PRIVILEGES),
    SCH_TRIGGERS("TRIGGERS", "TRIGGERS", TSchemaTableType.SCH_TRIGGERS),
    SCH_USER_PRIVILEGES("USER_PRIVILEGES", "USER_PRIVILEGES", TSchemaTableType.SCH_USER_PRIVILEGES),
    SCH_VARIABLES("VARIABLES", "VARIABLES", TSchemaTableType.SCH_VARIABLES),
    SCH_VIEWS("VIEWS", "VIEWS", TSchemaTableType.SCH_VIEWS),
    SCH_CREATE_TABLE("CREATE_TABLE", "CREATE_TABLE", TSchemaTableType.SCH_CREATE_TABLE),
    SCH_INVALID("NULL", "NULL", TSchemaTableType.SCH_INVALID),
    SCH_ROWSETS("ROWSETS", "ROWSETS", TSchemaTableType.SCH_ROWSETS),
    SCH_PARAMETERS("PARAMETERS", "PARAMETERS", TSchemaTableType.SCH_PARAMETERS),
    SCH_METADATA_NAME_IDS("METADATA_NAME_IDS", "METADATA_NAME_IDS", TSchemaTableType.SCH_METADATA_NAME_IDS),
    SCH_PROFILING("PROFILING", "PROFILING", TSchemaTableType.SCH_PROFILING),
    SCH_BACKEND_ACTIVE_TASKS("BACKEND_ACTIVE_TASKS", "BACKEND_ACTIVE_TASKS", TSchemaTableType.SCH_BACKEND_ACTIVE_TASKS),
    SCH_ACTIVE_QUERIES("ACTIVE_QUERIES", "ACTIVE_QUERIES", TSchemaTableType.SCH_ACTIVE_QUERIES),
    SCH_WORKLOAD_GROUPS("WORKLOAD_GROUPS", "WORKLOAD_GROUPS", TSchemaTableType.SCH_WORKLOAD_GROUPS),
    SCHE_USER("user", "user", TSchemaTableType.SCH_USER),
    SCH_PROCS_PRIV("procs_priv", "procs_priv", TSchemaTableType.SCH_PROCS_PRIV),
    SCH_WORKLOAD_POLICY("WORKLOAD_POLICY", "WORKLOAD_POLICY",
            TSchemaTableType.SCH_WORKLOAD_POLICY),
    SCH_TABLE_OPTIONS("TABLE_OPTIONS", "TABLE_OPTIONS",
            TSchemaTableType.SCH_TABLE_OPTIONS),
    SCH_WORKLOAD_GROUP_PRIVILEGES("WORKLOAD_GROUP_PRIVILEGES",
            "WORKLOAD_GROUP_PRIVILEGES", TSchemaTableType.SCH_WORKLOAD_GROUP_PRIVILEGES),
    SCH_WORKLOAD_GROUP_RESOURCE_USAGE("WORKLOAD_GROUP_RESOURCE_USAGE",
            "WORKLOAD_GROUP_RESOURCE_USAGE", TSchemaTableType.SCH_WORKLOAD_GROUP_RESOURCE_USAGE),
    SCH_TABLE_PROPERTIES("TABLE_PROPERTIES", "TABLE_PROPERTIES",
            TSchemaTableType.SCH_TABLE_PROPERTIES),
    SCH_CATALOG_META_CACHE_STATISTICS("CATALOG_META_CACHE_STATISTICS", "CATALOG_META_CACHE_STATISTICS",
            TSchemaTableType.SCH_CATALOG_META_CACHE_STATISTICS);

    private static final String dbName = "INFORMATION_SCHEMA";
    private static SelectList fullSelectLists;

    static {
        fullSelectLists = new SelectList();
        fullSelectLists.addItem(SelectListItem.createStarItem(null));
    }

    private final String description;
    private final String tableName;
    private final TSchemaTableType tableType;

    SchemaTableType(String description, String tableName, TSchemaTableType tableType) {
        this.description = description;
        this.tableName = tableName;
        this.tableType = tableType;
    }

    public static TSchemaTableType getThriftType(String name) {
        for (SchemaTableType type : SchemaTableType.values()) {
            if (type.tableName.equalsIgnoreCase(name)) {
                return type.tableType;
            }
        }
        return null;
    }

    public String toString() {
        return description;
    }

    public TSchemaTableType toThrift() {
        return tableType;
    }
}
