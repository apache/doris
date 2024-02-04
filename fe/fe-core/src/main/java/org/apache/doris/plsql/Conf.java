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
// This file is copied from
// https://github.com/apache/hive/blob/master/plsql/src/main/java/org/apache/hive/plsql/Conf.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.plsql.Exec.OnError;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;

/**
 * PL/SQL run-time configuration
 */
public class Conf extends Configuration {

    public static final String DOT_PLSQLRC = ".plsqlrc";
    public static final String PLSQLRC = "plsqlrc";
    public static final String PLSQL_LOCALS_SQL = "plsql_locals.sql";

    public static final String CONN_CONVERT = "plsql.conn.convert.";
    public static final String CONN_DEFAULT = "plsql.conn.default";
    public static final String DUAL_TABLE = "plsql.dual.table";
    public static final String INSERT_VALUES = "plsql.insert.values";
    public static final String ONERROR = "plsql.onerror";
    public static final String TEMP_TABLES = "plsql.temp.tables";
    public static final String TEMP_TABLES_SCHEMA = "plsql.temp.tables.schema";
    public static final String TEMP_TABLES_LOCATION = "plsql.temp.tables.location";

    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String YES = "yes";
    public static final String NO = "no";

    public enum InsertValues {
        NATIVE, SELECT
    }

    public enum TempTables {
        NATIVE, MANAGED
    }

    public String defaultConnection;

    OnError onError = OnError.EXCEPTION;
    InsertValues insertValues = InsertValues.NATIVE;
    TempTables tempTables = TempTables.NATIVE;

    String dualTable = null;

    String tempTablesSchema = "";
    String tempTablesLocation = "/tmp/plsql";

    HashMap<String, Boolean> connConvert = new HashMap<String, Boolean>();

    /**
     * Set an option
     */
    public void setOption(String key, String value) {
        if (key.startsWith(CONN_CONVERT)) {
            setConnectionConvert(key.substring(19), value);
        } else if (key.compareToIgnoreCase(CONN_DEFAULT) == 0) {
            defaultConnection = value;
        } else if (key.compareToIgnoreCase(DUAL_TABLE) == 0) {
            dualTable = value;
        } else if (key.compareToIgnoreCase(INSERT_VALUES) == 0) {
            setInsertValues(value);
        } else if (key.compareToIgnoreCase(ONERROR) == 0) {
            setOnError(value);
        } else if (key.compareToIgnoreCase(TEMP_TABLES) == 0) {
            setTempTables(value);
        } else if (key.compareToIgnoreCase(TEMP_TABLES_SCHEMA) == 0) {
            tempTablesSchema = value;
        } else if (key.compareToIgnoreCase(TEMP_TABLES_LOCATION) == 0) {
            tempTablesLocation = value;
        }
    }

    /**
     * Set plsql.insert.values option
     */
    private void setInsertValues(String value) {
        if (value.compareToIgnoreCase("NATIVE") == 0) {
            insertValues = InsertValues.NATIVE;
        } else if (value.compareToIgnoreCase("SELECT") == 0) {
            insertValues = InsertValues.SELECT;
        }
    }

    /**
     * Set plsql.temp.tables option
     */
    private void setTempTables(String value) {
        if (value.compareToIgnoreCase("NATIVE") == 0) {
            tempTables = TempTables.NATIVE;
        } else if (value.compareToIgnoreCase("MANAGED") == 0) {
            tempTables = TempTables.MANAGED;
        }
    }

    /**
     * Set error handling approach
     */
    private void setOnError(String value) {
        if (value.compareToIgnoreCase("EXCEPTION") == 0) {
            onError = OnError.EXCEPTION;
        } else if (value.compareToIgnoreCase("SETERROR") == 0) {
            onError = OnError.SETERROR;
        }
        if (value.compareToIgnoreCase("STOP") == 0) {
            onError = OnError.STOP;
        }
    }

    /**
     * Set whether convert or not SQL for the specified connection profile
     */
    void setConnectionConvert(String name, String value) {
        boolean convert = false;
        if (value.compareToIgnoreCase(TRUE) == 0 || value.compareToIgnoreCase(YES) == 0) {
            convert = true;
        }
        connConvert.put(name, convert);
    }

    /**
     * Get whether convert or not SQL for the specified connection profile
     */
    boolean getConnectionConvert(String name) {
        Boolean convert = connConvert.get(name);
        if (convert != null) {
            return convert.booleanValue();
        }
        return false;
    }

    /**
     * Load parameters
     */
    public void init() {
    }
}
