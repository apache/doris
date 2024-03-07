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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Meta.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.plsql.executor.Metadata;
import org.apache.doris.plsql.executor.QueryExecutor;
import org.apache.doris.plsql.executor.QueryResult;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Metadata
 */
public class Meta {

    HashMap<String, HashMap<String, Row>> dataTypes = new HashMap<String, HashMap<String, Row>>();

    Exec exec;
    boolean trace = false;
    boolean info = false;
    private QueryExecutor queryExecutor;

    Meta(Exec e, QueryExecutor queryExecutor) {
        exec = e;
        trace = exec.getTrace();
        info = exec.getInfo();
        this.queryExecutor = queryExecutor;
    }

    /**
     * Get the data type of column (column name is qualified i.e. schema.table.column)
     */
    String getDataType(ParserRuleContext ctx, String conn, String column) {
        String type = null;
        HashMap<String, Row> map = dataTypes.get(conn);
        if (map == null) {
            map = new HashMap<String, Row>();
            dataTypes.put(conn, map);
        }
        ArrayList<String> twoparts = splitIdentifierToTwoParts(column);
        if (twoparts != null) {
            String tab = twoparts.get(0);
            String col = twoparts.get(1).toUpperCase();
            Row row = map.get(tab);
            if (row != null) {
                type = row.getType(col);
            } else {
                row = readColumns(ctx, conn, tab, map);
                if (row != null) {
                    type = row.getType(col);
                }
            }
        }
        return type;
    }

    /**
     * Get data types for all columns of the table
     */
    Row getRowDataType(ParserRuleContext ctx, String conn, String table) {
        HashMap<String, Row> map = dataTypes.get(conn);
        if (map == null) {
            map = new HashMap<String, Row>();
            dataTypes.put(conn, map);
        }
        Row row = map.get(table);
        if (row == null) {
            row = readColumns(ctx, conn, table, map);
        }
        return row;
    }

    /**
     * Get data types for all columns of the SELECT statement
     */
    Row getRowDataTypeForSelect(ParserRuleContext ctx, String conn, String select) {
        Row row = null;
        Conn.Type connType = exec.getConnectionType(conn);
        // Hive does not support ResultSetMetaData on PreparedStatement, and Hive DESCRIBE
        // does not support queries, so we have to execute the query with LIMIT 1
        if (connType == Conn.Type.HIVE) {
            String sql = "SELECT * FROM (" + select + ") t LIMIT 1";
            QueryResult query = queryExecutor.executeQuery(sql, ctx);
            if (!query.error()) {
                try {
                    int cols = query.columnCount();
                    row = new Row();
                    for (int i = 0; i < cols; i++) {
                        String name = query.metadata().columnName(i);
                        if (name.startsWith("t.")) {
                            name = name.substring(2);
                        }
                        row.addColumnDefinition(name, query.metadata().columnTypeName(i));
                    }
                } catch (Exception e) {
                    exec.signal(e);
                }
            } else {
                exec.signal(query.exception());
            }
            query.close();
        } else {
            QueryResult query = queryExecutor.executeQuery(select, ctx);
            if (!query.error()) {
                try {
                    Metadata rm = query.metadata();
                    int cols = rm.columnCount();
                    for (int i = 1; i <= cols; i++) {
                        String col = rm.columnName(i);
                        String typ = rm.columnTypeName(i);
                        if (row == null) {
                            row = new Row();
                        }
                        row.addColumnDefinition(col.toUpperCase(), typ);
                    }
                } catch (Exception e) {
                    exec.signal(e);
                }
            }
            query.close();
        }
        return row;
    }

    /**
     * Read the column data from the database and cache it
     */
    Row readColumns(ParserRuleContext ctx, String conn, String table, HashMap<String, Row> map) {
        Row row = null;
        Conn.Type connType = exec.getConnectionType(conn);
        if (connType == Conn.Type.HIVE) {
            String sql = "DESCRIBE " + table;
            QueryResult query = queryExecutor.executeQuery(sql, ctx);
            if (!query.error()) {
                try {
                    while (query.next()) {
                        String col = query.column(0, String.class);
                        String typ = query.column(1, String.class);
                        if (row == null) {
                            row = new Row();
                        }
                        // Hive DESCRIBE outputs "empty_string NULL" row before partition information
                        if (typ == null) {
                            break;
                        }
                        row.addColumnDefinition(col.toUpperCase(), typ);
                    }
                    map.put(table, row);
                } catch (Exception e) {
                    exec.signal(e);
                }
            } else {
                exec.signal(query.exception());
            }
            query.close();
        } else {
            QueryResult query = queryExecutor.executeQuery("SELECT * FROM " + table, ctx);
            if (!query.error()) {
                try {
                    Metadata rm = query.metadata();
                    int cols = query.columnCount();
                    for (int i = 1; i <= cols; i++) {
                        String col = rm.columnName(i);
                        String typ = rm.columnTypeName(i);
                        if (row == null) {
                            row = new Row();
                        }
                        row.addColumnDefinition(col.toUpperCase(), typ);
                    }
                    map.put(table, row);
                } catch (Exception ignored) {
                    // ignored
                }
            }
            query.close();
        }
        return row;
    }

    /**
     * Normalize identifier for a database object (convert "" [] to `` i.e.)
     */
    public String normalizeObjectIdentifier(String name) {
        ArrayList<String> parts = splitIdentifier(name);
        if (parts != null) {  // more then one part exist
            StringBuilder norm = new StringBuilder();
            int size = parts.size();
            boolean appended = false;
            for (int i = 0; i < size; i++) {
                if (i == size - 2) {   // schema name
                    String schema = getTargetSchemaName(parts.get(i));
                    if (schema != null) {
                        norm.append(schema);
                        appended = true;
                    }
                } else {
                    norm.append(normalizeIdentifierPart(parts.get(i)));
                    appended = true;
                }
                if (i + 1 < parts.size() && appended) {
                    norm.append(".");
                }
            }
            return norm.toString();
        }
        return normalizeIdentifierPart(name);
    }

    /**
     * Get the schema name to be used in the final executed SQL
     */
    String getTargetSchemaName(String name) {
        if (name.equalsIgnoreCase("dbo") || name.equalsIgnoreCase("[dbo]")) {
            return null;
        }
        return normalizeIdentifierPart(name);
    }

    /**
     * Normalize identifier (single part) - convert "" [] to `` i.e.
     */
    public String normalizeIdentifierPart(String name) {
        char start = name.charAt(0);
        char end = name.charAt(name.length() - 1);
        if ((start == '[' && end == ']') || (start == '"' && end == '"')) {
            return '`' + name.substring(1, name.length() - 1) + '`';
        }
        return name;
    }

    /**
     * Split qualified object to 2 parts: schema.tab.col -&gt; schema.tab|col; tab.col -&gt; tab|col
     */
    public ArrayList<String> splitIdentifierToTwoParts(String name) {
        ArrayList<String> parts = splitIdentifier(name);
        ArrayList<String> twoparts = null;
        if (parts != null) {
            StringBuilder id = new StringBuilder();
            int i = 0;
            for (; i < parts.size() - 1; i++) {
                id.append(parts.get(i));
                if (i + 1 < parts.size() - 1) {
                    id.append(".");
                }
            }
            twoparts = new ArrayList<String>();
            twoparts.add(id.toString());
            id.setLength(0);
            id.append(parts.get(i));
            twoparts.add(id.toString());
        }
        return twoparts;
    }

    /**
     * Split identifier to parts (schema, table, colum name etc.)
     *
     * @return null if identifier contains single part
     */
    public ArrayList<String> splitIdentifier(String name) {
        ArrayList<String> parts = null;
        int start = 0;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            char del = '\0';
            if (c == '`' || c == '"') {
                del = c;
            } else if (c == '[') {
                del = ']';
            }
            if (del != '\0') {
                for (int j = i + 1; i < name.length(); j++) {
                    i++;
                    if (name.charAt(j) == del) {
                        break;
                    }
                }
                continue;
            }
            if (c == '.') {
                if (parts == null) {
                    parts = new ArrayList<String>();
                }
                parts.add(name.substring(start, i));
                start = i + 1;
            }
        }
        if (parts != null) {
            parts.add(name.substring(start));
        }
        return parts;
    }
}
