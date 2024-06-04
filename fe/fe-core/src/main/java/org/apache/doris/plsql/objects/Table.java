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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/objects/Table.java
// and modified by Doris

package org.apache.doris.plsql.objects;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.plsql.ColumnDefinition;
import org.apache.doris.plsql.Row;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.executor.QueryResult;

import java.util.HashMap;
import java.util.Map;

/**
 * Oracle's PL/SQL Table/associative array.
 * <p>
 * Tables can be modelled after a corresponding Hive table or they can be created manually.
 * <p>
 * 1. Model the table after the emp Hive table
 * TYPE t_tab IS TABLE OF emp%ROWTYPE INDEX BY BINARY_INTEGER;
 * <p>
 * 2. Model the table after a column of a Hive table (emp.name). This table will hold a single column only.
 * TYPE t_tab IS TABLE OF emp.name%TYPE INDEX BY BINARY_INTEGER;
 * <p>
 * 3. Or you can specify the column manually. This table will hold one column only.
 * TYPE t_tab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
 * <p>
 * In the first case the values will be records where each key in the record matches the columns to the corresponding
 * table.
 * tab(key).col_name;
 * <p>
 * In the last two cases the values will represent scalars, but they stored in a record with a single key.
 * tab(key)
 * <p>
 * Iteration logic uses the first/last next and prior methods.
 * First/last returns a key, next/prior gives back the next or previous key based on the key passed in.
 */
public class Table implements PlObject {
    private final TableClass plClass;
    private final Map<Object, Value> rows = new HashMap<>();
    private Object lastKey = null;
    private Object firstKey = null;

    public Table(TableClass plClass) {
        this.plClass = plClass;
    }

    public void populate(QueryResult query, long rowIndex, int columnIndex) throws AnalysisException {
        if (plClass().rowType()) {
            putRow(rowIndex, query);
        } else {
            putColumn(rowIndex, query, columnIndex);
        }
    }

    public void putRow(Object key, QueryResult result) throws AnalysisException {
        put(key, readRow(result));
    }

    public void putColumn(Object key, QueryResult query, int columnIndex) throws AnalysisException {
        put(key, readColumn(query, columnIndex));
    }

    public void put(Object key, Row row) {
        Value existing = rows.get(key);
        if (existing != null) {
            existing.row = row;
        } else {
            if (lastKey != null) {
                rows.get(lastKey).nextKey = key;
            }
            rows.put(key, new Value(row, lastKey));
            lastKey = key;
            if (firstKey == null) {
                firstKey = key;
            }
        }
    }

    private Row readRow(QueryResult result) throws AnalysisException {
        Row row = new Row();
        int idx = 0;
        for (ColumnDefinition column : plClass.columns()) {
            Var var = new Var(column.columnName(), column.columnType().typeString(), (Integer) null, null, null);
            var.setValue(result, idx);
            row.addColumn(column.columnName(), column.columnTypeString(), var);
            idx++;
        }
        return row;
    }

    private Row readColumn(QueryResult result, int columnIndex) throws AnalysisException {
        Row row = new Row();
        ColumnDefinition column = plClass.columns().get(0);
        Var var = new Var(column.columnName(), column.columnType().typeString(), (Integer) null, null, null);
        var.setValue(result, columnIndex);
        row.addColumn(column.columnName(), column.columnTypeString(), var);
        return row;
    }

    public Row at(Object key) {
        Value value = rows.get(key);
        return value == null ? null : value.row;
    }

    public boolean removeAt(Object key) {
        Value value = rows.remove(key);
        if (value != null) {
            updateLinks(key, value.nextKey, value.prevKey);
        }
        return value != null;
    }

    private void updateLinks(Object deletedKey, Object nextKey, Object prevKey) {
        if (prevKey != null) {
            rows.get(prevKey).nextKey = nextKey;
        }
        if (nextKey != null) {
            rows.get(nextKey).prevKey = prevKey;
        }
        if (deletedKey.equals(firstKey)) {
            firstKey = nextKey;
        }
        if (deletedKey.equals(lastKey)) {
            lastKey = prevKey;
        }
    }

    public void removeFromTo(Object fromKey, Object toKey) {
        Object current = fromKey;
        while (current != null && !current.equals(toKey)) {
            Object next = nextKey(current);
            removeAt(current);
            current = next;
        }
        if (current != null) {
            removeAt(current);
        }
    }

    public void removeAll() {
        lastKey = null;
        firstKey = null;
        rows.clear();
    }

    public Object nextKey(Object key) {
        Value value = rows.get(key);
        return value == null ? null : value.nextKey;
    }

    public Object priorKey(Object key) {
        Value value = rows.get(key);
        return value == null ? null : value.prevKey;
    }

    public Object firstKey() {
        return firstKey;
    }

    public Object lastKey() {
        return lastKey;
    }

    public boolean existsAt(Object key) {
        return rows.get(key) != null;
    }

    public int count() {
        return rows.size();
    }

    @Override
    public TableClass plClass() {
        return plClass;
    }

    private static class Value {
        private Row row;
        private Object prevKey;
        private Object nextKey;

        public Value(Row row, Object prevKey) {
            this.row = row;
            this.prevKey = prevKey;
        }

        public void setPrevKey(Object prevKey) {
            this.prevKey = prevKey;
        }

        public void setNextKey(Object nextKey) {
            this.nextKey = nextKey;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Table{");
        sb.append("plClass=").append(plClass.getClass());
        sb.append(", size=").append(count());
        sb.append(", lastKey=").append(lastKey);
        sb.append(", firstKey=").append(firstKey);
        sb.append('}');
        return sb.toString();
    }
}
