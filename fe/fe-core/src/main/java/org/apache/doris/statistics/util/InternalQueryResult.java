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

package org.apache.doris.statistics.util;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Readable results of internal SQL execution,
 * providing some read operations.
 */
public class InternalQueryResult {
    private final List<ResultRow> resultRows = Lists.newArrayList();

    public InternalQueryResult() {
    }

    public List<ResultRow> getResultRows() {
        return resultRows;
    }

    public static class ResultRow {
        private final List<String> columns;
        private final List<PrimitiveType> types;
        private final List<String> values;

        private final Map<String, Integer> columnNameMap = Maps.newHashMap();
        private final Map<Integer, String> columnIndexMap = Maps.newHashMap();

        public ResultRow(List<String> columns, List<PrimitiveType> types, List<String> values) {
            this.columns = columns;
            this.types = types;
            this.values = values;
            buildColumnNameMap();
            buildColumnIndexMap();
        }

        public List<String> getColumns() {
            return columns != null ? columns : Collections.emptyList();
        }

        public List<PrimitiveType> getTypes() {
            return types != null ? types : Collections.emptyList();
        }

        public List<String> getValues() {
            return values != null ? values : Collections.emptyList();
        }

        private void buildColumnNameMap() {
            List<String> columns = getColumns();
            for (int i = 0; i < columns.size(); i++) {
                columnNameMap.put(columns.get(i), i);
            }
        }

        private void buildColumnIndexMap() {
            List<String> columns = getColumns();
            for (int i = 0; i < columns.size(); i++) {
                columnIndexMap.put(i, columns.get(i));
            }
        }

        public int getColumnIndex(String columnName) {
            return columnNameMap.getOrDefault(columnName, -1);
        }

        public String getColumnName(int index) throws DdlException {
            List<String> columns = getColumns();
            if (columnIndexMap.containsKey(index)) {
                return columnIndexMap.get(index);
            } else {
                throw new DdlException("Index should be between 0 and " + columns.size());
            }
        }

        public PrimitiveType getColumnType(String columnName) throws DdlException {
            List<PrimitiveType> types = getTypes();
            int index = getColumnIndex(columnName);
            if (index == -1) {
                throw new DdlException(String.format("The column name:[%s] does not exist.", columnName));
            }
            return types.get(index);
        }

        public PrimitiveType getColumnType(int index) throws DdlException {
            List<PrimitiveType> types = getTypes();
            if (index >= 0 && index < types.size()) {
                return types.get(index);
            } else {
                throw new DdlException("Index should be between 0 and " + types.size());
            }
        }

        public String getColumnValue(String columnName) throws DdlException {
            int index = getColumnIndex(columnName);
            if (index == -1) {
                throw new DdlException(String.format("The column name:[%s] does not exist.", columnName));
            }
            return values.get(index);
        }

        public String getColumnValueWithDefault(String columnName, String defaultVal) throws DdlException {
            String val = getColumnValue(columnName);
            return val == null ? defaultVal : val;
        }

        public Object getColumnValue(int index) throws DdlException {
            List<String> columns = getColumns();
            if (index >= 0 && index < columns.size()) {
                return values.get(index);
            } else {
                throw new DdlException("Index should be between 0 and " + columns.size());
            }
        }

        public String getString(int index) throws DdlException {
            List<String> columns = getColumns();
            if (index >= 0 && index < columns.size()) {
                return values.get(index);
            }
            throw new DdlException("Index should be between 0 and " + columns.size());
        }

        public int getInt(int index) throws DdlException {
            List<PrimitiveType> types = getTypes();
            if (index >= 0 && index < types.size()) {
                String value = values.get(index);
                PrimitiveType type = types.get(index);
                switch (type) {
                    case BOOLEAN:
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                        return new Integer(value);
                    default:
                        throw new DdlException("Unable to convert field to int: " + value);
                }
            }
            throw new DdlException("Index should be between 0 and " + types.size());
        }

        public long getLong(int index) throws DdlException {
            List<PrimitiveType> types = getTypes();
            if (index >= 0 && index < types.size()) {
                String value = values.get(index);
                PrimitiveType type = types.get(index);
                switch (type) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                        return Long.parseLong(value);
                    default:
                        throw new DdlException("Unable to convert field to long: " + value);
                }
            }
            throw new DdlException("Index should be between 0 and " + types.size());
        }

        public float getFloat(int index) throws DdlException {
            List<PrimitiveType> types = getTypes();
            if (index >= 0 && index < types.size()) {
                String value = values.get(index);
                PrimitiveType type = types.get(index);
                if (type == PrimitiveType.FLOAT) {
                    return Float.parseFloat(value);
                }
                throw new DdlException("Unable to convert field to float: " + value);
            }
            throw new DdlException("Index should be between 0 and " + types.size());
        }

        public double getDouble(int index) throws DdlException {
            List<PrimitiveType> types = getTypes();
            if (index >= 0 && index < types.size()) {
                String value = values.get(index);
                PrimitiveType type = types.get(index);
                if (type == PrimitiveType.DOUBLE) {
                    return Double.parseDouble(value);
                }
                throw new DdlException("Unable to convert field to long: " + value);
            }
            throw new DdlException("Index should be between 0 and " + types.size());
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("ResultRow{ ");
            if (values != null && values.size() > 0) {
                List<String> columns = getColumns();
                for (int i = 0; i < values.size(); i++) {
                    sb.append(columns.get(i));
                    sb.append(":");
                    sb.append(values.get(i));
                    sb.append(" ");
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        if (resultRows.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("InternalQueryResult:\n");
            for (ResultRow resultRow : resultRows) {
                sb.append(" - ");
                sb.append(resultRow.toString());
                sb.append("\n");
            }
            return sb.toString();
        }
        return "InternalQueryResult{" + "resultRows=" + resultRows + '}';
    }
}
