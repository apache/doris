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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class IndexDef {
    private String indexName;
    private boolean ifNotExists;
    private List<String> columns;
    private IndexType indexType;
    private String comment;
    private Map<String, String> properties;

    public IndexDef(String indexName, boolean ifNotExists, List<String> columns, IndexType indexType,
                    Map<String, String> properties, String comment) {
        this.indexName = indexName;
        this.ifNotExists = ifNotExists;
        this.columns = columns;
        if (indexType == null) {
            this.indexType = IndexType.BITMAP;
        } else {
            this.indexType = indexType;
        }
        if (columns == null) {
            this.comment = "";
        } else {
            this.comment = comment;
        }
        if (properties == null) {
            this.properties = new HashMap<>();
        } else {
            this.properties = properties;
        }
    }

    public void analyze() throws AnalysisException {
        if (indexType == IndexDef.IndexType.BITMAP
                || indexType == IndexDef.IndexType.INVERTED) {
            if (columns == null || columns.size() != 1) {
                throw new AnalysisException(indexType.toString() + " index can only apply to a single column.");
            }
            if (Strings.isNullOrEmpty(indexName)) {
                throw new AnalysisException("index name cannot be blank.");
            }
            if (indexName.length() > 64) {
                throw new AnalysisException("index name too long, the index name length at most is 64.");
            }
            TreeSet<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            distinct.addAll(columns);
            if (columns.size() != distinct.size()) {
                throw new AnalysisException("columns of index has duplicated.");
            }
        }
    }

    public String toSql() {
        return toSql(null);
    }

    public String toSql(String tableName) {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(indexName);
        if (tableName != null && !tableName.isEmpty()) {
            sb.append(" ON ").append(tableName);
        }
        sb.append(" (");
        boolean first = true;
        for (String col : columns) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("`" + col + "`");
        }
        sb.append(")");
        if (indexType != null) {
            sb.append(" USING ").append(indexType.toString());
        }
        if (properties != null && properties.size() > 0) {
            sb.append(" PROPERTIES(");
            first = true;
            for (Map.Entry<String, String> e : properties.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append("\"").append(e.getKey()).append("\"=").append("\"").append(e.getValue()).append("\"");
            }
            sb.append(")");
        }
        if (comment != null) {
            sb.append(" COMMENT '" + comment + "'");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String getIndexName() {
        return indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public enum IndexType {
        BITMAP,
        INVERTED,
        BLOOMFILTER,
    }

    public boolean isInvertedIndex() {
        return (this.indexType == IndexType.INVERTED);
    }

    public void checkColumn(Column column, KeysType keysType) throws AnalysisException {
        if (indexType == IndexType.BITMAP || indexType == IndexType.INVERTED || indexType == IndexType.BLOOMFILTER) {
            String indexColName = column.getName();
            PrimitiveType colType = column.getDataType();
            if (!(colType.isDateType() || colType.isDecimalV2Type() || colType.isDecimalV3Type()
                    || colType.isFixedPointType() || colType.isStringType() || colType == PrimitiveType.BOOLEAN)) {
                throw new AnalysisException(colType + " is not supported in " + indexType.toString() + " index. "
                        + "invalid column: " + indexColName);
            } else if ((keysType == KeysType.AGG_KEYS && !column.isKey())) {
                throw new AnalysisException(indexType.toString()
                        + " index only used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of"
                                + " AGG_KEYS table. invalid column: " + indexColName);
            }

            if (indexType == IndexType.INVERTED) {
                InvertedIndexUtil.checkInvertedIndexParser(indexColName, colType, properties);
            }
        } else {
            throw new AnalysisException("Unsupported index type: " + indexType);
        }
    }

    public void checkColumns(List<Column> columns, KeysType keysType) throws AnalysisException {
        if (indexType == IndexType.BITMAP || indexType == IndexType.INVERTED || indexType == IndexType.BLOOMFILTER) {
            for (Column col : columns) {
                checkColumn(col, keysType);
            }
        }
    }
}
