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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.info.PartitionNamesInfo;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class IndexDef {
    private String indexName;
    private boolean ifNotExists;
    private List<String> columns;
    // add the column name of olapTable column into caseSensitivityColumns
    // instead of the column name which from sql_parser analyze
    private List<String> caseSensitivityColumns = Lists.newArrayList();
    private IndexType indexType;
    private String comment;
    private Map<String, String> properties;
    private boolean isBuildDeferred = false;
    private PartitionNamesInfo partitionNamesInfo;
    public static final int MIN_NGRAM_SIZE = 1;
    public static final int MAX_NGRAM_SIZE = 255;
    public static final int MIN_BF_SIZE = 64;
    public static final int MAX_BF_SIZE = 65535;

    public static final String NGRAM_SIZE_KEY = "gram_size";
    public static final String NGRAM_BF_SIZE_KEY = "bf_size";
    public static final String DEFAULT_NGRAM_SIZE = "2";
    public static final String DEFAULT_NGRAM_BF_SIZE = "256";


    public IndexDef(String indexName, boolean ifNotExists, List<String> columns, IndexType indexType,
                    Map<String, String> properties, String comment) {
        this.indexName = indexName;
        this.ifNotExists = ifNotExists;
        this.columns = columns;
        if (indexType == null) {
            this.indexType = IndexType.INVERTED;
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
        if (indexType == IndexType.NGRAM_BF) {
            this.properties.putIfAbsent(NGRAM_SIZE_KEY, DEFAULT_NGRAM_SIZE);
            this.properties.putIfAbsent(NGRAM_BF_SIZE_KEY, DEFAULT_NGRAM_BF_SIZE);
        }
    }

    public IndexDef(String indexName, PartitionNamesInfo partitionNamesInfo,
                        IndexType indexType, boolean isBuildDeferred) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.partitionNamesInfo = partitionNamesInfo;
        this.isBuildDeferred = isBuildDeferred;
    }

    public void analyze() throws AnalysisException {
        if (isBuildDeferred && (indexType == IndexDef.IndexType.INVERTED || indexType == IndexDef.IndexType.ANN)) {
            if (Strings.isNullOrEmpty(indexName)) {
                throw new AnalysisException("index name cannot be blank.");
            }
            if (indexName.length() > 128) {
                throw new AnalysisException("index name too long, the index name length at most is 128.");
            }
            return;
        }

        if (indexType == IndexDef.IndexType.BITMAP
                || indexType == IndexDef.IndexType.INVERTED
                || indexType == IndexDef.IndexType.ANN) {
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
        sb.append("`" + indexName + "`");
        if (tableName != null && !tableName.isEmpty()) {
            sb.append(" ON ").append(tableName);
        }
        if (columns != null && columns.size() > 0) {
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
        }
        if (indexType != null) {
            sb.append(" USING ").append(indexType.toString());
        }
        if (properties != null && properties.size() > 0) {
            sb.append(" PROPERTIES(");
            boolean first = true;
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
            sb.append(" COMMENT \"").append(SqlUtils.escapeQuota(comment)).append("\"");
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
        if (caseSensitivityColumns.size() > 0) {
            return caseSensitivityColumns;
        }
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

    public List<String> getPartitionNamesInfo() {
        return partitionNamesInfo == null ? Lists.newArrayList() : partitionNamesInfo.getPartitionNames();
    }

    public enum IndexType {
        BITMAP,
        INVERTED,
        BLOOMFILTER,
        NGRAM_BF,
        ANN
    }

    public boolean isInvertedIndex() {
        return (this.indexType == IndexType.INVERTED);
    }

    public boolean isAnnIndex() {
        return (this.indexType == IndexType.ANN);
    }
}
