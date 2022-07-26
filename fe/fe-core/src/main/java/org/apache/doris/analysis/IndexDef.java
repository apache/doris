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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

public class IndexDef {
    private String indexName;
    private boolean ifNotExists;
    private List<String> columns;
    private IndexType indexType;
    private List<Expr> arguments;
    private String comment;

    private static final Expr DEFAULT_NGRAM_SIZE = new IntLiteral(2);
    private static final Expr DEFAULT_NGRAM_BF_SIZE = new IntLiteral(256);

    public IndexDef(String indexName, boolean ifNotExists, List<String> columns,
                    IndexTypeWithArgument indexTypeWithArguments, String comment
    ) {
        this(indexName, ifNotExists, columns, indexTypeWithArguments == null ? null : indexTypeWithArguments.getType(),
                indexTypeWithArguments == null ? new ArrayList<>() : indexTypeWithArguments.getArguments(), comment
        );
    }

    public IndexDef(String indexName, boolean ifNotExists, List<String> columns, IndexType indexType, List<Expr> args,
                    String comment) {
        this.indexName = indexName;
        this.ifNotExists = ifNotExists;
        this.columns = columns;
        if (args == null) {
            this.arguments = new ArrayList<>();
        } else {
            this.arguments = args;
        }
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
        if (this.indexType == IndexType.NGRAM_BF) {
            if (this.arguments.isEmpty()) {
                this.arguments = Arrays.asList(DEFAULT_NGRAM_SIZE, DEFAULT_NGRAM_BF_SIZE);
            } else if (this.arguments.size() == 1) {
                this.arguments = Arrays.asList(this.arguments.get(0), DEFAULT_NGRAM_BF_SIZE);
            }
        }
    }

    public void analyze() throws AnalysisException {
        if (indexType == IndexDef.IndexType.BITMAP) {
            if (columns == null || columns.size() != 1) {
                throw new AnalysisException("bitmap index can only apply to a single column.");
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
            if (arguments != null && !arguments.isEmpty()) {
                throw new AnalysisException("bimap index do not need arguments.");
            }
        } else if (indexType == IndexType.NGRAM_BF) {
            if (columns == null || columns.size() != 1) {
                throw new AnalysisException("ngram index can only apply to a single column.");
            }
            if (Strings.isNullOrEmpty(indexName)) {
                throw new AnalysisException("index name cannot be blank.");
            }
            if (indexName.length() > 64) {
                throw new AnalysisException("index name too long, the index name length at most is 64.");
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
        if (arguments != null && !arguments.isEmpty()) {
            sb.append(" (");
            first = true;
            for (Expr argument : arguments) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(argument.toSql());
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

    public List<Expr> getArguments() {
        return arguments;
    }

    public String getComment() {
        return comment;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public enum IndexType {
        BITMAP,
        NGRAM_BF
    }

    public static class IndexTypeWithArgument {
        private final IndexType type;
        private final List<Expr> arguments;

        public IndexTypeWithArgument(IndexType type, List<Expr> arguments) {
            this.type = type;
            this.arguments = arguments;
        }

        public IndexType getType() {
            return type;
        }

        public List<Expr> getArguments() {
            return arguments;
        }
    }

    public void checkColumn(Column column, KeysType keysType) throws AnalysisException {
        if (indexType == IndexType.BITMAP) {
            String indexColName = column.getName();
            PrimitiveType colType = column.getDataType();
            if (!(colType.isDateType() || colType.isDecimalV2Type() || colType.isDecimalV3Type()
                    || colType.isFixedPointType() || colType.isStringType() || colType == PrimitiveType.BOOLEAN)) {
                throw new AnalysisException(colType + " is not supported in bitmap index. "
                        + "invalid column: " + indexColName);
            } else if ((keysType == KeysType.AGG_KEYS && !column.isKey())) {
                throw new AnalysisException(
                        "BITMAP index only used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of"
                                + " AGG_KEYS table. invalid column: " + indexColName);
            }
        } else if (indexType == IndexType.NGRAM_BF) {
            String indexColName = column.getName();
            PrimitiveType colType = column.getDataType();
            if (colType != PrimitiveType.CHAR && colType != PrimitiveType.VARCHAR) {
                throw new AnalysisException(colType + " is not supported in ngram_bf index. "
                        + "invalid column: " + indexColName);
            } else if ((keysType == KeysType.AGG_KEYS && !column.isKey())) {
                throw new AnalysisException(
                        "ngram_bf index only used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of"
                                + " AGG_KEYS table. invalid column: " + indexColName);
            }
            if (arguments == null || arguments.size() != 2) {
                throw new AnalysisException("ngram should have ngram size and bloom filter size arguments");
            }
            Expr ngramSize = arguments.get(0);
            if (!(ngramSize instanceof IntLiteral && ((IntLiteral) ngramSize).getLongValue() < 256
                    && ((IntLiteral) ngramSize).getLongValue() >= 1)) {
                throw new AnalysisException("ngram size should be integer and less than 256");
            }
            Expr bfSize = arguments.get(1);
            if (!(bfSize instanceof IntLiteral && ((IntLiteral) bfSize).getLongValue() < 65536
                    && ((IntLiteral) bfSize).getLongValue() >= 64)) {
                throw new AnalysisException("bloom filter size should be integer and between 64 and 65536");
            }
        } else {
            throw new AnalysisException("Unsupported index type: " + indexType);
        }
    }

    public void checkColumns(List<Column> columns, KeysType keysType) throws AnalysisException {
        if (indexType == IndexType.BITMAP) {
            for (Column col : columns) {
                checkColumn(col, keysType);
            }
        }
    }
}
