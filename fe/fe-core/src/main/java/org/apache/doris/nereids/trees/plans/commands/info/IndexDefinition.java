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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * index definition
 */
public class IndexDefinition {
    private final String name;
    private final List<String> cols;
    private final String comment;
    // add the column name of olapTable column into caseSensitivityColumns
    // instead of the column name which from DorisParser
    private List<String> caseSensitivityCols = Lists.newArrayList();
    private IndexType indexType;
    private Map<String, String> properties = new HashMap<>();
    private boolean isBuildDeferred = false;

    private boolean ifNotExists = false;

    private PartitionNamesInfo partitionNames;

    /**
     * constructor for IndexDefinition
     */
    public IndexDefinition(String name, boolean ifNotExists, List<String> cols, String indexTypeName,
            Map<String, String> properties, String comment) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.cols = Utils.copyRequiredList(cols);
        this.indexType = IndexType.INVERTED;
        if (indexTypeName != null) {
            switch (indexTypeName) {
                case "BITMAP": {
                    this.indexType = IndexType.BITMAP;
                    break;
                }
                case "INVERTED": {
                    this.indexType = IndexType.INVERTED;
                    break;
                }
                case "NGRAM_BF": {
                    this.indexType = IndexType.NGRAM_BF;
                    break;
                }
                default:
                    throw new AnalysisException("unknown index type " + indexTypeName);
            }
        }

        if (properties != null) {
            this.properties.putAll(properties);
        }

        if (indexType == IndexType.NGRAM_BF) {
            this.properties.putIfAbsent(IndexDef.NGRAM_SIZE_KEY, IndexDef.DEFAULT_NGRAM_SIZE);
            this.properties.putIfAbsent(IndexDef.NGRAM_BF_SIZE_KEY, IndexDef.DEFAULT_NGRAM_BF_SIZE);
        }

        this.comment = comment;
    }

    /**
     * constructor for build index
     */
    public IndexDefinition(String name, PartitionNamesInfo partitionNames, IndexType indexType) {
        this.name = name;
        this.indexType = indexType;
        this.partitionNames = partitionNames;
        this.isBuildDeferred = true;
        this.cols = null;
        this.comment = null;
    }

    /**
     * Check if the column type is supported for inverted index
     */
    public static boolean isSupportIdxType(DataType columnType) {
        if (columnType.isArrayType()) {
            DataType itemType = ((ArrayType) columnType).getItemType();
            if (itemType.isArrayType()) {
                return false;
            }
            return isSupportIdxType(itemType);
        }
        return columnType.isDateLikeType() || columnType.isDecimalLikeType()
                || columnType.isIntegralType() || columnType.isStringLikeType()
                || columnType.isBooleanType() || columnType.isVariantType()
                || columnType.isIPType() || columnType.isFloatLikeType();
    }

    /**
     * checkColumn
     */
    public void checkColumn(ColumnDefinition column, KeysType keysType,
            boolean enableUniqueKeyMergeOnWrite,
            TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat) throws AnalysisException {
        if (indexType == IndexType.BITMAP || indexType == IndexType.INVERTED
                || indexType == IndexType.BLOOMFILTER || indexType == IndexType.NGRAM_BF) {
            String indexColName = column.getName();
            caseSensitivityCols.add(indexColName);
            DataType colType = column.getType();
            if (!isSupportIdxType(colType)) {
                // TODO add colType.isAggState()
                throw new AnalysisException(colType + " is not supported in " + indexType.toString()
                        + " index. " + "invalid index: " + name);
            }

            // In inverted index format v1, each subcolumn of a variant has its own index file, leading to high IOPS.
            // when the subcolumn type changes, it may result in missing files, causing link file failure.
            // There are two cases in which the inverted index format v1 is not supported:
            // 1. in cloud mode
            // 2. enable_inverted_index_v1_for_variant = false
            boolean notSupportInvertedIndexForVariant =
                    (invertedIndexFileStorageFormat == TInvertedIndexFileStorageFormat.V1
                        || invertedIndexFileStorageFormat == TInvertedIndexFileStorageFormat.DEFAULT)
                            && (Config.isCloudMode() || !Config.enable_inverted_index_v1_for_variant);

            if (colType.isVariantType() && notSupportInvertedIndexForVariant) {
                throw new AnalysisException(colType + " is not supported in inverted index format V1,"
                        + "Please set properties(\"inverted_index_storage_format\"= \"v2\"),"
                        + "or upgrade to a newer version");
            }
            if (!column.isKey()) {
                if (keysType == KeysType.AGG_KEYS) {
                    throw new AnalysisException("index should only be used in columns of DUP_KEYS/UNIQUE_KEYS table"
                        + " or key columns of AGG_KEYS table. invalid index: " + name);
                } else if (keysType == KeysType.UNIQUE_KEYS && !enableUniqueKeyMergeOnWrite
                               && indexType == IndexType.INVERTED && properties != null
                               && properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY)) {
                    throw new AnalysisException("INVERTED index with parser can NOT be used in value columns of"
                        + " UNIQUE_KEYS table with merge_on_write disable. invalid index: " + name);
                }
            }

            if (indexType == IndexType.INVERTED) {
                try {
                    InvertedIndexUtil.checkInvertedIndexParser(indexColName,
                            colType.toCatalogDataType().getPrimitiveType(), properties,
                            invertedIndexFileStorageFormat);
                } catch (Exception ex) {
                    throw new AnalysisException("invalid INVERTED index:" + ex.getMessage(), ex);
                }
            } else if (indexType == IndexType.NGRAM_BF) {
                if (!colType.isStringLikeType()) {
                    throw new AnalysisException(colType + " is not supported in ngram_bf index. "
                            + "invalid column: " + indexColName);
                }
                if (properties.size() != 2) {
                    throw new AnalysisException(
                            "ngram_bf index should have gram_size and bf_size properties");
                }
                try {
                    IndexDef.parseAndValidateProperty(properties, IndexDef.NGRAM_SIZE_KEY, IndexDef.MIN_NGRAM_SIZE,
                                                      IndexDef.MAX_NGRAM_SIZE);
                    IndexDef.parseAndValidateProperty(properties, IndexDef.NGRAM_BF_SIZE_KEY, IndexDef.MIN_BF_SIZE,
                                                      IndexDef.MAX_BF_SIZE);
                } catch (Exception ex) {
                    throw new AnalysisException("invalid ngram bf index params:" + ex.getMessage(), ex);
                }
            }
        } else {
            throw new AnalysisException("Unsupported index type: " + indexType);
        }
    }

    /**
     * validate
     */
    public void validate() {
        if (partitionNames != null) {
            partitionNames.validate();
        }
        if (isBuildDeferred && indexType == IndexDef.IndexType.INVERTED) {
            if (Strings.isNullOrEmpty(name)) {
                throw new AnalysisException("index name cannot be blank.");
            }
            if (name.length() > 128) {
                throw new AnalysisException(
                        "index name too long, the index name length at most is 128.");
            }
            return;
        }

        if (indexType == IndexDef.IndexType.BITMAP || indexType == IndexDef.IndexType.INVERTED) {
            if (cols == null || cols.size() != 1) {
                throw new AnalysisException(
                        indexType.toString() + " index can only apply to a single column.");
            }
            if (Strings.isNullOrEmpty(name)) {
                throw new AnalysisException("index name cannot be blank.");
            }
            if (name.length() > 64) {
                throw new AnalysisException(
                        "index name too long, the index name length at most is 64.");
            }
            TreeSet<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            distinct.addAll(cols);
            if (cols.size() != distinct.size()) {
                throw new AnalysisException("columns of index has duplicated.");
            }
        }
    }

    public List<String> getColumnNames() {
        if (!caseSensitivityCols.isEmpty()) {
            return ImmutableList.copyOf(caseSensitivityCols);
        } else {
            return ImmutableList.copyOf(cols);
        }
    }

    public String getIndexName() {
        return name;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public Index translateToCatalogStyle() {
        return new Index(Env.getCurrentEnv().getNextId(), name, cols, indexType, properties,
                comment);
    }

    public List<String> getPartitionNames() {
        return partitionNames == null ? Lists.newArrayList() : partitionNames.getPartitionNames();
    }

    /**
     * translateToLegacyIndexDef
     */
    public IndexDef translateToLegacyIndexDef() {
        if (isBuildDeferred) {
            return new IndexDef(name, partitionNames != null ? partitionNames.translateToLegacyPartitionNames() : null,
                    indexType, true);
        } else {
            return new IndexDef(name, ifNotExists, cols, indexType, properties, comment);
        }
    }

    public String toSql() {
        return toSql(null);
    }

    /**
     * toSql
     */
    public String toSql(String tableName) {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(name);
        if (tableName != null && !tableName.isEmpty()) {
            sb.append(" ON ").append(tableName);
        }
        if (cols != null && cols.size() > 0) {
            sb.append(" (");
            boolean first = true;
            for (String col : cols) {
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
            sb.append(" COMMENT '" + comment + "'");
        }
        return sb.toString();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isAnalyzedInvertedIndex() {
        return indexType == IndexType.INVERTED
                && properties != null
                        && (properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY)
                            || properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_CUSTOM_ANALYZER_KEY));
    }
}
