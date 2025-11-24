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

import org.apache.doris.analysis.AnnIndexPropertiesChecker;
import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * index definition
 */
public class IndexDefinition {
    public static final int MIN_NGRAM_SIZE = 1;
    public static final int MAX_NGRAM_SIZE = 255;
    public static final int MIN_BF_SIZE = 64;
    public static final int MAX_BF_SIZE = 65535;

    public static final String NGRAM_SIZE_KEY = "gram_size";
    public static final String NGRAM_BF_SIZE_KEY = "bf_size";
    public static final String DEFAULT_NGRAM_SIZE = "2";
    public static final String DEFAULT_NGRAM_BF_SIZE = "256";
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
     * IndexType
     */
    public enum IndexType {
        BITMAP,
        INVERTED,
        BLOOMFILTER,
        NGRAM_BF,
        ANN
    }

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
                case "ANN": {
                    this.indexType = IndexType.ANN;
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
            this.properties.putIfAbsent(NGRAM_SIZE_KEY, DEFAULT_NGRAM_SIZE);
            this.properties.putIfAbsent(NGRAM_BF_SIZE_KEY, DEFAULT_NGRAM_BF_SIZE);
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
        if (indexType == IndexType.ANN) {
            if (column.isNullable()) {
                throw new AnalysisException("ANN index must be built on a column that is not nullable");
            }
            String indexColName = column.getName();
            caseSensitivityCols.add(indexColName);
            DataType colType = column.getType();
            if (!colType.isArrayType()) {
                throw new AnalysisException("ANN index column must be array type, invalid index: " + name);
            }
            DataType itemType = ((ArrayType) colType).getItemType();
            if (!itemType.isFloatType()) {
                throw new AnalysisException("ANN index column item type must be float type, invalid index: " + name);
            }
            if (keysType != KeysType.DUP_KEYS) {
                throw new AnalysisException("ANN index can only be used in DUP_KEYS table");
            }
            return;
        }

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
                               && (properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY)
                                   || properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY_ALIAS)
                                   || properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_CUSTOM_ANALYZER_KEY))) {
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
                    parseAndValidateProperty(properties, NGRAM_SIZE_KEY, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);
                    parseAndValidateProperty(properties, NGRAM_BF_SIZE_KEY, MIN_BF_SIZE, MAX_BF_SIZE);
                } catch (Exception ex) {
                    throw new AnalysisException("invalid ngram bf index params:" + ex.getMessage(), ex);
                }
            }
        } else {
            throw new AnalysisException("Unsupported index type: " + indexType);
        }
    }

    /**
     * checkColumn
     */
    public void checkColumn(Column column, KeysType keysType, boolean enableUniqueKeyMergeOnWrite,
                            TInvertedIndexFileStorageFormat invertedIndexFileStorageFormat) throws AnalysisException {
        if (indexType == IndexType.ANN) {
            if (column.isAllowNull()) {
                throw new AnalysisException("ANN index must be built on a column that is not nullable");
            }

            String indexColName = column.getName();
            caseSensitivityCols.add(indexColName);
            PrimitiveType primitiveType = column.getDataType();
            if (!primitiveType.isArrayType()) {
                throw new AnalysisException("ANN index column must be array type");
            }
            Type columnType = column.getType();
            Type itemType = ((org.apache.doris.catalog.ArrayType) columnType).getItemType();
            if (!itemType.isFloatingPointType()) {
                throw new AnalysisException("ANN index column item type must be float type");
            }
            if (keysType != KeysType.DUP_KEYS) {
                throw new AnalysisException("ANN index can only be used in DUP_KEYS table");
            }
            if (invertedIndexFileStorageFormat == TInvertedIndexFileStorageFormat.V1) {
                throw new AnalysisException("ANN index is not supported in index format V1");
            }
            return;
        }

        if (indexType == IndexType.BITMAP || indexType == IndexType.INVERTED || indexType == IndexType.BLOOMFILTER
                || indexType == IndexType.NGRAM_BF) {
            String indexColName = column.getName();
            caseSensitivityCols.add(indexColName);
            PrimitiveType colType = column.getDataType();
            Type columnType = column.getType();
            if (!isSupportIdxType(DataType.fromCatalogType(columnType))) {
                throw new AnalysisException(colType + " is not supported in " + indexType.toString() + " index. "
                    + "invalid index: " + name);
            }

            if (indexType == IndexType.ANN && !colType.isArrayType()) {
                throw new AnalysisException("ANN index column must be array type");
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
                    InvertedIndexUtil.checkInvertedIndexParser(indexColName, colType, properties,
                            invertedIndexFileStorageFormat);
                } catch (Exception e) {
                    throw new AnalysisException("invalid INVERTED index:" + e.getMessage(), e);
                }
            } else if (indexType == IndexType.NGRAM_BF) {
                if (colType != PrimitiveType.CHAR && colType != PrimitiveType.VARCHAR
                        && colType != PrimitiveType.STRING) {
                    throw new AnalysisException(colType + " is not supported in ngram_bf index. "
                        + "invalid column: " + indexColName);
                }
                if (properties.size() != 2) {
                    throw new AnalysisException("ngram_bf index should have gram_size and bf_size properties");
                }

                parseAndValidateProperty(properties, NGRAM_SIZE_KEY, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);
                parseAndValidateProperty(properties, NGRAM_BF_SIZE_KEY, MIN_BF_SIZE, MAX_BF_SIZE);
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
        if (isBuildDeferred && indexType == IndexType.INVERTED) {
            if (Strings.isNullOrEmpty(name)) {
                throw new AnalysisException("index name cannot be blank.");
            }
            if (name.length() > 128) {
                throw new AnalysisException(
                        "index name too long, the index name length at most is 128.");
            }
            return;
        }

        if (indexType == IndexType.ANN) {
            AnnIndexPropertiesChecker.checkProperties(this.properties);
        }

        if (indexType == IndexType.BITMAP || indexType == IndexType.INVERTED) {
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
            return caseSensitivityCols;
        }
        return cols;
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

    public boolean isSetIfNotExists() {
        return ifNotExists;
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

    public boolean isAnnIndex() {
        return (this.indexType == IndexType.ANN);
    }

    public String getComment() {
        return comment;
    }

    private void parseAndValidateProperty(Map<String, String> properties, String key, int minValue, int maxValue)
            throws AnalysisException {
        String valueStr = properties.get(key);
        if (valueStr == null) {
            throw new AnalysisException("Property '" + key + "' is missing.");
        }
        try {
            int value = Integer.parseInt(valueStr);
            if (value < minValue || value > maxValue) {
                throw new AnalysisException("'" + key + "' should be an integer between "
                    + minValue + " and " + maxValue + ".");
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid value for '" + key + "': " + valueStr, e);
        }
    }

    public boolean isAnalyzedInvertedIndex() {
        return indexType == IndexType.INVERTED
                && properties != null
                        && (properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY)
                            || properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY_ALIAS)
                            || properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_CUSTOM_ANALYZER_KEY));
    }
}
