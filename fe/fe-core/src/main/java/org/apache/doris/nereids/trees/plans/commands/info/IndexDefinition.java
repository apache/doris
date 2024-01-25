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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;

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

    /**
     * constructor for IndexDefinition
     */
    public IndexDefinition(String name, List<String> cols, String indexTypeName,
            Map<String, String> properties, String comment) {
        this.name = name;
        this.cols = Utils.copyRequiredList(cols);
        this.indexType = IndexType.BITMAP;
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
     * checkColumn
     */
    public void checkColumn(ColumnDefinition column, KeysType keysType,
            boolean enableUniqueKeyMergeOnWrite) throws AnalysisException {
        if (indexType == IndexType.BITMAP || indexType == IndexType.INVERTED
                || indexType == IndexType.BLOOMFILTER || indexType == IndexType.NGRAM_BF) {
            String indexColName = column.getName();
            caseSensitivityCols.add(indexColName);
            DataType colType = column.getType();
            if (indexType == IndexType.INVERTED && colType.isArrayType()) {
                colType = ((ArrayType) colType).getItemType();
            }
            if (!(colType.isDateLikeType() || colType.isDecimalLikeType()
                    || colType.isIntegralType() || colType.isStringLikeType()
                    || colType.isBooleanType())) {
                // TODO add colType.isVariantType() and colType.isAggState()
                throw new AnalysisException(colType + " is not supported in " + indexType.toString()
                        + " index. " + "invalid column: " + indexColName);
            } else if (indexType == IndexType.INVERTED && ((keysType == KeysType.AGG_KEYS
                    && !column.isKey())
                    || (keysType == KeysType.UNIQUE_KEYS && !enableUniqueKeyMergeOnWrite))) {
                throw new AnalysisException(indexType.toString()
                        + " index only used in columns of DUP_KEYS table"
                        + " or UNIQUE_KEYS table with merge_on_write enabled"
                        + " or key columns of AGG_KEYS table. invalid column: " + indexColName);
            } else if (keysType == KeysType.AGG_KEYS && !column.isKey()
                    && indexType != IndexType.INVERTED) {
                throw new AnalysisException(indexType.toString()
                        + " index only used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of"
                        + " AGG_KEYS table. invalid column: " + indexColName);
            }

            if (indexType == IndexType.INVERTED) {
                try {
                    InvertedIndexUtil.checkInvertedIndexParser(indexColName,
                            colType.toCatalogDataType().getPrimitiveType(), properties);
                } catch (Exception ex) {
                    throw new AnalysisException("invalid INVERTED index:" + ex.getMessage(), ex);
                }
            } else if (indexType == IndexType.NGRAM_BF) {
                if (!colType.isStringLikeType()) {
                    throw new AnalysisException(colType + " is not supported in ngram_bf index. "
                            + "invalid column: " + indexColName);
                } else if ((keysType == KeysType.AGG_KEYS && !column.isKey())) {
                    throw new AnalysisException(
                            "ngram_bf index only used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of"
                                    + " AGG_KEYS table. invalid column: " + indexColName);
                }
                if (properties.size() != 2) {
                    throw new AnalysisException(
                            "ngram_bf index should have gram_size and bf_size properties");
                }
                try {
                    int ngramSize = Integer.parseInt(properties.get(IndexDef.NGRAM_SIZE_KEY));
                    int bfSize = Integer.parseInt(properties.get(IndexDef.NGRAM_BF_SIZE_KEY));
                    if (ngramSize > 256 || ngramSize < 1) {
                        throw new AnalysisException(
                                "gram_size should be integer and less than 256");
                    }
                    if (bfSize > 65536 || bfSize < 64) {
                        throw new AnalysisException(
                                "bf_size should be integer and between 64 and 65536");
                    }
                } catch (NumberFormatException e) {
                    throw new AnalysisException("invalid ngram properties:" + e.getMessage(), e);
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
}
