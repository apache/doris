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

package org.apache.doris.catalog;

import org.apache.doris.analysis.InvertedIndexProperties;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.foundation.util.BasicPrintableMap;
import org.apache.doris.persist.gson.GsonUtilsCatalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Internal representation of index, including index type, name, columns and comments.
 * This class will be used in olap table
 */
public class Index implements Writable {
    public static final int INDEX_ID_INIT_VALUE = -1;

    @SerializedName(value = "i", alternate = {"indexId"})
    private long indexId = -1; // -1 for compatiable
    @SerializedName(value = "in", alternate = {"indexName"})
    private String indexName;
    @SerializedName(value = "c", alternate = {"columns"})
    private List<String> columns;
    @SerializedName(value = "it", alternate = {"indexType"})
    private IndexType indexType;
    @SerializedName(value = "pt", alternate = {"properties"})
    private Map<String, String> properties;
    @SerializedName(value = "ct", alternate = {"comment"})
    private String comment;

    public Index(long indexId, String indexName, List<String> columns,
            IndexType indexType, Map<String, String> properties, String comment) {
        this.indexId = indexId;
        this.indexName = indexName;
        this.columns = columns == null ? Lists.newArrayList() : Lists.newArrayList(columns);
        this.indexType = indexType;
        this.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
        this.comment = comment;
        if (indexType == IndexType.INVERTED) {
            if (this.properties != null && !this.properties.isEmpty()) {
                String supportPhraseKey = InvertedIndexProperties.INVERTED_INDEX_SUPPORT_PHRASE_KEY;
                if (isTokenizedInvertedIndex(this.properties)) {
                    if (!this.properties.containsKey(supportPhraseKey)) {
                        this.properties.put(supportPhraseKey, "true");
                    }
                } else {
                    // No analyzer on either side, so the query string is never split
                    // either: InvertedIndexAnalyzer::get_analyse_result returns the
                    // whole search string as ONE term, and every phrase variant takes
                    // its terms from there. A single-term phrase is a term query, so
                    // no query against this index can observe a position. Drop the
                    // option instead of carrying it down to the BE, where it would ask
                    // for position data nobody can read and make the index look
                    // scoreable to IndexReaderHelper::is_need_similarity_score.
                    this.properties.remove(supportPhraseKey);
                }
                if (this.properties.containsKey(InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY)
                        || this.properties.containsKey(InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY_ALIAS)) {
                    String lowerCaseKey = InvertedIndexProperties.INVERTED_INDEX_PARSER_LOWERCASE_KEY;
                    if (!this.properties.containsKey(lowerCaseKey)) {
                        this.properties.put(lowerCaseKey, "true");
                    }
                }
            }
        }
    }

    // True when the BE will run an analyzer over this index, i.e. when it serves the
    // column with a FULLTEXT reader rather than the keyword lane. This has to mirror
    // InvertedIndexAnalyzer::should_analyzer EXACTLY, so it is composed from the same
    // two resolvers the BE uses rather than re-reading the keys by hand:
    //
    //   getPreferredAnalyzer   <-> get_analyzer_name_from_properties
    //                              (note both fall back to the NORMALIZER key)
    //   getInvertedIndexParser <-> get_parser_string_from_properties
    //                              (both default to "none", both accept the
    //                               "built_in_analyzer" alias)
    //
    // A normalizer therefore counts as analyzed even though it emits a single token:
    // the BE gives such an index a FULLTEXT reader, and match.cpp rejects
    // MATCH_PHRASE outright on a FULLTEXT-served index whose support_phrase is
    // absent. Dropping the key there would turn a working (degenerate, single-term)
    // phrase query into a hard error.
    private static boolean isTokenizedInvertedIndex(Map<String, String> properties) {
        if (!InvertedIndexProperties.getPreferredAnalyzer(properties).isEmpty()) {
            return true;
        }
        return !InvertedIndexProperties.INVERTED_INDEX_PARSER_NONE
                .equalsIgnoreCase(InvertedIndexProperties.getInvertedIndexParser(properties));
    }

    public Index() {
        this.indexName = null;
        this.columns = null;
        this.indexType = null;
        this.properties = null;
        this.comment = null;
    }

    public long getIndexId() {
        return indexId;
    }

    public void setIndexId(long indexId) {
        this.indexId = indexId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getPropertiesString() {
        if (properties == null || properties.isEmpty()) {
            return "";
        }

        // Use TreeMap to ensure consistent ordering of properties
        return "(" + new BasicPrintableMap<>(new java.util.TreeMap<>(properties), "=", true, false, ",").toString()
                + ")";
    }

    public String getInvertedIndexParser() {
        return InvertedIndexProperties.getInvertedIndexParser(properties);
    }

    public boolean isInvertedIndexParserNone() {
        return InvertedIndexProperties.INVERTED_INDEX_PARSER_NONE.equals(getInvertedIndexParser());
    }

    public String getInvertedIndexFieldPattern() {
        return InvertedIndexProperties.getInvertedIndexFieldPattern(properties);
    }

    // Whether the index can be changed in light mode
    public boolean isLightIndexChangeSupported() {
        return indexType == IndexType.INVERTED
                || indexType == IndexType.NGRAM_BF
                || indexType == IndexType.BLOOMFILTER
                || indexType == IndexType.ANN;
    }

    // Whether the index can be added in light mode
    // cloud mode supports light add for bf index, ngram_bf index and non-tokenized inverted index (parser="none")
    // local mode supports light add for bf index, inverted index, ann index and ngram_bf index
    // the rest of the index types do not support light add
    public boolean isLightAddIndexSupported(boolean enableAddIndexForNewData) {
        if (Config.isCloudMode()) {
            if (indexType == IndexType.INVERTED) {
                return isInvertedIndexParserNone() && enableAddIndexForNewData;
            } else if (indexType == IndexType.NGRAM_BF || indexType == IndexType.BLOOMFILTER) {
                return enableAddIndexForNewData;
            }
            return false;
        }
        return ((indexType == IndexType.NGRAM_BF || indexType == IndexType.BLOOMFILTER)
                && enableAddIndexForNewData)
                || (indexType == IndexType.INVERTED) || (indexType == IndexType.ANN);
    }

    public String getComment() {
        return getComment(false);
    }

    public String getComment(boolean escapeQuota) {
        if (!escapeQuota) {
            return comment;
        }
        return SqlUtils.escapeQuota(comment);
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtilsCatalog.GSON.toJson(this));
    }

    public static Index read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtilsCatalog.GSON.fromJson(json, Index.class);
    }

    @Override
    public int hashCode() {
        return 31 * (indexName.hashCode() + columns.hashCode() + indexType.hashCode());
    }

    public Index clone() {
        return new Index(indexId, indexName, new ArrayList<>(columns),
                indexType, new HashMap<>(properties), comment);
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(indexName);
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
            sb.append(" PROPERTIES");
            sb.append(getPropertiesString());
        }
        if (StringUtils.isNotBlank(comment)) {
            sb.append(" COMMENT \"").append(getComment(true)).append("\"");
        }
        return sb.toString();
    }

    public List<Integer> getColumnUniqueIds(List<Column> schema) {
        List<Integer> columnUniqueIds = new ArrayList<>();
        if (schema != null) {
            for (String columnName : columns) {
                for (Column column : schema) {
                    // Remove shadow prefix when comparing to handle schema change scenarios
                    if (columnName.equalsIgnoreCase(
                            Column.removeNamePrefix(column.getName()))) {
                        columnUniqueIds.add(column.getUniqueId());
                    }
                }
            }
        }
        return columnUniqueIds;
    }

    public static Set<String> getBfIndexColumns(Collection<Index> indexes) {
        Set<String> bfIndexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (indexes == null) {
            return bfIndexColumns;
        }
        for (Index index : indexes) {
            if (index.getIndexType() != IndexType.BLOOMFILTER) {
                continue;
            }
            bfIndexColumns.addAll(index.getColumns());
        }
        return bfIndexColumns;
    }

    public static void checkConflict(Collection<Index> indices, Set<String> bfColumns) throws AnalysisException {
        indices = indices == null ? Collections.emptyList() : indices;
        bfColumns = bfColumns == null ? Collections.emptySet() : bfColumns;
        Set<String> bfIndexColumns = new HashSet<>();
        for (Index index : indices) {
            if (IndexType.NGRAM_BF == index.getIndexType()
                    || IndexType.BLOOMFILTER == index.getIndexType()) {
                for (String column : index.getColumns()) {
                    column = column.toLowerCase();
                    if (bfIndexColumns.contains(column)) {
                        throw new AnalysisException(column + " should have only one ngram bloom filter index or bloom "
                                + "filter index");
                    }
                    bfIndexColumns.add(column);
                }
            }
        }
        for (String column : bfColumns) {
            column = column.toLowerCase();
            if (bfIndexColumns.contains(column)) {
                throw new AnalysisException(column + " should have only one ngram bloom filter index or bloom "
                        + "filter index");
            }
            bfIndexColumns.add(column);
        }
    }

    /**
     * Returns whether this index is an analyzed inverted index,
     * i.e. an inverted index with parser/analyzer/normalizer properties.
     */
    public boolean isAnalyzedInvertedIndex() {
        return indexType == IndexType.INVERTED
                && properties != null
                && (properties.containsKey(InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY)
                || properties.containsKey(InvertedIndexProperties.INVERTED_INDEX_PARSER_KEY_ALIAS)
                || properties.containsKey(InvertedIndexProperties.INVERTED_INDEX_ANALYZER_NAME_KEY)
                || properties.containsKey(InvertedIndexProperties.INVERTED_INDEX_NORMALIZER_NAME_KEY));
    }
}
