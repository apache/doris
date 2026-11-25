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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@code vector_search} or {@code full_text_search} over a Lance table, as the engine scans it: the table pinned to
 * the version the search reads, the search request, and the columns the engine reads after merging the candidates.
 *
 * <p>Created by {@link LanceConnectorMetadata#prepareSearch}; the scan plans one range per index segment the search
 * can use and, for a vector search, one per fragment no such segment covers ({@link LanceScanPlanner#planSearch}).</p>
 */
final class LanceSearchTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;

    private final LanceTableHandle table;
    private final TExternalSearchRequest request;
    private final int searchFieldId;
    private final List<String> lazyColumns;

    /**
     * @param table the searched table, pinned to a version
     * @param request the logical search, with the whole search's {@code top_k} and {@code offset}
     * @param searchFieldId the Lance field id of the searched column; -1 when the search reads no index
     */
    LanceSearchTableHandle(LanceTableHandle table, TExternalSearchRequest request, int searchFieldId) {
        this(table, request, searchFieldId, Collections.emptyList());
    }

    private LanceSearchTableHandle(LanceTableHandle table, TExternalSearchRequest request, int searchFieldId,
            List<String> lazyColumns) {
        if (!table.getVersion().isPresent()) {
            throw new IllegalArgumentException("A Lance search reads a pinned version, not " + table);
        }
        if (request.getSearchQuery().isSetVectorSearch() == request.getSearchQuery().isSetFullTextSearch()) {
            throw new IllegalArgumentException("A Lance search request sets exactly one kind of search");
        }
        if (request.getSearchQuery().isSetVectorSearch()) {
            TVectorSearchParams vector = request.getSearchQuery().getVectorSearch();
            if (!vector.isSetMetric() || vector.getMetric() == TVectorMetric.DEFAULT) {
                throw new IllegalArgumentException("A Lance vector search request names its metric");
            }
        }
        this.table = table;
        this.request = request.deepCopy();
        this.searchFieldId = searchFieldId;
        this.lazyColumns = Collections.unmodifiableList(new ArrayList<>(lazyColumns));
    }

    /** The same search, with the columns Top-N lazy materialization reads after merging the candidates. */
    LanceSearchTableHandle withLazyColumns(List<String> columns) {
        return new LanceSearchTableHandle(table, request, searchFieldId, columns);
    }

    /** The searched table, pinned to the version the search reads. */
    LanceTableHandle getTable() {
        return table;
    }

    boolean isVectorSearch() {
        return request.getSearchQuery().isSetVectorSearch();
    }

    /** The logical search; a copy. */
    TExternalSearchRequest getRequest() {
        return request.deepCopy();
    }

    int getSearchFieldId() {
        return searchFieldId;
    }

    /** The columns read by row id after the candidates are merged, which the scan itself does not read. */
    List<String> getLazyColumns() {
        return lazyColumns;
    }

    /**
     * What each range of the scan searches: its own best {@code top_k + offset} candidates, skipping none. The rows
     * to skip are the best of all ranges, so only the engine's TopN above the scan applies the offset. A vector
     * search also pins its metric, so that no range takes another one from the index it searches.
     */
    TExternalSearchRequest splitRequest() {
        TExternalSearchRequest splitRequest = request.deepCopy();
        if (isVectorSearch()) {
            TVectorSearchParams vector = splitRequest.getSearchQuery().getVectorSearch();
            vector.setTopK(vector.getTopK() + vector.getOffset());
            vector.setOffset(0);
        } else {
            TFullTextSearchParams fullText = splitRequest.getSearchQuery().getFullTextSearch();
            fullText.setTopK(fullText.getTopK() + fullText.getOffset());
            fullText.setOffset(0);
        }
        return splitRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceSearchTableHandle)) {
            return false;
        }
        LanceSearchTableHandle that = (LanceSearchTableHandle) o;
        return searchFieldId == that.searchFieldId && table.equals(that.table) && request.equals(that.request)
                && lazyColumns.equals(that.lazyColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, request, searchFieldId, lazyColumns);
    }

    /** Names the table and what is searched, never the query vector, the query text or the filter: user data. */
    @Override
    public String toString() {
        return table + (isVectorSearch()
                ? " vector_search(" + request.getSearchQuery().getVectorSearch().getColumn() + ")"
                : " full_text_search(" + request.getSearchQuery().getFullTextSearch().getColumn() + ")");
    }
}
