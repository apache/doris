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

package org.apache.doris.connector.spi.search;

/**
 * The engine's search table functions, which a connector serves for its tables through
 * {@link ConnectorSearchOps}.
 *
 * <p>Each one is a relation over one table: the table's columns plus a result column that holds the rows nearest
 * to a query, or best matching it. The name and type of the result column and the order in which the engine merges
 * the candidates belong to the function, not to the connector: every range of the scan returns its own best
 * candidates, and the engine keeps the best of all of them with a TopN over the result column.</p>
 */
public enum ConnectorSearchFunction {

    /**
     * {@code vector_search(...)}: the rows nearest to a query vector. The result column {@code _distance} is a
     * FLOAT distance, merged smallest first.
     */
    VECTOR_SEARCH("vector_search", "_distance", true),

    /**
     * {@code full_text_search(...)}: the rows that best match a text query. The result column {@code _score} is a
     * FLOAT relevance score, merged largest first.
     */
    FULL_TEXT_SEARCH("full_text_search", "_score", false);

    private final String functionName;
    private final String resultColumn;
    private final boolean resultAscending;

    ConnectorSearchFunction(String functionName, String resultColumn, boolean resultAscending) {
        this.functionName = functionName;
        this.resultColumn = resultColumn;
        this.resultAscending = resultAscending;
    }

    /** The table function's name, as users write it. */
    public String getFunctionName() {
        return functionName;
    }

    /**
     * The name of the FLOAT column that holds each row's distance or score. The columns a connector returns in
     * {@link ConnectorSearch#getColumns()} end with it, and its scan ranges fill it.
     */
    public String getResultColumn() {
        return resultColumn;
    }

    /** Whether the engine keeps the smallest results (a distance) rather than the largest (a score). */
    public boolean isResultAscending() {
        return resultAscending;
    }
}
