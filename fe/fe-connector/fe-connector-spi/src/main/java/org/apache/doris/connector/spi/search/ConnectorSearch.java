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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A search the connector has prepared: what the engine scans, which columns the relation has, and how the engine
 * merges the candidates of the scan's ranges; see {@link ConnectorSearchOps}.
 */
public final class ConnectorSearch {
    private final ConnectorTableHandle scanHandle;
    private final List<ConnectorColumn> columns;
    private final long topK;
    private final long offset;
    private final ConnectorSearchLazyMaterialization lazyMaterialization;

    /**
     * @throws IllegalArgumentException when {@code topK} is not positive or {@code offset} is negative
     */
    public ConnectorSearch(ConnectorTableHandle scanHandle, List<ConnectorColumn> columns, long topK, long offset,
            ConnectorSearchLazyMaterialization lazyMaterialization) {
        this.scanHandle = Objects.requireNonNull(scanHandle, "scanHandle");
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.lazyMaterialization = Objects.requireNonNull(lazyMaterialization, "lazyMaterialization");
        if (topK <= 0 || offset < 0) {
            throw new IllegalArgumentException("A search returns a positive number of rows after a non-negative "
                    + "offset, but was top_k=" + topK + ", offset=" + offset);
        }
        this.topK = topK;
        this.offset = offset;
    }

    /**
     * The handle the engine scans the search through, with the connector's scan-plan provider and column handles,
     * as it scans a table. It pins whatever the search reads, such as a snapshot, so that the columns and every
     * range describe the same data. Each range returns at most {@code topK + offset} candidates of its own, each
     * with its result column filled, and the engine keeps the best of all ranges; a range applies no offset, as
     * the rows to skip are the best of all ranges, not of one.
     */
    public ConnectorTableHandle getScanHandle() {
        return scanHandle;
    }

    /**
     * The relation's columns: the searched table's columns, then the function's result column
     * ({@link ConnectorSearchFunction#getResultColumn()}, FLOAT). The column handles of {@link #getScanHandle()}
     * include the result column, so that a query that reads it keeps it. The engine authorizes the search by the
     * table's columns listed here: the user needs SELECT on every one of them, as the arguments may read any.
     */
    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    /** How many rows the search returns, after skipping {@link #getOffset()} rows; positive. */
    public long getTopK() {
        return topK;
    }

    /** How many of the best rows the search skips; not negative. */
    public long getOffset() {
        return offset;
    }

    public ConnectorSearchLazyMaterialization getLazyMaterialization() {
        return lazyMaterialization;
    }
}
