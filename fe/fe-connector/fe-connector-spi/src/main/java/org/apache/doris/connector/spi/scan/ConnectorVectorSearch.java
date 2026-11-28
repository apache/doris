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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.thrift.TExternalSearchRequest;

import java.util.Objects;
import java.util.Optional;

/** A bounded vector search admitted by the connector for one pinned scan. */
public final class ConnectorVectorSearch {
    private final TExternalSearchRequest request;
    private final String distanceColumn;
    private final boolean tableFunction;
    private final Optional<ConnectorExpression> filter;

    public ConnectorVectorSearch(TExternalSearchRequest request, String distanceColumn,
            boolean tableFunction, Optional<ConnectorExpression> filter) {
        this.request = Objects.requireNonNull(request, "request").deepCopy();
        this.distanceColumn = Objects.requireNonNull(distanceColumn, "distanceColumn");
        this.tableFunction = tableFunction;
        this.filter = Objects.requireNonNull(filter, "filter");
    }

    public TExternalSearchRequest getRequest() {
        return request.deepCopy();
    }

    public String getDistanceColumn() {
        return distanceColumn;
    }

    public boolean isTableFunction() {
        return tableFunction;
    }

    /** Only predicates applied before candidate selection, never the TVF's outer WHERE. */
    public Optional<ConnectorExpression> getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ConnectorVectorSearch)) {
            return false;
        }
        ConnectorVectorSearch that = (ConnectorVectorSearch) other;
        return request.equals(that.request) && distanceColumn.equals(that.distanceColumn)
                && tableFunction == that.tableFunction && filter.equals(that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(request, distanceColumn, tableFunction, filter);
    }
}
