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

package org.apache.doris.connector.api.handle;

/**
 * A table handle that wraps a raw passthrough SQL query (e.g., from query() TVF).
 *
 * <p>Connector scan plan providers should detect this handle type and use the
 * raw query directly instead of building a query from table/column metadata.</p>
 */
public class PassthroughQueryTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String query;

    public PassthroughQueryTableHandle(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return "PassthroughQuery{"
                + query.substring(0, Math.min(50, query.length()))
                + (query.length() > 50 ? "..." : "") + "}";
    }
}
