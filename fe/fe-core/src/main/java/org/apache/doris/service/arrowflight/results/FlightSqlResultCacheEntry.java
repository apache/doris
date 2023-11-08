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

package org.apache.doris.service.arrowflight.results;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Objects;


public final class FlightSqlResultCacheEntry implements AutoCloseable {

    private final VectorSchemaRoot vectorSchemaRoot;
    private final String query;

    public FlightSqlResultCacheEntry(final VectorSchemaRoot vectorSchemaRoot, final String query) {
        this.vectorSchemaRoot = Objects.requireNonNull(vectorSchemaRoot, "result cannot be null.");
        this.query = query;
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public void close() throws Exception {
        vectorSchemaRoot.clear();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof VectorSchemaRoot)) {
            return false;
        }
        final VectorSchemaRoot that = (VectorSchemaRoot) other;
        return vectorSchemaRoot.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vectorSchemaRoot);
    }
}
