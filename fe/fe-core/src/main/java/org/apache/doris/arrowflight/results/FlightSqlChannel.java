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

package org.apache.doris.arrowflight.results;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlightSqlChannel {
    private final Cache<String, FlightSqlResultCacheEntry> resultCache;
    private final BufferAllocator allocator;

    public FlightSqlChannel() {
        this(Ticker.systemTicker());
    }

    @VisibleForTesting
    public FlightSqlChannel(Ticker ticker) {
        // The Stmt result is not picked up by the Client within 10 minutes and will be deleted.
        resultCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker).removalListener(new ResultRemovalListener()).build();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    public void addResult(String queryId, String runningQuery, ResultSet resultSet) {
        List<Field> fields = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (Column col : metaData.getColumns()) {
            fields.add(Field.nullable(col.getName(), new Utf8()));
        }
        cacheRows(queryId, runningQuery, fields, resultSet.getResultRows());
    }

    public void addOKResult(String queryId, String query) {
        cacheRows(queryId, query, List.of(Field.nullable("StatusResult", new Utf8())), List.of(List.of("0")));
    }

    private void cacheRows(String queryId, String query, List<Field> fields, List<List<String>> rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
        try {
            root.allocateNew();
            List<FieldVector> columns = root.getFieldVectors();
            for (int i = 0; i < rows.size(); i++) {
                List<String> row = rows.get(i);
                for (int j = 0; j < row.size(); j++) {
                    VarCharVector column = (VarCharVector) columns.get(j);
                    String value = row.get(j);
                    if (value == null || value.equals(FeConstants.null_string)) {
                        column.setNull(i);
                    } else {
                        column.setSafe(i, value.getBytes());
                    }
                }
            }
            root.setRowCount(rows.size());
            resultCache.put(queryId, new FlightSqlResultCacheEntry(root, query));
        } catch (RuntimeException | Error e) {
            // Until publication the cache's removal listener cannot release these buffers.
            root.close();
            throw e;
        }
    }

    public FlightSqlResultCacheEntry getResult(String queryId) {
        return resultCache.getIfPresent(queryId);
    }

    public void invalidate(String handle) {
        resultCache.invalidate(handle);
    }

    public long resultNum() {
        return resultCache.size();
    }

    /** Runs expiry maintenance while the session owns execution; reads never extend the write deadline. */
    public void expireResults() {
        resultCache.cleanUp();
    }

    // Returns the off-heap memory (bytes) currently held by this channel's Arrow allocator.
    // Exposed so tests can assert prepared-statement handling does not leak VectorSchemaRoot buffers.
    public long getAllocatedMemory() {
        return allocator.getAllocatedMemory();
    }

    public void reset() {
        resultCache.invalidateAll();
    }

    public void close() {
        reset();
        allocator.close();
    }

    private static class ResultRemovalListener implements RemovalListener<String, FlightSqlResultCacheEntry> {
        @Override
        public void onRemoval(@NotNull final RemovalNotification<String, FlightSqlResultCacheEntry> notification) {
            try {
                AutoCloseables.close(notification.getValue());
            } catch (final Exception e) {
                // swallow
            }
        }
    }
}
