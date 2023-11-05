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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ResultSetMetaData;

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
import org.apache.arrow.vector.types.pojo.FieldType;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlightSqlChannel {
    private final Cache<String, FlightSqlResultCacheEntry> resultCache;
    private final BufferAllocator allocator;

    public FlightSqlChannel() {
        resultCache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
                        .removalListener(new ResultRemovalListener())
                        .build();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    // TODO
    public String getRemoteIp() {
        return "0.0.0.0";
    }

    // TODO
    public String getRemoteHostPortString() {
        return "0.0.0.0:0";
    }

    public void addResult(String queryId, String runningQuery, ResultSet resultSet) {
        List<Field> schemaFields = new ArrayList<>();
        List<FieldVector> dataFields = new ArrayList<>();
        List<List<String>> resultData = resultSet.getResultRows();
        ResultSetMetaData metaData = resultSet.getMetaData();

        // TODO: only support varchar type
        for (Column col : metaData.getColumns()) {
            schemaFields.add(new Field(col.getName(), FieldType.nullable(new Utf8()), null));
            VarCharVector varCharVector = new VarCharVector(col.getName(), allocator);
            varCharVector.allocateNew();
            varCharVector.setValueCount(resultData.size());
            dataFields.add(varCharVector);
        }

        for (int i = 0; i < resultData.size(); i++) {
            List<String> row = resultData.get(i);
            for (int j = 0; j < row.size(); j++) {
                String item = row.get(j);
                if (item == null || item.equals(FeConstants.null_string)) {
                    dataFields.get(j).setNull(i);
                } else {
                    ((VarCharVector) dataFields.get(j)).setSafe(i, item.getBytes());
                }
            }
        }
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(schemaFields, dataFields);
        final FlightSqlResultCacheEntry flightSqlResultCacheEntry = new FlightSqlResultCacheEntry(vectorSchemaRoot,
                runningQuery);
        resultCache.put(queryId, flightSqlResultCacheEntry);
    }

    public void addEmptyResult(String queryId, String query) {
        List<Field> schemaFields = new ArrayList<>();
        List<FieldVector> dataFields = new ArrayList<>();
        schemaFields.add(new Field("StatusResult", FieldType.nullable(new Utf8()), null));
        VarCharVector varCharVector = new VarCharVector("StatusResult", allocator);
        varCharVector.allocateNew();
        varCharVector.setValueCount(1);
        varCharVector.setSafe(0, "OK".getBytes());
        dataFields.add(varCharVector);

        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(schemaFields, dataFields);
        final FlightSqlResultCacheEntry flightSqlResultCacheEntry = new FlightSqlResultCacheEntry(vectorSchemaRoot,
                query);
        resultCache.put(queryId, flightSqlResultCacheEntry);
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

    public void reset() {
        resultCache.invalidateAll();
    }

    public void close() {
        reset();
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
