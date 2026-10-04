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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class FlightSqlResultCacheEntryTest {
    @Test
    public void testQueryIdIsIndependentOfInputAndReturnedObjects() throws Exception {
        VectorSchemaRoot root = Mockito.mock(VectorSchemaRoot.class);
        TUniqueId queryId = new TUniqueId(1, 2);
        try (FlightSqlResultCacheEntry entry = new FlightSqlResultCacheEntry(root, "SELECT 1", queryId)) {
            queryId.setHi(3);
            queryId.setLo(4);
            Assertions.assertEquals(new TUniqueId(1, 2), entry.getQueryId());
            TUniqueId returned = entry.getQueryId();
            returned.setLo(5);
            Assertions.assertEquals(new TUniqueId(1, 2), entry.getQueryId());
            Assertions.assertSame(root, entry.getVectorSchemaRoot());
        }
        Mockito.verify(root).close();
    }

    @Test
    public void testLegacyResultHasNoQueryLogIdentity() throws Exception {
        VectorSchemaRoot root = Mockito.mock(VectorSchemaRoot.class);
        try (FlightSqlResultCacheEntry entry = new FlightSqlResultCacheEntry(root, "SELECT 1")) {
            Assertions.assertNull(entry.getQueryId());
        }
        Mockito.verify(root).close();
    }

    @Test
    public void testLegacyChannelResultDoesNotTreatTheCacheKeyAsQueryIdentity() {
        FlightSqlChannel channel = new FlightSqlChannel();
        try {
            ShowResultSet result = new ShowResultSet(ShowResultSetMetaData.builder()
                    .addColumn(new Column("value", ScalarType.createVarchar(10))).build(),
                    ImmutableList.of(ImmutableList.of("7")));
            channel.addResult("independent-cache-key", "SELECT 7", result);
            FlightSqlResultCacheEntry entry = channel.getResult("independent-cache-key");
            Assertions.assertNull(entry.getQueryId());
            Assertions.assertEquals("SELECT 7", entry.getQuery());
            Assertions.assertEquals("7", entry.getVectorSchemaRoot().getVector(0).getObject(0).toString());
            channel.invalidate("independent-cache-key");
            Assertions.assertNull(channel.getResult("independent-cache-key"));
            Assertions.assertEquals(0, channel.getAllocatedMemory());
        } finally {
            channel.close();
        }
    }
}
