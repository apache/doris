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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TVectorSearchParams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ConnectorVectorSearchTest {
    @Test
    public void searchIsImmutableAndSurvivesPartitionScoping() {
        TExternalSearchRequest wire = new TExternalSearchRequest().setSearchQuery(
                TExternalSearchQuery.vector_search(new TVectorSearchParams().setColumn("embedding").setTopK(7)));
        ConnectorVectorSearch search = new ConnectorVectorSearch(wire, "distance", false, Optional.empty());
        wire.getSearchQuery().getVectorSearch().setTopK(99);
        search.getRequest().getSearchQuery().getVectorSearch().setColumn("changed");
        ConnectorTableHandle handle = new ConnectorTableHandle() {};
        ConnectorScanRequest request = ConnectorScanRequest.builder(handle, Collections.emptyList())
                .coarseSplitSize(512L << 20).vectorSearch(Optional.of(search)).build()
                .withRequiredPartitions(Collections.singletonList("p=1"));
        Assertions.assertEquals(512L << 20, request.getCoarseSplitSize());
        Assertions.assertEquals(Collections.singletonList("p=1"), request.getRequiredPartitions());
        ConnectorVectorSearch scoped = request.getVectorSearch().get();
        Assertions.assertEquals(7, scoped.getRequest().getSearchQuery().getVectorSearch().getTopK());
        Assertions.assertEquals("embedding", scoped.getRequest().getSearchQuery().getVectorSearch().getColumn());
        Assertions.assertEquals("distance", scoped.getDistanceColumn());
        Assertions.assertEquals(search, scoped);
    }

    @Test
    public void connectorsMustExplicitlyAdmitSearch() {
        ConnectorScanPlanProvider provider = new ConnectorScanPlanProvider() {
            @Override
            public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
                return Collections.emptyList();
            }
        };
        ConnectorVectorSearch search = new ConnectorVectorSearch(new TExternalSearchRequest(), "distance",
                false, Optional.empty());
        Assertions.assertFalse(provider.prepareVectorSearch(null, null, search, Collections.emptySet()).isPresent());
    }
}
