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

package org.apache.doris.connector.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ConnectorSecuritySurfaceTest {
    @Test
    void addingEndpointChecksPreservesTheExistingDriverPolicy() {
        ConnectorSecuritySurface original = ConnectorSecuritySurface.ofJdbcDriverUrls("catalog.type", "driver_url");
        ConnectorSecuritySurface composed = original.withSsrfEndpoints("kms.endpoint");
        Assertions.assertEquals(List.of("driver_url"), composed.jdbcDriverUrlKeys());
        Assertions.assertEquals("catalog.type", composed.jdbcDriverUrlFlavorKey());
        Assertions.assertEquals(List.of("kms.endpoint"), composed.ssrfEndpointKeys());
        Assertions.assertTrue(original.ssrfEndpointKeys().isEmpty(), "composing must not mutate the original surface");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> composed.ssrfEndpointKeys().clear());
    }

    @Test
    void composingEndpointChecksKeepsExistingEndpointsWithoutDuplicates() {
        ConnectorSecuritySurface original = ConnectorSecuritySurface.ofSsrfEndpoints("catalog.uri");
        ConnectorSecuritySurface composed = original.withSsrfEndpoints("catalog.uri", "kms.endpoint");
        Assertions.assertEquals(List.of("catalog.uri", "kms.endpoint"), composed.ssrfEndpointKeys());
        Assertions.assertEquals(List.of("catalog.uri"), original.ssrfEndpointKeys());
    }
}
