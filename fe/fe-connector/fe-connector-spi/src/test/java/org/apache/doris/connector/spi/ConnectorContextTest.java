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

import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ConnectorContextTest {

    /** A minimal ConnectorContext implementing only the two abstract methods; everything else default. */
    private static ConnectorContext minimalContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    @Test
    public void getStorageProperties_defaultsToEmptyList() {
        // The new storage seam (D-003): fe-core overrides this to hand the connector the catalog's
        // typed fe-filesystem StorageProperties. Every OTHER connector keeps the default empty list,
        // so introducing the seam must not change their behavior -- and it must never return null.
        List<StorageProperties> storage = minimalContext().getStorageProperties();
        Assertions.assertNotNull(storage, "getStorageProperties() must never return null");
        Assertions.assertTrue(storage.isEmpty(),
                "default getStorageProperties() must be empty so non-paimon connectors are unaffected");
    }
}
