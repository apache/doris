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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What each REFRESH statement reaches once it arrives at the connector.
 *
 * <p>The three hooks were no-ops for as long as the connector remembered nothing, and a cache without them
 * would be worse than no cache: metadata that no statement can refresh. Nothing here opens a driver -- the
 * connector's constructor only reads properties, which is what lets an FE replaying the edit log build a
 * catalog whose driver file it does not have.
 */
class AdbcConnectorCacheHookTest {

    private static final AdbcNamespace MAIN = new AdbcNamespace("main", "");
    private static final AdbcTableHandle MAIN_T1 = new AdbcTableHandle(MAIN, "t1");

    private static AdbcConnector connector() {
        return new AdbcConnector(Map.of(AdbcCatalogProperties.URI, "file:/tmp/does-not-matter.db",
                AdbcCatalogProperties.DRIVER_URL, "libadbc_driver_sqlite.so"), context());
    }

    private static ConnectorContext context() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "adbc_test";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    /** Remembers one table's schema and answers how many times the source was asked for it since. */
    private static AtomicInteger rememberSchemaOf(AdbcMetadataCache cache, AdbcTableHandle handle) {
        AtomicInteger reads = new AtomicInteger();
        cache.tableSchema(handle, () -> {
            reads.incrementAndGet();
            return new Schema(Collections.emptyList());
        });
        return reads;
    }

    private static AtomicInteger rememberTableNamesOf(AdbcMetadataCache cache, AdbcNamespace namespace) {
        AtomicInteger reads = new AtomicInteger();
        cache.tableNames(namespace, () -> {
            reads.incrementAndGet();
            return List.of("t1");
        });
        return reads;
    }

    @Test
    void refreshTableReachesTheCatalogsMemory() {
        AdbcConnector connector = connector();
        AtomicInteger reads = rememberSchemaOf(connector.metadataCache(), MAIN_T1);

        connector.invalidateTable("main", "t1");

        connector.metadataCache().tableSchema(MAIN_T1, () -> {
            reads.incrementAndGet();
            return new Schema(Collections.emptyList());
        });
        Assertions.assertEquals(2, reads.get());
    }

    @Test
    void refreshDatabaseReachesTheCatalogsMemory() {
        AdbcConnector connector = connector();
        AtomicInteger reads = rememberTableNamesOf(connector.metadataCache(), MAIN);

        connector.invalidateDb("main");

        connector.metadataCache().tableNames(MAIN, () -> {
            reads.incrementAndGet();
            return List.of("t1");
        });
        Assertions.assertEquals(2, reads.get());
    }

    @Test
    void refreshCatalogReachesTheCatalogsMemory() {
        AdbcConnector connector = connector();
        AtomicInteger reads = new AtomicInteger();
        connector.metadataCache().namespaces(() -> {
            reads.incrementAndGet();
            return List.of(MAIN);
        });

        connector.invalidateAll();

        connector.metadataCache().namespaces(() -> {
            reads.incrementAndGet();
            return List.of(MAIN);
        });
        Assertions.assertEquals(2, reads.get());
    }

    /**
     * The premise the cache rests on. Its keys are bare object names, so whatever one query caches is served
     * to the next query whoever sends it -- correct only while a catalog reaches the source as one fixed
     * principal. Declaring {@link ConnectorCapability#SUPPORTS_USER_SESSION} would make the connection carry
     * the querying user's identity, and then these keys would hand one user's metadata to another. If this
     * test ever fails, the capability is not the thing to revert: the cache keys have to carry the identity
     * too, or the cache has to be per-session.
     */
    @Test
    void theCatalogReachesItsSourceAsOnePrincipalSoOneCacheCanServeEveryUser() {
        Assertions.assertFalse(connector().getCapabilities().contains(
                        ConnectorCapability.SUPPORTS_USER_SESSION),
                "ADBC now projects the querying user onto the connection, so AdbcMetadataCache's keys"
                        + " (database / table names) no longer identify what they cache");
    }
}
