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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalCatalog;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Probe-level coverage for {@link LanceExternalCatalog#checkIndexJobDataset(String, String)}.
 * Admission persists local database and table names, which can be lowercase while the
 * case-sensitive namespace stores {@code Foo.Bar}, and the local lookup itself can miss
 * transiently — so a case-sensitive resolve of the local spelling can false-prove
 * absence while the dataset still exists. The verdicts pinned here: absence is proven
 * only through full case-insensitive listings (whole database listing, then the found
 * remote database's table listing), the durable locator is resolved under the REMOTE
 * spelling, and any listing or resolve failure reads UNRESOLVED, never gone. The
 * dispatcher-side consumption of each outcome is covered by
 * {@code LanceIndexJobRefreshDriverTest}.
 */
public class LanceExternalCatalogDatasetCheckTest {
    private static final String LOCATOR = "file:///warehouse/dataset.lance";

    @BeforeAll
    public static void initializeEnvironment() {
        // onClose() reaches the Env access manager; touch the singleton before assertions.
        Env.getCurrentEnv().getExtMetaCacheMgr();
    }

    @Test
    public void localLowercaseSpellingOfARemoteCasedDatasetIsPresent() throws Exception {
        LanceCatalogClient client = client();
        // The namespace stores Foo.Bar while admission persisted the local foo.bar:
        // the exact spelling that used to read as a case-sensitive TableNotFound and
        // false-prove VERIFIED_ABSENT.
        Mockito.doReturn(Arrays.asList("other", "Foo")).when(client).listDatabaseNames();
        Mockito.doReturn(Arrays.asList("Bar", "unrelated")).when(client).listTableNames("Foo");
        Mockito.doReturn(LOCATOR).when(client).resolveCurrentIndexJobLocator("Foo", "Bar");
        LanceExternalCatalog catalog = catalog(client);
        try {
            LanceIndexDatasetCheck check = catalog.checkIndexJobDataset("foo", "bar");
            Assertions.assertEquals(LanceIndexDatasetCheck.Outcome.PRESENT, check.outcome);
            Assertions.assertEquals(LOCATOR, check.locator);
            // The locator is resolved under the REMOTE spelling only; probing the local
            // spelling case-sensitively is precisely the false-absence this check removed.
            Mockito.verify(client).resolveCurrentIndexJobLocator("Foo", "Bar");
            Mockito.verify(client, Mockito.never()).resolveCurrentIndexJobLocator("foo", "bar");
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void databaseMissingFromTheWholeNamespaceListingIsVerifiedAbsent() throws Exception {
        LanceCatalogClient client = client();
        Mockito.doReturn(Arrays.asList("other", "foo2")).when(client).listDatabaseNames();
        LanceExternalCatalog catalog = catalog(client);
        try {
            LanceIndexDatasetCheck check = catalog.checkIndexJobDataset("db1", "tbl1");
            Assertions.assertEquals(LanceIndexDatasetCheck.Outcome.VERIFIED_ABSENT, check.outcome);
            Assertions.assertNull(check.locator);
            // No case-insensitive database match anywhere: the proof stops before tables.
            Mockito.verify(client, Mockito.never()).listTableNames(Mockito.anyString());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void tableMissingFromTheRemoteDatabaseListingIsVerifiedAbsent() throws Exception {
        LanceCatalogClient client = client();
        Mockito.doReturn(Collections.singletonList("Db1")).when(client).listDatabaseNames();
        Mockito.doReturn(Arrays.asList("Unrelated", "tbl2")).when(client).listTableNames("Db1");
        LanceExternalCatalog catalog = catalog(client);
        try {
            LanceIndexDatasetCheck check = catalog.checkIndexJobDataset("db1", "tbl1");
            Assertions.assertEquals(LanceIndexDatasetCheck.Outcome.VERIFIED_ABSENT, check.outcome);
            Assertions.assertNull(check.locator);
            Mockito.verify(client, Mockito.never()).resolveCurrentIndexJobLocator(Mockito.anyString(),
                    Mockito.anyString());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void listingOrResolveFailureIsUnresolvedNeverAbsent() throws Exception {
        // The database listing itself fails (provider outage): no proof either way.
        LanceCatalogClient outage = client();
        Mockito.doThrow(new RuntimeException("namespace unreachable")).when(outage).listDatabaseNames();
        LanceExternalCatalog outageCatalog = catalog(outage);
        try {
            Assertions.assertEquals(LanceIndexDatasetCheck.Outcome.UNRESOLVED,
                    outageCatalog.checkIndexJobDataset("db1", "tbl1").outcome);
        } finally {
            outageCatalog.onClose();
        }

        // The database matched but its table listing failed mid-proof.
        LanceCatalogClient tableListing = client();
        Mockito.doReturn(Collections.singletonList("Db1")).when(tableListing).listDatabaseNames();
        Mockito.doThrow(new RuntimeException("list tables failed")).when(tableListing).listTableNames("Db1");
        LanceExternalCatalog tableListingCatalog = catalog(tableListing);
        try {
            Assertions.assertEquals(LanceIndexDatasetCheck.Outcome.UNRESOLVED,
                    tableListingCatalog.checkIndexJobDataset("db1", "tbl1").outcome);
        } finally {
            tableListingCatalog.onClose();
        }

        // Both listings matched, but the locator resolve failed: fail closed instead of
        // handing out a PRESENT without a durable locator or an ABSENT without a proof.
        LanceCatalogClient resolve = client();
        Mockito.doReturn(Collections.singletonList("Db1")).when(resolve).listDatabaseNames();
        Mockito.doReturn(Collections.singletonList("Tbl1")).when(resolve).listTableNames("Db1");
        Mockito.doThrow(new RuntimeException("describe table failed"))
                .when(resolve).resolveCurrentIndexJobLocator("Db1", "Tbl1");
        LanceExternalCatalog resolveCatalog = catalog(resolve);
        try {
            LanceIndexDatasetCheck check = resolveCatalog.checkIndexJobDataset("db1", "tbl1");
            Assertions.assertEquals(LanceIndexDatasetCheck.Outcome.UNRESOLVED, check.outcome);
            Assertions.assertNull(check.locator);
        } finally {
            resolveCatalog.onClose();
        }
    }

    /** A leased client whose listing/resolve seams are stubbed; acquire() stays real. */
    private static LanceCatalogClient client() {
        return Mockito.spy(new LanceCatalogClient(Mockito.mock(LanceNamespace.class),
                Mockito.mock(BufferAllocator.class), Mockito.mock(Session.class), "filesystem", "default",
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), Collections.emptyList()));
    }

    private static LanceExternalCatalog catalog(LanceCatalogClient client) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.WAREHOUSE, "/unused/lance-warehouse");
        LanceExternalCatalog catalog = new LanceExternalCatalog(903, "datasetcheck", null, properties, "");
        // Isolate the resource lifecycle from external metadata-cache initialization.
        setField(ExternalCatalog.class, catalog, "objectCreated", true);
        setField(ExternalCatalog.class, catalog, "initialized", true);
        setField(LanceExternalCatalog.class, catalog, "client", client);
        return catalog;
    }

    private static void setField(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
