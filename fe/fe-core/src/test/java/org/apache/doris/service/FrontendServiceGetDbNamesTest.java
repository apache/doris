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

package org.apache.doris.service;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FrontendServiceGetDbNamesTest {
    private final Env env = Mockito.mock(Env.class);
    private final CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
    private final AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    private final CatalogIf catalog = Mockito.mock(CatalogIf.class);
    private final FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));
    private MockedStatic<Env> envMock;

    @BeforeEach
    public void setUp() {
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(catalogMgr.listCatalogs()).thenReturn(Collections.singletonList(catalog));
        Mockito.when(catalog.getName()).thenReturn("example_catalog");
        Mockito.when(catalog.getId()).thenReturn(42L);
        Mockito.when(accessManager.checkDbPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
    }

    @Test
    public void testNamesOnlyDoesNotMaterializeDatabases() throws Exception {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            names.add("namespace_" + i);
        }
        names.add(InfoSchemaDb.DATABASE_NAME);
        names.add("mysql");
        Mockito.when(catalog.getDbNames()).thenReturn(names);
        Mockito.when(catalog.getAllDbs()).thenThrow(new AssertionError("Database objects must not be loaded"));

        TGetDbsParams params = params();
        params.setIncludeDbIds(false);
        TGetDbsResult result = service.getDbNames(params);

        Assertions.assertEquals(names.size(), result.getDbsSize());
        Assertions.assertEquals(names.size(), result.getCatalogsSize());
        Assertions.assertEquals(names.size(), result.getCatalogIdsSize());
        Assertions.assertTrue(result.getCatalogs().stream().allMatch("example_catalog"::equals));
        Assertions.assertTrue(result.getCatalogIds().stream().allMatch(id -> id == 42L));
        Assertions.assertFalse(result.isSetDbIds());
        Mockito.verify(catalog, Mockito.times(1)).getDbNames();
        Mockito.verify(catalog, Mockito.never()).getAllDbs();
    }

    @Test
    public void testNamesOnlyPreservesPrivilegesPatternAndDisplayNames() throws Exception {
        Mockito.when(catalog.getDbNames()).thenReturn(Arrays.asList("visible_a", "visible_hidden", "other"));
        Mockito.when(accessManager.checkDbPriv(Mockito.any(UserIdentity.class), Mockito.eq("example_catalog"),
                Mockito.eq("visible_hidden"), Mockito.eq(PrivPredicate.SHOW))).thenReturn(false);

        boolean original = GlobalVariable.showFullDbNameInInfoSchemaDb;
        try {
            for (boolean showFullName : new boolean[] {false, true}) {
                GlobalVariable.showFullDbNameInInfoSchemaDb = showFullName;
                TGetDbsParams params = params();
                params.setIncludeDbIds(false);
                params.setPattern(showFullName ? "example_catalog.visible%" : "visible%");
                TGetDbsResult result = service.getDbNames(params);
                Assertions.assertEquals(Collections.singletonList(
                        showFullName ? "example_catalog.visible_a" : "visible_a"), result.getDbs());
                Assertions.assertFalse(result.isSetDbIds());
            }
        } finally {
            GlobalVariable.showFullDbNameInInfoSchemaDb = original;
        }
    }

    @Test
    public void testExistingCallersKeepDatabaseIds() throws Exception {
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getFullName()).thenReturn("visible_a");
        Mockito.when(database.getId()).thenReturn(123L);
        Mockito.when(catalog.getAllDbs()).thenReturn(Collections.singletonList(database));

        for (boolean explicitlySet : new boolean[] {false, true}) {
            TGetDbsParams params = params();
            if (explicitlySet) {
                params.setIncludeDbIds(true);
            }
            TGetDbsResult result = service.getDbNames(params);
            Assertions.assertEquals(Collections.singletonList(123L), result.getDbIds());
            Assertions.assertEquals(Collections.singletonList(42L), result.getCatalogIds());
        }
        Mockito.verify(catalog, Mockito.never()).getDbNames();
    }

    @Test
    public void testAbsentIncludeDbIdsOnWireKeepsLegacyDefault() throws Exception {
        TGetDbsParams legacy = params();
        legacy.unsetIncludeDbIds();
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        TGetDbsParams decoded = new TGetDbsParams();
        deserializer.deserialize(decoded, serializer.serialize(legacy));
        Assertions.assertTrue(decoded.isIncludeDbIds());

        legacy.setIncludeDbIds(false);
        deserializer.deserialize(decoded, serializer.serialize(legacy));
        Assertions.assertFalse(decoded.isIncludeDbIds());
    }

    @Test
    public void testNamesOnlyPreservesEmptyCatalogPlaceholder() throws Exception {
        Mockito.when(catalog.getDbNames()).thenReturn(Collections.emptyList());
        TGetDbsParams params = params();
        params.setIncludeDbIds(false);
        params.setGetNullCatalog(true);

        TGetDbsResult result = service.getDbNames(params);
        Assertions.assertEquals(Collections.singletonList("NULL"), result.getDbs());
        Assertions.assertEquals(Collections.singletonList("example_catalog"), result.getCatalogs());
        Assertions.assertEquals(Collections.singletonList(42L), result.getCatalogIds());
        Assertions.assertFalse(result.isSetDbIds());
    }

    @Test
    public void testNamesOnlySkipsUnavailableCatalog() throws Exception {
        Mockito.when(catalog.getDbNames()).thenThrow(new IllegalStateException("Metadata service unavailable"));
        TGetDbsParams params = params();
        params.setIncludeDbIds(false);

        TGetDbsResult result = service.getDbNames(params);
        Assertions.assertTrue(result.getDbs().isEmpty());
        Assertions.assertTrue(result.getCatalogs().isEmpty());
        Assertions.assertFalse(result.isSetDbIds());
    }

    private TGetDbsParams params() {
        TGetDbsParams params = new TGetDbsParams();
        params.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
        return params;
    }
}
