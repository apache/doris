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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class HiveMetadataOpsTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testColdTruncateReplayInvalidatesCanonicalDatabaseScope() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getId()).thenReturn(2L);
        Mockito.when(db.getFullName()).thenReturn("MixedDb");
        Mockito.when(db.getTableForReplay("mixedtbl")).thenReturn(Optional.empty());
        Mockito.when(catalog.getDbForReplay("mixeddb")).thenReturn((Optional) Optional.of(db));

        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };

        HiveMetadataOps metadataOps = new HiveMetadataOps(catalog, Mockito.mock(HMSCachedClient.class));
        metadataOps.afterTruncateTable("mixeddb", "mixedtbl", 100L);

        // The event's lower-cased spelling must not be routed as a cache key directly.
        Mockito.verify(metaCacheMgr).invalidateTableByNameOrWider(1L, "mixeddb", "mixedtbl");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testColdTruncateReplayWithColdDatabaseUsesWideningHelper() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(3L);
        Mockito.when(catalog.getDbForReplay("db")).thenReturn(Optional.empty());

        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        new MockUp<Env>() {
            @Mock
            Env getCurrentEnv() {
                return env;
            }
        };

        HiveMetadataOps metadataOps = new HiveMetadataOps(catalog, Mockito.mock(HMSCachedClient.class));
        metadataOps.afterTruncateTable("db", "tbl", 100L);

        Mockito.verify(metaCacheMgr).invalidateTableByNameOrWider(3L, "db", "tbl");
    }

    @Test
    public void testAfterDropTableWithoutResolvedDatabaseRetiresGeneration() {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(4L);
        Mockito.when(catalog.getName()).thenReturn("hive");
        Mockito.when(catalog.getDbForReplay("db")).thenReturn(Optional.empty());

        HiveMetadataOps metadataOps = new HiveMetadataOps(catalog, Mockito.mock(HMSCachedClient.class));
        metadataOps.afterDropTable("db", "tbl");

        // An empty lookup can hide a resident canonical database object after a lost mode-2 mapping.
        Mockito.verify(catalog).retireUnresolvedDatabaseGeneration();
    }
}
