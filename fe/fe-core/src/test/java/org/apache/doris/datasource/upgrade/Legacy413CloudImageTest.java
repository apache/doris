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

package org.apache.doris.datasource.upgrade;

import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.Legacy413Fixtures;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The same 4.1.3 upgrade, in cloud (storage-compute separated) mode.
 *
 * <p>This is deliberately one small class rather than a second copy of the whole matrix. The only
 * cloud-conditional branch anywhere on the catalog path is the tail of {@code GsonUtils}'
 * {@code dsTypeAdapterFactory}, and it swaps exactly one label: {@code InternalCatalog} resolves to
 * {@link CloudInternalCatalog}. Every external-catalog label, and every database and table label, is
 * registered unconditionally. The fixture proves it empirically -- the generator was run twice on 4.1.3,
 * once with {@code deploy_mode="cloud"}, and the two modules differ in exactly one entry (id 0). So a
 * cloud copy of the external matrix would assert nothing the non-cloud one does not.
 */
public class Legacy413CloudImageTest {

    static {
        // MUST be a static initialiser of this class, not @BeforeAll or @BeforeEach.
        // GsonUtils builds its type-adapter factories in a static initialiser, and
        // RuntimeTypeAdapterFactory.create() snapshots the label map, so flipping the mode after anything
        // has touched GsonUtils is a SILENT no-op: the tests would still pass while asserting nothing about
        // cloud mode. Safe because surefire runs fe-core with reuseForks=false, i.e. one fresh JVM per class.
        Config.deploy_mode = "cloud";
    }

    @Test
    public void cloudInternalCatalogSurvivesTheUpgrade() throws Exception {
        CatalogMgr mgr = Legacy413Fixtures.loadCloudCatalogMgr();

        Assertions.assertTrue(Config.isCloudMode(), "the deploy_mode latch must have been set before class init");
        Assertions.assertSame(CloudInternalCatalog.class, mgr.getCatalog(0L).getClass());
    }

    @Test
    public void preCloudInternalCatalogLabelIsRemappedInCloudMode() throws Exception {
        // The compat direction: metadata written before the cloud internal catalog got its own label carries
        // "clazz":"InternalCatalog". In cloud mode that label must resolve to CloudInternalCatalog, otherwise
        // a cloud FE upgraded from such an image loses its internal catalog entirely.
        // The non-cloud fixture IS that metadata -- no synthetic JSON needed.
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        Assertions.assertSame(CloudInternalCatalog.class, mgr.getCatalog(0L).getClass(),
                "in cloud mode the legacy 'InternalCatalog' label must resolve to CloudInternalCatalog");
    }

    @Test
    public void externalCatalogMigrationIsIdenticalInCloudMode() throws Exception {
        // Cheap regression guard, not a matrix: if a future change ever makes an external catalog's
        // migration mode-dependent, this is what notices.
        CatalogMgr mgr = Legacy413Fixtures.loadCloudCatalogMgr();

        assertMigrated(mgr, 10001L, "hms");
        assertMigrated(mgr, 10003L, "iceberg");
        assertMigrated(mgr, 10009L, "paimon");
        // Resource-backed: the logType backfill must work the same way in cloud mode.
        assertMigrated(mgr, 10023L, "trino-connector");
    }

    private void assertMigrated(CatalogMgr mgr, long id, String expectedType) {
        CatalogIf<?> catalog = mgr.getCatalog(id);
        Assertions.assertSame(PluginDrivenExternalCatalog.class, catalog.getClass(),
                "catalog " + id + " must migrate identically in cloud mode");
        Assertions.assertEquals(expectedType, ((ExternalCatalog) catalog).getType());
    }
}
