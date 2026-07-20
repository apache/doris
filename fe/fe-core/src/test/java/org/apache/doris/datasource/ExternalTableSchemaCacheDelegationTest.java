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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class ExternalTableSchemaCacheDelegationTest {

    @Test
    public void testGetFullSchemaDelegatesToGetSchemaCacheValue() {
        List<Column> schema = Lists.newArrayList(new Column("c1", PrimitiveType.INT));
        ExternalTable table = new DelegatingExternalTable(Optional.of(new SchemaCacheValue(schema)));
        Assert.assertEquals(schema, table.getFullSchema());
    }

    @Test
    public void testGetFullSchemaReturnsNullWhenSchemaCacheMissing() {
        ExternalTable table = new DelegatingExternalTable(Optional.empty());
        Assert.assertNull(table.getFullSchema());
    }

    @Test
    public void getSchemaCacheValueBypassesSharedCacheUnderSessionUser() {
        // Read-site routing for the "list != load" fix: when the catalog reports bypass (session=user + delegated
        // credential), getSchemaCacheValue reads schema LIVE via initSchema and NEVER consults the shared
        // name-keyed cache (Env.getExtMetaCacheMgr), so one user's schema is not served to another who can list
        // but not load the table. MUTATION: dropping the bypass branch -> this bare unit reaches Env (null) and
        // NPEs instead of returning the live sentinel.
        List<Column> live = Lists.newArrayList(new Column("c1", PrimitiveType.INT));
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.shouldBypassSchemaCache(Mockito.any())).thenReturn(true);
        BypassProbeTable table = new BypassProbeTable(catalog, live);

        Optional<SchemaCacheValue> result = table.getSchemaCacheValue();
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(live, result.get().getSchema());
        Assert.assertEquals("schema was read live, once, through initSchema (not the shared cache)",
                1, table.initSchemaCalls);
    }

    private static final class BypassProbeTable extends ExternalTable {
        private final List<Column> live;
        private int initSchemaCalls;

        private BypassProbeTable(ExternalCatalog catalog, List<Column> live) {
            this.catalog = catalog;
            this.live = live;
        }

        @Override
        public NameMapping getOrBuildNameMapping() {
            return NameMapping.createForTest("db", "tbl");
        }

        @Override
        public Optional<SchemaCacheValue> initSchema() {
            initSchemaCalls++;
            return Optional.of(new SchemaCacheValue(live));
        }
    }

    private static final class DelegatingExternalTable extends ExternalTable {
        private final Optional<SchemaCacheValue> schemaCacheValue;

        private DelegatingExternalTable(Optional<SchemaCacheValue> schemaCacheValue) {
            this.schemaCacheValue = schemaCacheValue;
        }

        @Override
        public Optional<SchemaCacheValue> getSchemaCacheValue() {
            return schemaCacheValue;
        }
    }
}
