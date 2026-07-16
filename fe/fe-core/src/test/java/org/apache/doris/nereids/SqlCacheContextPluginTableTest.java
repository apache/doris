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

package org.apache.doris.nereids;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.PluginDrivenMvccExternalTable;
import org.apache.doris.nereids.SqlCacheContext.TableVersion;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for the connector-agnostic invalidation token the SQL-result-cache migration wired into
 * {@link SqlCacheContext#addUsedTable}. A flipped lakehouse table is a
 * {@code PluginDrivenMvccExternalTable} (implements MTMVRelatedTableIf); its cache token must be the stable,
 * data-tied {@code getNewestUpdateVersionOrTime()}, NOT the wall-clock {@code getUpdateTime()} it inherits
 * (which changes on every FE schema reload and would serve stale results). A token &lt;= 0 (no reliable
 * data-change signal) fails safe: the table is marked unsupported rather than pinned at a bogus constant.
 */
public class SqlCacheContextPluginTableTest {

    private PluginDrivenMvccExternalTable mockTable(long id, long token) {
        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
        Mockito.when(catalog.getName()).thenReturn("hms_ctl");
        Mockito.when(catalog.getProperties()).thenReturn(Maps.newHashMap());
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn("hms_db");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getType()).thenReturn(TableType.PLUGIN_EXTERNAL_TABLE);
        Mockito.when(table.getNewestUpdateVersionOrTime()).thenReturn(token);
        return table;
    }

    /**
     * A plugin table with a real data-version token is admitted, and the recorded TableVersion carries the
     * connector token (not a wall-clock or a constant 0). RED on the pre-cutover HEAD, which only recorded a
     * token for {@code instanceof HMSExternalTable} and left a flipped plugin table at version 0.
     */
    @Test
    public void testAddUsedTableCapturesConnectorToken() {
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        long token = 1_700_000_000_000L;
        context.addUsedTable(mockTable(42L, token));

        Assertions.assertFalse(context.hasUnsupportedTables());
        Assertions.assertEquals(1, context.getUsedTables().size());
        TableVersion recorded = context.getUsedTables().values().iterator().next();
        Assertions.assertEquals(42L, recorded.id);
        Assertions.assertEquals(token, recorded.version);
        Assertions.assertEquals(TableType.PLUGIN_EXTERNAL_TABLE, recorded.type);
    }

    /**
     * A non-positive token means the connector has no reliable data-change signal (empty partition set /
     * dropped table). The table must be marked unsupported (fail safe) rather than cached against a bogus
     * constant that could never invalidate.
     */
    @Test
    public void testAddUsedTableFailsSafeOnNonPositiveToken() {
        SqlCacheContext context = new SqlCacheContext(UserIdentity.ROOT);
        context.addUsedTable(mockTable(42L, 0L));

        Assertions.assertTrue(context.hasUnsupportedTables());
        Assertions.assertTrue(context.getUsedTables().isEmpty());
    }
}
