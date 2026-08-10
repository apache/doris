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

package org.apache.doris.common.cache;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.SqlCacheContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NereidsSqlCacheManagerPublicationFenceTest {
    private static final String CATALOG = "internal";
    private static final String DATABASE = "fence_db";

    @Test
    public void testFenceChurnIsBoundedAndRejectsRetiredFencePublisher() {
        NereidsSqlCacheManager cacheManager = new NereidsSqlCacheManager(2);
        TableIf firstTable = mockTable(1L, "t1");
        SqlCacheContext stalePublisher =
                new SqlCacheContext(UserIdentity.ROOT, cacheManager.getPublicationSequence());
        stalePublisher.addUsedTable(firstTable);

        cacheManager.invalidateAboutTableAndFencePublication(firstTable);
        cacheManager.invalidateAboutTableAndFencePublication(mockTable(2L, "t2"));
        cacheManager.invalidateAboutTableAndFencePublication(mockTable(3L, "t3"));

        Assertions.assertEquals(2, cacheManager.getTableIdPublicationFenceCount());
        Assertions.assertEquals(2, cacheManager.getTableNamePublicationFenceCount());
        Assertions.assertEquals(0L, cacheManager.getTableInvalidationSequence(
                new TableNameInfo(CATALOG, DATABASE, "t1")));
        Assertions.assertFalse(isPublicationValid(cacheManager, stalePublisher));

        SqlCacheContext freshPublisher =
                new SqlCacheContext(UserIdentity.ROOT, cacheManager.getPublicationSequence());
        freshPublisher.addUsedTable(firstTable);
        Assertions.assertTrue(isPublicationValid(cacheManager, freshPublisher));
    }

    private boolean isPublicationValid(NereidsSqlCacheManager cacheManager, SqlCacheContext context) {
        return Deencapsulation.invoke(cacheManager, "isPublicationValid", context);
    }

    private TableIf mockTable(long tableId, String tableName) {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(catalog.getName()).thenReturn(CATALOG);
        Mockito.when(catalog.isInternalCatalog()).thenReturn(true);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn(DATABASE);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getType()).thenReturn(TableType.OLAP);
        return table;
    }
}
