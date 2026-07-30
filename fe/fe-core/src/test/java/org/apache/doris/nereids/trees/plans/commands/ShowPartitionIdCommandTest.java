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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class ShowPartitionIdCommandTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;
    private static final long PARTITION_ID = 3L;

    @Test
    void testFormatUsesPartitionAwareFallback() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(Mockito.same(context), Mockito.eq(PrivPredicate.ADMIN)))
                .thenReturn(true);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbIds()).thenReturn(Collections.singletonList(DB_ID));
        Mockito.when(catalog.getDbNullable(DB_ID)).thenReturn(database);
        Mockito.when(database.getTables()).thenReturn(Collections.singletonList(table));
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getId()).thenReturn(DB_ID);
        Mockito.when(table.getPartition(PARTITION_ID)).thenReturn(partition);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(partition.getName()).thenReturn("p1");
        Mockito.when(table.getInvertedIndexFileStorageFormatForPartition(PARTITION_ID))
                .thenReturn(TInvertedIndexFileStorageFormat.V2);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> contextMock = Mockito.mockStatic(ConnectContext.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            contextMock.when(ConnectContext::get).thenReturn(context);

            ShowResultSet result = new ShowPartitionIdCommand(PARTITION_ID).doRun(context, null);

            Assertions.assertEquals("V2", result.getResultRows().get(0).get(5));
            Mockito.verify(table).getInvertedIndexFileStorageFormatForPartition(PARTITION_ID);
        }
    }
}
