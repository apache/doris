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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache.WritableTableLease;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;

import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseIcebergActionTest {

    @Test
    public void testActionRetainsWritableGenerationUntilExecutionFinishes() throws Exception {
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Table icebergTable = Mockito.mock(Table.class);
        AtomicBoolean released = new AtomicBoolean();
        WritableTableLease lease = Mockito.mock(WritableTableLease.class);
        Mockito.when(lease.getTable()).thenReturn(icebergTable);
        Mockito.when(lease.getAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        Mockito.doAnswer(invocation -> {
            released.set(true);
            return null;
        }).when(lease).close();
        BaseIcebergAction action = new BaseIcebergAction(
                "test", Collections.emptyMap(), Optional.empty(), Optional.empty()) {
            @Override
            protected void registerIcebergArguments() {
            }

            @Override
            protected List<String> executeIcebergAction(TableIf table, Table retainedTable) {
                Assert.assertSame(dorisTable, table);
                Assert.assertSame(icebergTable, retainedTable);
                Assert.assertFalse(released.get());
                return Collections.singletonList("ok");
            }

            @Override
            public String getDescription() {
                return "test action";
            }
        };

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(IcebergUtils.class)) {
            mockedUtils.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable))
                    .thenReturn(lease);
            Assert.assertEquals(Collections.singletonList("ok"), action.executeAction(dorisTable));
            Assert.assertTrue(released.get());
        }
    }
}
