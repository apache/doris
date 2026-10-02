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

import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PluginDrivenExternalCatalogCreateTest {
    @Test
    public void testDelegatesStructuredCreateRequest() throws Exception {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            Mockito.when(fixture.database.getTableNullable("events")).thenReturn(null);
            Mockito.when(fixture.metadata.getTableHandle(fixture.session, "remote_db", "events"))
                    .thenReturn(Optional.empty());
            CreateTableInfo info = createInfo();

            Assertions.assertFalse(fixture.catalog.createTable(info));

            ArgumentCaptor<ConnectorCreateTableRequest> request =
                    ArgumentCaptor.forClass(ConnectorCreateTableRequest.class);
            Mockito.verify(fixture.metadata).createTable(Mockito.same(fixture.session), request.capture());
            Assertions.assertEquals("remote_db", request.getValue().getDbName());
            Assertions.assertEquals("events", request.getValue().getTableName());
            Assertions.assertEquals(List.of("id", "payload"),
                    request.getValue().getColumns().stream().map(column -> column.getName()).toList());
            Assertions.assertFalse(request.getValue().getColumns().get(0).isNullable());
            Assertions.assertTrue(request.getValue().getColumns().get(1).isNullable());
            Assertions.assertEquals(Map.of("owner", "doris", "custom", "value"),
                    request.getValue().getProperties());
            Assertions.assertEquals("events table", request.getValue().getComment());
            Assertions.assertTrue(request.getValue().isIfNotExists());
            Mockito.verify(fixture.editLog).logCreateTable(Mockito.any());
            Mockito.verify(fixture.connector).invalidateTable("remote_db", "events");
            Mockito.verify(fixture.database).invalidateTableForCreate("events");
        }
    }

    @Test
    public void testIfNotExistsPreservesTheCtasShortCircuit() throws Exception {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            Assertions.assertTrue(fixture.catalog.createTable(createInfo()));
            Mockito.verify(fixture.metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.editLog, Mockito.never()).logCreateTable(Mockito.any());
        }
    }

    private static CreateTableInfo createInfo() {
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("local_db");
        Mockito.when(info.getTableName()).thenReturn("events");
        Mockito.when(info.getColumnDefinitions()).thenReturn(List.of(
                new ColumnDefinition("id", BigIntType.INSTANCE, false),
                new ColumnDefinition("payload", StringType.INSTANCE, true)));
        Mockito.when(info.getProperties()).thenReturn(Map.of("owner", "doris"));
        Mockito.when(info.getExtProperties()).thenReturn(Map.of("custom", "value"));
        Mockito.when(info.getComment()).thenReturn("events table");
        Mockito.when(info.isIfNotExists()).thenReturn(true);
        return info;
    }
}
