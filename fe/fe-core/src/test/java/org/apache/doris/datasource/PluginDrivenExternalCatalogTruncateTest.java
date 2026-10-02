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

import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.persist.TruncateTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class PluginDrivenExternalCatalogTruncateTest {
    @Test
    public void testTruncateResolvesRemoteHandleAndRefreshesLocalTable() throws Exception {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            fixture.catalog.truncateTable("local_db", "events", null, false, "");

            Mockito.verify(fixture.metadata).truncateTable(fixture.session, fixture.handle, null);
            Mockito.verify(fixture.refreshManager).refreshTableInternal(
                    Mockito.same(fixture.database), Mockito.same(fixture.table), Mockito.anyLong());
            Mockito.verify(fixture.editLog).logTruncateTable(Mockito.any());
        }
    }

    @Test
    public void testConnectorRejectionDoesNotJournalOrRefreshTruncate() {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            Mockito.doThrow(new DorisConnectorException("Truncate table is not supported"))
                    .when(fixture.metadata).truncateTable(fixture.session, fixture.handle, null);

            Assertions.assertThrows(DdlException.class,
                    () -> fixture.catalog.truncateTable("local_db", "events", null, false, ""));

            Mockito.verifyNoInteractions(fixture.refreshManager);
            Mockito.verify(fixture.editLog, Mockito.never()).logTruncateTable(Mockito.any());
        }
    }

    @Test
    public void testTruncateRejectsMissingRemoteTable() {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            Mockito.when(fixture.metadata.getTableHandle(fixture.session, "remote_db", "events"))
                    .thenReturn(Optional.empty());

            Assertions.assertThrows(DdlException.class,
                    () -> fixture.catalog.truncateTable("local_db", "events", null, false, ""));

            Mockito.verify(fixture.metadata, Mockito.never()).truncateTable(Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.verifyNoInteractions(fixture.refreshManager);
        }
    }

    @Test
    public void testPartitionTruncateIsForwardedAndRejectedByTheConnector() {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            PartitionNamesInfo partitions = Mockito.mock(PartitionNamesInfo.class);
            Mockito.when(partitions.getPartitionNames()).thenReturn(List.of("p1"));
            Mockito.doThrow(new DorisConnectorException("TRUNCATE TABLE supports full tables only"))
                    .when(fixture.metadata).truncateTable(fixture.session, fixture.handle, List.of("p1"));

            Assertions.assertThrows(DdlException.class,
                    () -> fixture.catalog.truncateTable("local_db", "events", partitions, false, ""));

            Mockito.verify(fixture.metadata).truncateTable(fixture.session, fixture.handle, List.of("p1"));
            Mockito.verifyNoInteractions(fixture.refreshManager);
        }
    }

    @Test
    public void testReplayRefreshesLocalTableWithoutRemoteCall() {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            fixture.catalog.replayTruncateTable(new TruncateTableInfo(
                    "delta_catalog", "local_db", "events", null, 1L));

            Mockito.verify(fixture.refreshManager).refreshTableInternal(fixture.database, fixture.table, 1L);
            Mockito.verifyNoInteractions(fixture.metadata);
        }
    }
}
