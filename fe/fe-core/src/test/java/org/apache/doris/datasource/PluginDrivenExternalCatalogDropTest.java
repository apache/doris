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

import org.apache.doris.common.DdlException;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class PluginDrivenExternalCatalogDropTest {
    @Test
    public void testDropResolvesRemoteHandleAndInvalidatesLocalTable() throws Exception {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            fixture.catalog.dropTable("local_db", "events", false, false, false, false, false, false);

            Mockito.verify(fixture.metadata).dropTable(fixture.session, fixture.handle);
            Mockito.verify(fixture.connector).invalidateTable("remote_db", "events");
            Mockito.verify(fixture.database).unregisterTable("events");
            Mockito.verify(fixture.editLog).logDropTable(Mockito.any());
        }
    }

    @Test
    public void testDropIfExistsCleansStaleLocalTable() throws Exception {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            Mockito.when(fixture.metadata.getTableHandle(fixture.session, "remote_db", "events"))
                    .thenReturn(Optional.empty());

            fixture.catalog.dropTable("local_db", "events", false, false, false, true, false, false);

            Mockito.verify(fixture.metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
            Mockito.verify(fixture.database).unregisterTable("events");
        }
    }

    @Test
    public void testConnectorRejectionDoesNotInvalidateOrJournalDrop() {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            Mockito.doThrow(new DorisConnectorException("Drop table is not supported"))
                    .when(fixture.metadata).dropTable(fixture.session, fixture.handle);

            Assertions.assertThrows(DdlException.class, () -> fixture.catalog.dropTable(
                    "local_db", "events", false, false, false, false, false, false));

            Mockito.verify(fixture.database, Mockito.never()).unregisterTable(Mockito.any());
            Mockito.verify(fixture.editLog, Mockito.never()).logDropTable(Mockito.any());
        }
    }

    @Test
    public void testReplayInvalidatesLocalTableWithoutRemoteCall() {
        try (PluginDrivenCatalogTestFixture fixture = new PluginDrivenCatalogTestFixture()) {
            fixture.catalog.replayDropTable("local_db", "events");

            Mockito.verify(fixture.database).unregisterTable("events");
            Mockito.verifyNoInteractions(fixture.metadata);
        }
    }
}
