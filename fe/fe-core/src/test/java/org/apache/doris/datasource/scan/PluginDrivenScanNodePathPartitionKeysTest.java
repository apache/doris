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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class PluginDrivenScanNodePathPartitionKeysTest {
    @Test
    public void namedKeysAvoidBuildingScanPropertiesBeforeTheFilterIsAttached() throws Exception {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.getPathPartitionKeys(session, handle)).thenReturn(Optional.of(List.of("part")));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(node, "connector", connector);
        Deencapsulation.setField(node, "currentHandle", handle);
        Deencapsulation.setField(node, "connectorSession", session);
        Assertions.assertEquals(List.of("part"), node.getPathPartitionKeys());
        Mockito.verify(provider).getPathPartitionKeys(session, handle);
        Mockito.verifyNoMoreInteractions(provider);
    }
}
