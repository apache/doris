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
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class CatalogFactoryTest {

    @Test
    public void testCloseConnectorWhenCreateValidationFails() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogMgr.CATALOG_TYPE_PROP, "jdbc");
        Connector connector = Mockito.mock(Connector.class);
        Mockito.doThrow(new DdlException("validation failed"))
                .when(connector).preCreateValidation(Mockito.any());
        CreateCatalogCommand command = new CreateCatalogCommand(
                "jdbc_catalog", false, "", "", properties);

        try (MockedStatic<ConnectorFactory> factory = Mockito.mockStatic(ConnectorFactory.class)) {
            factory.when(() -> ConnectorFactory.createConnector(
                    Mockito.eq("jdbc"), Mockito.anyMap(), Mockito.any()))
                    .thenReturn(connector);

            Assert.assertThrows(DdlException.class, () -> CatalogFactory.createFromCommand(1, command));

            Mockito.verify(connector).close();
        }
    }
}
