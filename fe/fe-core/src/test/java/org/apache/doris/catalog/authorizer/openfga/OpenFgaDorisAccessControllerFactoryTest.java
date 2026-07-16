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

package org.apache.doris.catalog.authorizer.openfga;

import org.apache.doris.mysql.privilege.CatalogAccessController;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class OpenFgaDorisAccessControllerFactoryTest {

    @Before
    public void resetSingleton() throws Exception {
        Field field = OpenFgaDorisAccessControllerFactory.class.getDeclaredField("instance");
        field.setAccessible(true);
        field.set(null, null);
    }

    private Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(OpenFgaConfig.API_URL, "http://localhost:8080");
        props.put(OpenFgaConfig.STORE_ID, "01TESTSTORE");
        return props;
    }

    @Test
    public void testFactoryIdentifier() {
        Assert.assertEquals("openfga-doris", new OpenFgaDorisAccessControllerFactory().factoryIdentifier());
    }

    @Test
    public void testCreateAccessControllerReturnsSingleton() {
        try (MockedConstruction<OpenFgaClientWrapper> mockedConstruction =
                Mockito.mockConstruction(OpenFgaClientWrapper.class)) {
            CatalogAccessController first = new OpenFgaDorisAccessControllerFactory()
                    .createAccessController(validProps());
            CatalogAccessController second = new OpenFgaDorisAccessControllerFactory()
                    .createAccessController(validProps());

            // Only one OpenFGA client is built for the JVM, and the controller is shared.
            Assert.assertEquals(1, mockedConstruction.constructed().size());
            Assert.assertSame(first, second);
        }
    }

    @Test
    public void testCreateAccessControllerRejectsMissingApiUrl() {
        Map<String, String> props = new HashMap<>();
        props.put(OpenFgaConfig.STORE_ID, "01TESTSTORE");

        Assert.assertThrows(IllegalArgumentException.class, () ->
                new OpenFgaDorisAccessControllerFactory().createAccessController(props));
    }
}
