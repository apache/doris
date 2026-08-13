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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;

public class RangerDorisAccessControllerFactoryTest {

    @Before
    public void forgetPreviouslyCreatedController() throws Exception {
        Field instance = RangerDorisAccessControllerFactory.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    /**
     * The two strings an operator selects this source by, frozen.
     *
     * <p>The name is what {@code access_controller_type} in fe.conf holds; the factory's class name is what a
     * catalog's {@code access_controller.class} may hold, and that one is persisted with the catalog and read
     * back verbatim by later releases. Written as literals on purpose: derived from the class, they would
     * travel with it and a package move would leave this green. Moving this class again means adding the name
     * it has today to the table of superseded class names in {@code AccessControllerManager}.
     */
    @Test
    public void testTheSelectorsThisSourceIsNamedBy() {
        RangerDorisAccessControllerFactory factory = new RangerDorisAccessControllerFactory();

        Assert.assertEquals("ranger-doris", factory.name());
        Assert.assertEquals(
                "org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessControllerFactory",
                factory.getClass().getName());
    }

    /**
     * One controller per FE, whoever asks for it: it starts a Ranger policy refresher, and a second one would
     * mean a second refresher polling the same service.
     */
    @Test
    public void testCreateReturnsSingleton() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerDorisAccessController> mockedConstruction =
                Mockito.mockConstruction(RangerDorisAccessController.class)) {
            AuthorizationPlugin first = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin second = new RangerDorisAccessControllerFactory()
                    .create(Collections.singletonMap("ranger.service.name", "other"), context);

            Assert.assertEquals(1, mockedConstruction.constructed().size());
            Assert.assertSame(first, second);
        }
    }
}
