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
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;

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
        setStatic("instance", null);
        setStatic("instanceProperties", null);
        setStatic("holders", 0);
    }

    private static void setStatic(String name, Object value) throws Exception {
        Field field = RangerDorisAccessControllerFactory.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(null, value);
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
     *
     * <p>Two factory <em>instances</em> on purpose, and each one built the way the engine builds it. The
     * factory class is loaded by a classloader per plugin directory, so "the same static field" is only
     * shared by the bindings one manager makes; what a test can pin is that repeated bindings within one FE
     * produce one controller, which is the case the deployments this source ships to are in.
     */
    @Test
    public void testEveryBindingSharesOneController() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerDorisAccessController> mockedConstruction =
                Mockito.mockConstruction(RangerDorisAccessController.class)) {
            AuthorizationPlugin first = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin second = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);

            Assert.assertEquals(1, mockedConstruction.constructed().size());
            Assert.assertSame(first, second);
        }
    }

    /**
     * A second binding asking for a different configuration is refused, not quietly given the first one's.
     *
     * <p>There is one controller and it reads {@code ranger.defer_to_global_scope_authority} once, in its
     * constructor. Serving the second binding from it would mean a catalog created with the deference
     * switched off silently keeping it on because another catalog was created first - which is the same
     * failure the strict parsing of that property exists to prevent, reached by another route.
     */
    @Test
    public void testASecondBindingWithADifferentConfigurationIsRefused() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerDorisAccessController> mockedConstruction =
                Mockito.mockConstruction(RangerDorisAccessController.class)) {
            new RangerDorisAccessControllerFactory().create(Collections.emptyMap(), context);

            IllegalArgumentException refused = Assert.assertThrows(IllegalArgumentException.class,
                    () -> new RangerDorisAccessControllerFactory().create(
                            Collections.singletonMap(
                                    RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY, "false"),
                            context));

            Assert.assertTrue(refused.getMessage(),
                    refused.getMessage().contains(RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY));
            Assert.assertEquals(1, mockedConstruction.constructed().size());
        }
    }

    /**
     * The shared controller is shut down when the last binding lets go of it, and not before.
     *
     * <p>Nothing else can stop it: the refresher and the policy download timer a Ranger plugin starts run
     * until {@code cleanup()}, so a controller that no binding holds and that was never closed keeps polling
     * the Ranger service for as long as the FE runs.
     */
    @Test
    public void testTheControllerIsShutDownOnceNoBindingHoldsIt() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerDorisAccessController> mockedConstruction = Mockito.mockConstruction(
                RangerDorisAccessController.class,
                (mock, ctx) -> Mockito.doCallRealMethod().when(mock).close())) {
            AuthorizationPlugin first = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin second = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            RangerDorisAccessController controller = mockedConstruction.constructed().get(0);

            first.close();
            Mockito.verify(controller, Mockito.never()).shutdown();

            second.close();
            Mockito.verify(controller).shutdown();
        }
    }
}
