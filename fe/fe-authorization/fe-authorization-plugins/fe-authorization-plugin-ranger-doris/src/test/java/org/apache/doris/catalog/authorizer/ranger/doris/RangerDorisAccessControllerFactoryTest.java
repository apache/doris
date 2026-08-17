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

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
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
import java.util.Map;

public class RangerDorisAccessControllerFactoryTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("user1", "%");
    private static final AuthorizedResource TABLE = AuthorizedResource.table("ctl", "db", "tbl");
    private static final Map<String, String> STRICT =
            Collections.singletonMap(RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY, "false");

    @Before
    @SuppressWarnings("unchecked")
    public void forgetPreviouslyCreatedControllers() throws Exception {
        setStatic("sharedPlugin", null);
        ((Map<Object, Object>) readStatic("byConfiguration")).clear();
    }

    private static void setStatic(String name, Object value) throws Exception {
        field(name).set(null, value);
    }

    private static Object readStatic(String name) throws Exception {
        return field(name).get(null);
    }

    private static Field field(String name) throws Exception {
        Field field = RangerDorisAccessControllerFactory.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    /** Whether the controller allows reading a table, which is all these cases need to tell them apart. */
    private static boolean allowsReading(AuthorizationPlugin controller) {
        try {
            controller.checkPrivilege(SUBJECT, TABLE, AccessRequirements.SELECT, AccessContext.NONE);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
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
     * Bindings configured alike share one controller, and it is the identical object in both places.
     *
     * <p>Identity is what {@code AccessControllerManager.isGlobalScopeAuthority} compares, so this is what
     * keeps a catalog bound to this source while it also governs the instance from asking itself, through a
     * second object, a question it is about to answer anyway.
     *
     * <p>Two factory <em>instances</em> on purpose, and each one built the way the engine builds it. The
     * factory class is loaded by a classloader per plugin directory, so "the same static field" is only shared
     * by the bindings one manager makes; what a test can pin is that repeated bindings within one FE come back
     * to the same controller, which is the case the deployments this source ships to are in.
     */
    @Test
    public void testBindingsConfiguredAlikeShareOneController() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerDorisPlugin> plugin =
                Mockito.mockConstruction(RangerDorisPlugin.class)) {
            AuthorizationPlugin first = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin second = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);

            Assert.assertSame(first, second);
            Assert.assertEquals(1, plugin.constructed().size());
        }
    }

    /**
     * A binding configured differently gets a controller of its own over that same Ranger plugin - it is
     * neither refused nor quietly served the other one's configuration.
     *
     * <p>What is shared is what polling the Ranger service costs. {@code defer_to_global_scope_authority}
     * decides who may read what and is read once, in the controller's constructor, so it belongs to the
     * binding: two catalogs, one of them with the deference switched off, are a configuration an operator can
     * ask for and both of them work. Serving the second from the first would silently give back a deference
     * that was deliberately taken away; refusing it, as this source used to, left whichever catalog lost the
     * race throwing on every statement that touched it.
     */
    @Test
    public void testABindingConfiguredDifferentlyGetsItsOwnControllerOverTheSamePlugin() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        // Somebody else governs instance scope and grants this: the deferring controller honours that, the
        // one configured not to defer does not, and that difference is how their configurations are visible.
        Mockito.when(context.grantedByGlobalScopeAuthority(SUBJECT, AccessRequirements.SELECT))
                .thenReturn(true);
        try (MockedConstruction<RangerDorisPlugin> plugin =
                Mockito.mockConstruction(RangerDorisPlugin.class)) {
            AuthorizationPlugin deferring = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin strict = new RangerDorisAccessControllerFactory().create(STRICT, context);

            Assert.assertNotSame(deferring, strict);
            Assert.assertEquals("one Ranger plugin for the whole FE, whatever the bindings configure",
                    1, plugin.constructed().size());
            Assert.assertTrue("the binding that defers to instance scope lost its configuration",
                    allowsReading(deferring));
            Assert.assertFalse("the binding that refuses to defer was served the other one's configuration",
                    allowsReading(strict));
        }
    }

    /**
     * The Ranger plugin is stopped when no configuration is in use any more, and not before.
     *
     * <p>Nothing else can stop it: the refresher and the policy download timer a Ranger plugin starts run
     * until {@code cleanup()}, so a plugin no binding holds and that was never stopped keeps polling the
     * Ranger service for as long as the FE runs. And it must not be stopped early either - one configuration
     * going away leaves the other one answering out of that same plugin.
     */
    @Test
    public void testThePluginIsStoppedOnceNoConfigurationIsInUse() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerDorisPlugin> construction =
                Mockito.mockConstruction(RangerDorisPlugin.class)) {
            AuthorizationPlugin first = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin second = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            AuthorizationPlugin other = new RangerDorisAccessControllerFactory().create(STRICT, context);
            RangerDorisPlugin plugin = construction.constructed().get(0);

            first.close();
            Mockito.verify(plugin, Mockito.never()).cleanup();

            second.close();
            Mockito.verify(plugin, Mockito.never()).cleanup();

            other.close();
            Mockito.verify(plugin).cleanup();
        }
    }

    /**
     * A controller the last binding let go of refuses every later check, rather than answering out of a plugin
     * that is being cleaned up - or, once, dereferencing a field the shutdown had just set to null.
     *
     * <p>The manager hands a controller out without a lock and closes it outside every lock, so a query can be
     * holding this one when the catalog it was bound to is dropped.
     */
    @Test
    public void testAReleasedControllerRefusesLaterChecks() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Mockito.when(context.grantedByGlobalScopeAuthority(SUBJECT, AccessRequirements.SELECT))
                .thenReturn(true);
        try (MockedConstruction<RangerDorisPlugin> plugin =
                Mockito.mockConstruction(RangerDorisPlugin.class)) {
            AuthorizationPlugin controller = new RangerDorisAccessControllerFactory()
                    .create(Collections.emptyMap(), context);
            Assert.assertTrue(allowsReading(controller));

            controller.close();

            Assert.assertFalse("a released controller still answered a privilege check",
                    allowsReading(controller));
            IllegalStateException refused = Assert.assertThrows(IllegalStateException.class,
                    () -> controller.getRowFilters(SUBJECT, AuthorizedResource.table("ctl", "db", "tbl"),
                            AccessContext.NONE));
            Assert.assertTrue(refused.getMessage(),
                    refused.getMessage().contains(RangerDorisAccessController.NAME));
        }
    }
}
