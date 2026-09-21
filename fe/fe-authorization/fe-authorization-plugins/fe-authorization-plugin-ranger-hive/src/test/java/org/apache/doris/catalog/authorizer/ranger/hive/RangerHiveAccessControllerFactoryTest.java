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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Map;

public class RangerHiveAccessControllerFactoryTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("user1", "%");
    private static final long PRODUCTION_GRACE_SECONDS =
            RangerHiveAccessControllerFactory.idleStackGraceSeconds;

    @AfterEach
    public void restoreTheGracePeriod() {
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = PRODUCTION_GRACE_SECONDS;
    }

    /**
     * The two strings an operator selects this source by, frozen.
     *
     * <p>The class name matters most here: this source governs one catalog, and a catalog names it in
     * {@code access_controller.class}, which is persisted with the catalog and read back verbatim by every
     * later release - the regression suite for Hive catalogs writes exactly this string. It survived the move
     * out of fe-core unchanged, which is the only reason those catalogs kept working, so nothing but this
     * test stands between the next package move and a catalog nobody can query.
     *
     * <p>Written as literals on purpose: derived from the class, they would travel with it and a package move
     * would leave this green. Moving this class means adding the name it has today to the table of superseded
     * class names in {@code AccessControllerManager}.
     */
    @Test
    public void testTheSelectorsThisSourceIsNamedBy() {
        RangerHiveAccessControllerFactory factory = new RangerHiveAccessControllerFactory();

        Assertions.assertEquals("ranger-hive", factory.name());
        Assertions.assertEquals(
                "org.apache.doris.catalog.authorizer.ranger.hive.RangerHiveAccessControllerFactory",
                factory.getClass().getName());
    }

    /**
     * Two bindings reading one Ranger service poll it once.
     *
     * <p>A second plugin over the same service downloads the same policies a second time and answers out of
     * its own copy of them, so what the service name keys is the polling and not the binding. Everything a
     * binding configures belongs to the controller instead, which is why the two configurations below get a
     * controller each over one stack.
     */
    @Test
    public void testBindingsOnOneServiceShareItsAuditStack() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Map<String, String> deferring = ImmutableMap.of("ranger.service.name", "shared_stack");
        Map<String, String> strict = ImmutableMap.of("ranger.service.name", "shared_stack",
                "ranger.defer_to_global_scope_authority", "false");

        try (MockedConstruction<RangerHivePlugin> plugins = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> handlers =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            AuthorizationPlugin first = new RangerHiveAccessControllerFactory().create(deferring, context);
            AuthorizationPlugin second = new RangerHiveAccessControllerFactory().create(strict, context);

            Assertions.assertNotSame(first, second, "two differently configured bindings were served one controller");
            Assertions.assertEquals(1, plugins.constructed().size(),
                    "a second Ranger plugin was started for a service already being polled");

            stopPolling(first, second);
        }
    }

    /**
     * A Ranger service nothing reads any more stops being polled.
     *
     * <p>The stack is keyed on the operator's {@code ranger.service.name}, and new keys are ordinary: the
     * validation run of a {@code CREATE CATALOG} builds a source and lets it go again - including one whose
     * name was a typo - a first check on a catalog builds one lazily, and {@code ALTER CATALOG} can change
     * the name. Kept for the life of the process, each would go on holding a policy refresher thread and a
     * download timer, and one naming a service Ranger cannot resolve logs an error every thirty seconds for
     * as long as the FE runs.
     */
    @Test
    public void testAServiceNothingReadsStopsBeingPolled() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Map<String, String> properties = ImmutableMap.of("ranger.service.name", "abandoned");
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = 0;

        try (MockedConstruction<RangerHivePlugin> plugins = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> handlers =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            AuthorizationPlugin only = new RangerHiveAccessControllerFactory().create(properties, context);
            RangerHivePlugin polling = plugins.constructed().get(0);

            only.close();
            awaitNothingPolled();

            // With a timeout: the entry leaves the map before the stack is stopped, deliberately - stopping
            // it joins the Ranger policy refresher and must not happen under the factory's lock.
            Mockito.verify(polling, Mockito.timeout(30_000)).cleanup();
            Mockito.verify(handlers.constructed().get(0), Mockito.timeout(30_000)).flushAudit();

            // And the next binding on that name starts a fresh one rather than reviving a stopped plugin.
            AuthorizationPlugin later = new RangerHiveAccessControllerFactory().create(properties, context);
            Assertions.assertEquals(2, plugins.constructed().size(),
                    "re-acquiring a stopped service did not start a plugin for it");
            stopPolling(later);
        }
    }

    /**
     * A binding arriving before the grace period is up keeps the stack, and its plugin, exactly as it was.
     *
     * <p>This is the case the grace period exists for. {@code ALTER CATALOG} and {@code REFRESH CATALOG}
     * both detach a catalog's access controller and attach a new one, so the count of bindings on a service
     * passes through zero as a matter of course. Stopped there and rebuilt, the plugin costs a
     * {@code cleanup()} on the DDL thread - it interrupts the policy refresher and joins it with no timeout -
     * and its whole first load on the way back up, before the catalog answers a single check again.
     */
    @Test
    public void testAReAcquireWithinTheGraceKeepsThePluginUp() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Map<String, String> properties = ImmutableMap.of("ranger.service.name", "re_attached");
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = 3600;

        try (MockedConstruction<RangerHivePlugin> plugins = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> handlers =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            AuthorizationPlugin detached = new RangerHiveAccessControllerFactory().create(properties, context);
            RangerHivePlugin polling = plugins.constructed().get(0);

            detached.close();
            AuthorizationPlugin reattached = new RangerHiveAccessControllerFactory()
                    .create(properties, context);

            Assertions.assertEquals(1, plugins.constructed().size(),
                    "re-attaching a catalog built a second Ranger plugin for the same service");
            Mockito.verify(polling, Mockito.never()).cleanup();

            stopPolling(reattached);
        }
    }

    /**
     * A stack whose plugin failed its first load is kept for nobody: the next binding on its service - the
     * same configuration, which would otherwise have been served the controller already over it - stops it
     * and gets a stack built afresh, which is how a configuration fixed in fe/conf reaches this source
     * without an FE restart. Every configuration on that service goes with it, and nothing on any other
     * service is touched. Stopped on the spot rather than after the grace period, because that period is
     * for a stack a re-attach is about to ask for again, and nothing will ask for this one.
     */
    @Test
    public void testAStackWhosePluginFailedIsReplacedByTheNextBinding() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Map<String, String> deferring = ImmutableMap.of("ranger.service.name", "failed_load");
        Map<String, String> strict = ImmutableMap.of("ranger.service.name", "failed_load",
                "ranger.defer_to_global_scope_authority", "false");
        Map<String, String> healthy = ImmutableMap.of("ranger.service.name", "healthy");
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = 3600;

        try (MockedConstruction<RangerHivePlugin> plugins = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> handlers =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            AuthorizationPlugin overTheFailed = new RangerHiveAccessControllerFactory().create(deferring, context);
            AuthorizationPlugin strictOverTheFailed = new RangerHiveAccessControllerFactory().create(strict, context);
            AuthorizationPlugin overTheHealthy = new RangerHiveAccessControllerFactory().create(healthy, context);
            RangerHivePlugin failed = plugins.constructed().get(0);
            Mockito.when(failed.isFailed()).thenReturn(true);

            AuthorizationPlugin replacement = new RangerHiveAccessControllerFactory().create(deferring, context);

            Assertions.assertEquals(3, plugins.constructed().size(), "the failed plugin was handed out again");
            Assertions.assertNotSame(overTheFailed, replacement,
                    "the controller over the failed stack was handed out again");
            Assertions.assertEquals(2, RangerHiveAccessControllerFactory.polledServiceCount());
            // What is left of the failed stack is stopped on the way: its audit flushed, its plugin's stop
            // asked for - a no-op on one that stopped itself, and the same call for a mock.
            Mockito.verify(failed).cleanup();
            Mockito.verify(handlers.constructed().get(0)).flushAudit();
            Mockito.verify(plugins.constructed().get(1), Mockito.never()).cleanup();
            Mockito.verify(plugins.constructed().get(2), Mockito.never()).cleanup();
            // The other configuration on that service is over the new stack from here on too, and the other
            // service was never touched.
            AuthorizationPlugin strictReplacement = new RangerHiveAccessControllerFactory().create(strict, context);
            Assertions.assertNotSame(strictOverTheFailed, strictReplacement,
                    "a differently configured binding was handed its controller over the failed stack again");
            Assertions.assertSame(overTheHealthy, new RangerHiveAccessControllerFactory().create(healthy, context));
            Assertions.assertEquals(3, plugins.constructed().size(), "a fourth plugin was built");

            // The bindings still over the failed stack let go with nothing of their own to stop, and without
            // touching what replaced it.
            overTheFailed.close();
            strictOverTheFailed.close();
            Mockito.verify(failed, Mockito.times(1)).cleanup();
            Assertions.assertEquals(2, RangerHiveAccessControllerFactory.polledServiceCount(),
                    "letting go of a controller the factory had already let go of stopped a stack");

            stopPolling(replacement, strictReplacement, overTheHealthy, overTheHealthy);
        }
    }

    /**
     * A failed stack nothing reads any more - released before its load ended, a stop scheduled for it - is
     * stopped by the binding that lets go of it, once, and the scheduled stop is cancelled rather than left
     * to stop it a second time.
     */
    @Test
    public void testAFailedStackAwaitingItsScheduledStopIsStoppedOnceByTheNextBinding() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Map<String, String> properties = ImmutableMap.of("ranger.service.name", "failed_then_idle");
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = 3600;

        try (MockedConstruction<RangerHivePlugin> plugins = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> handlers =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            new RangerHiveAccessControllerFactory().create(properties, context).close();
            RangerHivePlugin failed = plugins.constructed().get(0);
            Assertions.assertEquals(1, RangerHiveAccessControllerFactory.polledServiceCount(),
                    "the stack was stopped inside the grace period");
            Mockito.when(failed.isFailed()).thenReturn(true);

            AuthorizationPlugin replacement = new RangerHiveAccessControllerFactory().create(properties, context);

            Assertions.assertEquals(2, plugins.constructed().size(), "the failed plugin was handed out again");
            Mockito.verify(failed).cleanup();
            Mockito.verify(handlers.constructed().get(0)).flushAudit();
            Assertions.assertEquals(1, RangerHiveAccessControllerFactory.polledServiceCount());

            stopPolling(replacement);
            Mockito.verify(failed, Mockito.times(1)).cleanup();
        }
    }

    /**
     * A controller let go of with the failed stack is still one controller shared by every binding configured
     * alike, and the last of them letting go is the one that fences it - not the first, as it would be if
     * letting go of the stack had also dropped the count. Fenced early, the catalog still bound to it would
     * refuse as "closed", hiding the cause its refusals carry.
     */
    @Test
    public void testTheLastBindingOverAFailedStackIsTheOneThatFencesTheController() {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        Map<String, String> properties = ImmutableMap.of("ranger.service.name", "failed_shared");
        AuthorizedResource.Table table = AuthorizedResource.table("ctl", "db", "tbl");
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = 3600;

        try (MockedConstruction<RangerHivePlugin> plugins = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> handlers =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            AuthorizationPlugin first = new RangerHiveAccessControllerFactory().create(properties, context);
            AuthorizationPlugin second = new RangerHiveAccessControllerFactory().create(properties, context);
            Assertions.assertSame(first, second);
            Mockito.when(plugins.constructed().get(0).isFailed()).thenReturn(true);
            AuthorizationPlugin replacement = new RangerHiveAccessControllerFactory().create(properties, context);
            Assertions.assertEquals(2, plugins.constructed().size());

            first.close();
            // Still held by the second binding: refused for what it is - a plugin with no answer - not as closed.
            IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class,
                    () -> second.getRowFilters(SUBJECT, table, AccessContext.NONE));
            Assertions.assertTrue(refused.getMessage().contains("has no answer"), refused.getMessage());

            second.close();
            refused = Assertions.assertThrows(IllegalStateException.class,
                    () -> second.getRowFilters(SUBJECT, table, AccessContext.NONE));
            Assertions.assertTrue(refused.getMessage().contains("has been closed"), refused.getMessage());

            stopPolling(replacement);
            Mockito.verify(plugins.constructed().get(0), Mockito.times(1)).cleanup();
        }
    }

    /** Closes what a case acquired and waits until nothing is polled, so the next case starts clean. */
    private static void stopPolling(AuthorizationPlugin... acquired) {
        RangerHiveAccessControllerFactory.idleStackGraceSeconds = 0;
        for (AuthorizationPlugin plugin : acquired) {
            plugin.close();
        }
        awaitNothingPolled();
    }

    private static void awaitNothingPolled() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (RangerHiveAccessControllerFactory.polledServiceCount() > 0) {
            Assertions.assertTrue(System.currentTimeMillis() < deadline,
                    "the stack of a service nothing reads is still polling it");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
