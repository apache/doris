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

package org.apache.doris.catalog.authorizer.ranger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The loading protocol of {@link BackgroundLoadedRangerPlugin}: {@code init()} returns while the Ranger admin
 * is still being asked, nothing is answered before the admin has, and a stop while that is going on is
 * honoured once it is over. The admin stands in for is a latch this test holds.
 */
public class BackgroundLoadedRangerPluginTest {

    /** A plugin whose first load is answered by this test instead of by a Ranger admin. */
    private static final class Loading extends BackgroundLoadedRangerPlugin {
        /** Released by the test to let the load end, the way an admin's answer would. */
        private final CountDownLatch adminAnswers = new CountDownLatch(1);
        private final CountDownLatch loadStarted = new CountDownLatch(1);
        private final AtomicBoolean loadRan = new AtomicBoolean();
        /** Whether the load does what {@code RangerBasePlugin.init()} does first: call {@code cleanup()}. */
        private final boolean clearsStateFirst;
        /** The user store the load installs, standing for the one the enricher downloads; null for none. */
        private final RangerUserStore userStore;
        /**
         * Whether the load installs a policy engine and then throws - what {@code RangerBasePlugin.init()}
         * does when a chained plugin fails to start, after the root engine is up and being refreshed.
         */
        private final boolean publishesThenThrows;
        /** Whether an engine was there when the load threw; what the failure is supposed to take down. */
        private final AtomicBoolean enginePublished = new AtomicBoolean();

        private Loading(boolean clearsStateFirst, RangerUserStore userStore) {
            this(clearsStateFirst, userStore, false);
        }

        private Loading(boolean clearsStateFirst, RangerUserStore userStore, boolean publishesThenThrows) {
            // Service type "test" reads ranger-test-*.xml, none of which exist here, and no service name:
            // nothing about a Ranger admin is configured, which is fine for a load this class performs itself.
            super("test", null, null);
            this.clearsStateFirst = clearsStateFirst;
            this.userStore = userStore;
            this.publishesThenThrows = publishesThenThrows;
            if (publishesThenThrows) {
                // The engine built below would otherwise ask a Ranger admin for the user store; there is none.
                getConfig().set("ranger.plugin.test.use.rangerGroups", "false");
            }
        }

        @Override
        protected void firstLoad() {
            loadRan.set(true);
            if (clearsStateFirst) {
                cleanup();
            }
            loadStarted.countDown();
            try {
                Assertions.assertTrue(adminAnswers.await(30, TimeUnit.SECONDS), "the test never let the load end");
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            if (userStore != null) {
                getPluginContext().setAuthContext(new RangerAuthContext(null, null, null, userStore));
            }
            if (publishesThenThrows) {
                setPolicies(emptyPolicies());
                enginePublished.set(getPolicyEngine() != null);
                throw new IllegalStateException("a chained plugin failed to start");
            }
        }

        /** Policies with nothing in them, enough for RangerBasePlugin to build and install an engine. */
        private static ServicePolicies emptyPolicies() {
            RangerServiceDef serviceDef = new RangerServiceDef();
            serviceDef.setName("test");
            ServicePolicies policies = new ServicePolicies();
            policies.setServiceName("test");
            policies.setServiceDef(serviceDef);
            policies.setPolicyVersion(1L);
            return policies;
        }

        private void letTheLoadEnd() {
            adminAnswers.countDown();
        }
    }

    private Loading plugin;

    private Loading loading() {
        return loading(false, null);
    }

    private Loading loading(boolean clearsStateFirst, RangerUserStore userStore) {
        return loading(clearsStateFirst, userStore, false);
    }

    private Loading loading(boolean clearsStateFirst, RangerUserStore userStore, boolean publishesThenThrows) {
        plugin = new Loading(clearsStateFirst, userStore, publishesThenThrows);
        return plugin;
    }

    @AfterEach
    public void letAnyLoadEnd() {
        if (plugin != null) {
            plugin.letTheLoadEnd();
        }
    }

    private static <T> T within(CompletableFuture<T> future, long seconds)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(seconds, TimeUnit.SECONDS);
    }

    private static void assertStillWaiting(CompletableFuture<?> future) throws InterruptedException {
        // A wait that is not there shows up as the future completing well within this.
        Thread.sleep(200);
        Assertions.assertFalse(future.isDone(), "answered before the load had ended");
    }

    /** What used to hold the FE's start: init() now returns while the admin is still being asked. */
    @Test
    public void testInitReturnsWhileTheLoadRuns() throws Exception {
        Loading plugin = loading();

        plugin.init();

        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));
        Assertions.assertFalse(plugin.isLoaded());
        plugin.letTheLoadEnd();
        plugin.awaitLoaded();
        Assertions.assertTrue(plugin.isLoaded());
    }

    /**
     * A check made while the load runs is answered after it: out of the engine the load installs (none here,
     * so the answer is Ranger's null), never out of the nothing that is there meanwhile.
     */
    @Test
    public void testAnswersWaitForTheLoad() throws Exception {
        Loading plugin = loading();
        plugin.init();
        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));

        CompletableFuture<Object> answer = CompletableFuture.supplyAsync(
                () -> plugin.isAccessAllowed(new RangerAccessRequestImpl()));

        assertStillWaiting(answer);
        plugin.letTheLoadEnd();
        Assertions.assertNull(within(answer, 10));
    }

    /**
     * The groups a request carries are read after the load too, since the user store arrives with it. Read
     * before, a user the store puts in two groups would have been sent with none - and matched no policy item
     * written against either, the deny ones included.
     */
    @Test
    public void testGroupsAreReadAfterTheLoad() throws Exception {
        Loading plugin = loading(false, new RangerUserStore(1L, null, null,
                ImmutableMap.of("user1", ImmutableSet.of("analysts", "etl"))));
        plugin.init();
        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));

        CompletableFuture<Set<String>> groups = CompletableFuture.supplyAsync(
                () -> RangerUserStoreGroups.groupsOf(plugin, "user1"));

        assertStillWaiting(groups);
        plugin.letTheLoadEnd();
        Assertions.assertEquals(ImmutableSet.of("analysts", "etl"), within(groups, 10));
    }

    /**
     * A load that throws after it has installed an engine - {@code RangerBasePlugin.init()} publishes the
     * policy engine and starts its refresher before it initializes the chained plugins - is a failed load
     * all the same: what it installed is stopped, and every answer is a refusal, as the log line says.
     */
    @Test
    public void testALoadThatFailsAfterPublishingRefusesEverything() throws Exception {
        Loading plugin = loading(false, null, true);
        plugin.init();
        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));

        CompletableFuture<Object> answer = CompletableFuture.supplyAsync(
                () -> plugin.isAccessAllowed(new RangerAccessRequestImpl()));
        assertStillWaiting(answer);
        plugin.letTheLoadEnd();

        Assertions.assertNull(within(answer, 10), "answered out of the engine the failed load left behind");
        Assertions.assertTrue(plugin.enginePublished.get(), "the load never published an engine to take down");
        Assertions.assertTrue(plugin.isLoaded());
        Assertions.assertTrue(plugin.isFailed());
        Assertions.assertTrue(plugin.isStopped(), "the engine and refresher of a failed load were left running");
        Assertions.assertEquals(-1L, plugin.getPoliciesVersion(), "the engine the failed load left is still there");
        Assertions.assertNull(plugin.isAccessAllowed(new RangerAccessRequestImpl()));
        Assertions.assertNull(plugin.evalRowFilterPolicies(new RangerAccessRequestImpl(), null));
        Assertions.assertNull(plugin.evalDataMaskPolicies(new RangerAccessRequestImpl(), null));
        // Stopping it again, as the factory will, is a no-op rather than a second stop.
        plugin.cleanup();
        Assertions.assertTrue(plugin.isStopped());
    }

    /**
     * A caller that stops needing the answer - a controller closed while its check waits - stops waiting,
     * while the load goes on for whoever else holds the plugin.
     */
    @Test
    public void testAWaitGivesUpWhenAsked() throws Exception {
        Loading plugin = loading();
        plugin.init();
        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));
        AtomicBoolean giveUp = new AtomicBoolean();

        CompletableFuture<Void> waiting = CompletableFuture.runAsync(() -> plugin.awaitLoaded(giveUp::get));

        assertStillWaiting(waiting);
        giveUp.set(true);
        within(waiting, 10);
        Assertions.assertFalse(plugin.isLoaded(), "the load was cut short with the waiter");
        plugin.letTheLoadEnd();
        plugin.awaitLoaded(() -> false);
        Assertions.assertTrue(plugin.isLoaded());
    }

    /** A plugin never initialized - a test's own, answering out of its overrides - has nothing to wait for. */
    @Test
    public void testNeverInitializedHasNothingToWaitFor() {
        Loading plugin = loading();

        plugin.awaitLoaded();

        Assertions.assertFalse(plugin.isLoaded());
        Assertions.assertFalse(plugin.loadRan.get());
    }

    @Test
    public void testInitTwiceIsRefused() throws Exception {
        Loading plugin = loading();
        plugin.init();

        Assertions.assertThrows(IllegalStateException.class, plugin::init);
        plugin.letTheLoadEnd();
    }

    /**
     * Stopped while loading - the dry run of a CREATE CATALOG, the loser of a race in a factory - the plugin
     * is stopped once the load has ended, by the loader, and the caller is not made to wait for that.
     */
    @Test
    public void testStopWhileLoadingRunsAfterTheLoad() throws Exception {
        Loading plugin = loading();
        plugin.init();
        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));

        plugin.cleanup();

        Assertions.assertFalse(plugin.isStopped(), "stopped under a load still running");
        plugin.letTheLoadEnd();
        plugin.awaitLoaded();
        // The loader counts the load as ended a moment before it stops the plugin.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!plugin.isStopped() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(plugin.isStopped());
    }

    /** Stopped before the load has begun, there is nothing to load for and the load is skipped. */
    @Test
    public void testStopBeforeTheLoadSkipsIt() {
        Loading plugin = loading();

        plugin.cleanup();
        plugin.init();
        plugin.awaitLoaded();

        Assertions.assertTrue(plugin.isStopped());
        Assertions.assertFalse(plugin.loadRan.get());
    }

    @Test
    public void testStopAfterTheLoadStopsAtOnce() throws Exception {
        Loading plugin = loading();
        plugin.init();
        plugin.letTheLoadEnd();
        plugin.awaitLoaded();

        plugin.cleanup();

        Assertions.assertTrue(plugin.isStopped());
    }

    /**
     * {@code RangerBasePlugin.init()} begins with a call to {@code cleanup()}, clearing whatever state the
     * plugin has; made on the loader, that is what it is and not a stop, or every plugin would stop itself
     * the moment it started loading.
     */
    @Test
    public void testTheLoadsOwnCleanupCallIsNotAStop() throws Exception {
        Loading plugin = loading(true, null);
        plugin.init();
        plugin.letTheLoadEnd();
        plugin.awaitLoaded();

        Assertions.assertFalse(plugin.isStopped());
        plugin.cleanup();
        Assertions.assertTrue(plugin.isStopped());
    }

    /**
     * Interrupted while waiting, a check goes on to its question with the interrupt restored; the engine it
     * then asks may still be empty, and that null the caller already reads as a refusal.
     */
    @Test
    public void testInterruptedWaitReturnsWithTheInterruptRestored() throws Exception {
        Loading plugin = loading();
        plugin.init();
        Assertions.assertTrue(plugin.loadStarted.await(10, TimeUnit.SECONDS));

        Thread.currentThread().interrupt();
        try {
            plugin.awaitLoaded();
            Assertions.assertTrue(Thread.interrupted(), "the interrupt was swallowed");
        } finally {
            Thread.interrupted();
        }
        Assertions.assertFalse(plugin.isLoaded());
    }
}
