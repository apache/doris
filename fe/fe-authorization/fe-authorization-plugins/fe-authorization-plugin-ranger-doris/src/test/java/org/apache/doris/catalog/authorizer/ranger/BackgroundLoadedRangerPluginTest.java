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
 * is still being asked - having first refused, on the spot, a configuration the load could not have got
 * anywhere with - nothing is answered before the admin has, and a stop while that is going on is honoured
 * once it is over. The admin stands in for is a latch this test holds.
 */
public class BackgroundLoadedRangerPluginTest {

    /**
     * Where the admin would be. Never dialed: the load below never polls, and of the preflight only the admin
     * client and the configuration reads run for real here - the audit subsystem is a singleton of the JVM
     * and the doubles leave it alone, see {@link Loading#initializeAudit}.
     */
    private static final String ADMIN_URL_PROPERTY = "ranger.plugin.test.policy.rest.url";
    private static final String ADMIN_URL = "http://ranger.invalid:6080";

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
            // Service type "test" reads ranger-test-*.xml, none of which exist here. The one thing init()
            // insists on knowing about the admin before it starts the load is where it is - the client it
            // builds for the load refuses to exist without a URL - and with that, the preflight runs for
            // real, but for the audit subsystem.
            super("test", "test", null);
            getConfig().set(ADMIN_URL_PROPERTY, ADMIN_URL);
            this.clearsStateFirst = clearsStateFirst;
            this.userStore = userStore;
            this.publishesThenThrows = publishesThenThrows;
            if (publishesThenThrows) {
                // The engine built below would otherwise ask a Ranger admin for the user store; there is none.
                getConfig().set("ranger.plugin.test.use.rangerGroups", "false");
            }
        }

        /**
         * Left alone: Ranger's audit subsystem is a singleton of the JVM, initialized once and for good, so a
         * double that initialized it would decide, for every test after it, what the audit configuration of
         * the process is. What the preflight does about it is exercised through {@link AuditFailing}.
         */
        @Override
        protected void initializeAudit(boolean again) {
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

    /**
     * A plugin whose audit subsystem refuses to initialize a given number of times - what a
     * {@code ranger-<type>-audit.xml} naming a destination Doris does not ship does, through Ranger's
     * {@code AuditProviderFactory} - and records whether it was asked to try again after an earlier failure.
     */
    private static final class AuditFailing extends BackgroundLoadedRangerPlugin {
        private int failuresLeft;
        private Boolean askedToTryAgain;
        private final AtomicBoolean loadRan = new AtomicBoolean();

        private AuditFailing(int failures) {
            super("test", "test", null);
            getConfig().set(ADMIN_URL_PROPERTY, ADMIN_URL);
            this.failuresLeft = failures;
        }

        @Override
        protected void initializeAudit(boolean again) {
            askedToTryAgain = again;
            if (failuresLeft-- > 0) {
                throw new RuntimeException("Failed to create AuditDestination for class: solr");
            }
        }

        @Override
        protected void firstLoad() {
            loadRan.set(true);
        }
    }

    private Loading plugin;

    @AfterEach
    public void forgetAnyAuditFailure() {
        BackgroundLoadedRangerPlugin.forgetAuditInitFailure();
    }

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

    /**
     * What the load could not have got anywhere with is refused by init() itself, on the calling thread and
     * with the cause - which is what a CREATE CATALOG dry run and an FE start see, as they did when the
     * constructor ran the whole load. A call this refuses has started nothing.
     */
    @Test
    public void testAConfigurationTheLoadCouldNotUseIsRefusedBeforeItStarts() throws Exception {
        Loading noAdminUrl = loading();
        noAdminUrl.getConfig().unset(ADMIN_URL_PROPERTY);
        IllegalArgumentException refused = Assertions.assertThrows(IllegalArgumentException.class,
                noAdminUrl::init);
        Assertions.assertTrue(refused.getMessage().contains("Ranger URL"), refused.getMessage());

        Loading malformedTimeout = loading();
        malformedTimeout.getConfig().set("ranger.plugin.test.policy.rest.client.read.timeoutMs", "soon");
        Assertions.assertThrows(NumberFormatException.class, malformedTimeout::init);

        Loading malformedPollInterval = loading();
        malformedPollInterval.getConfig().set("ranger.plugin.test.policy.pollIntervalMs", "often");
        Assertions.assertThrows(NumberFormatException.class, malformedPollInterval::init);

        for (Loading plugin : new Loading[] {noAdminUrl, malformedTimeout, malformedPollInterval}) {
            Assertions.assertFalse(plugin.loadRan.get(), "the load was started for a refused configuration");
            Assertions.assertFalse(plugin.isLoaded());
            // Nothing was started, so there is nothing to wait for.
            within(CompletableFuture.runAsync(plugin::awaitLoaded), 10);
        }

        // As it was before the refused call: with the configuration fixed, the same plugin starts its load.
        noAdminUrl.getConfig().set(ADMIN_URL_PROPERTY, ADMIN_URL);
        noAdminUrl.init();
        Assertions.assertTrue(noAdminUrl.loadStarted.await(10, TimeUnit.SECONDS));
        noAdminUrl.letTheLoadEnd();
        noAdminUrl.awaitLoaded();
        Assertions.assertTrue(noAdminUrl.isLoaded());
    }

    /**
     * An audit subsystem that cannot be initialized refuses the plugin with the cause, before the load and
     * without the admin client's errors having been able to hide it - and, because Ranger marks the subsystem
     * done before it does the work, the next plugin built tries again rather than running without audit.
     */
    @Test
    public void testAnAuditSubsystemThatCannotBeInitializedIsRefusedAndTriedAgain() {
        AuditFailing first = new AuditFailing(1);
        IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class, first::init);
        Assertions.assertTrue(refused.getMessage().contains("ranger-test-audit.xml"), refused.getMessage());
        Assertions.assertTrue(refused.getMessage().contains("AuditDestination"), refused.getMessage());
        Assertions.assertEquals(Boolean.FALSE, first.askedToTryAgain);
        Assertions.assertFalse(first.loadRan.get(), "the load was started for a refused configuration");

        // Still broken: refused again, and asked as a retry - Ranger would otherwise say it is done.
        AuditFailing second = new AuditFailing(1);
        Assertions.assertThrows(IllegalStateException.class, second::init);
        Assertions.assertEquals(Boolean.TRUE, second.askedToTryAgain);
        Assertions.assertFalse(second.loadRan.get());

        // Fixed: the retry succeeds and the load starts; the one after it is not a retry any more.
        AuditFailing fixed = new AuditFailing(0);
        fixed.init();
        Assertions.assertEquals(Boolean.TRUE, fixed.askedToTryAgain);
        fixed.awaitLoaded();
        Assertions.assertTrue(fixed.loadRan.get());
        AuditFailing next = new AuditFailing(0);
        next.init();
        Assertions.assertEquals(Boolean.FALSE, next.askedToTryAgain);
        next.awaitLoaded();
    }

    /**
     * The admin client is the preflight's, built before the load starts and kept where the load's refresher
     * finds it: what an empty URL fails is this, on the calling thread, not something on the loader.
     */
    @Test
    public void testTheAdminClientIsBuiltBeforeTheLoad() throws Exception {
        Loading plugin = loading();
        Assertions.assertNull(plugin.getPluginContext().getAdminClient());

        plugin.init();

        Assertions.assertNotNull(plugin.getPluginContext().getAdminClient(), "left to the load");
        Assertions.assertFalse(plugin.isLoaded(), "the load had ended by the time init() returned");
        plugin.letTheLoadEnd();
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
