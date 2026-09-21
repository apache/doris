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

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * The plugin a Doris Ranger source answers out of: a {@link RangerBasePlugin} that loads without holding up
 * whoever built it, and whose requests can carry the groups Ranger keeps for a user.
 *
 * <p><b>The first load runs on a thread of its own.</b> {@code RangerBasePlugin.init()} does not return until
 * it has downloaded the service's roles and its policies from the Ranger admin - and, because the policies
 * arrive with the user store enricher on them (below), the user store as well: three REST calls, bounded
 * only by the plugin's REST timeouts and retries ({@code policy.rest.client.connection.timeoutMs} is two
 * minutes by default, {@code read.timeoutMs} thirty seconds). The plugin behind
 * {@code access_controller_type=ranger-doris} is built in the {@code Env} constructor, on the thread starting
 * the FE, so that used to be time an FE could not start for: a user store the size of an enterprise
 * directory, or an admin that is slow or unreachable, held the whole process. Here {@link #init()} starts
 * that load and returns, and {@link #awaitLoaded()} is where
 * whoever needs its outcome waits - which every answer this plugin gives does first, so a check made before
 * the load has ended is answered after it, never out of an engine that has nothing in it yet. Whatever else
 * the FE does to start runs meanwhile, and the check that pays for Ranger's latency is the first one that
 * needs Ranger, not every process that embeds it.
 *
 * <p>What a check waits for is exactly what the constructor used to guarantee: that the load has
 * <em>ended</em> - with the policies from the admin, or from the local cache when the admin could not be
 * reached, or with nothing at all, in which case the engine is null and {@code RangerAccessController}
 * refuses, as it always has. What the load could not have got anywhere with is not left to it: before the
 * load starts, {@link #init()} does the part of it that needs no admin on the calling thread - the audit
 * subsystem and the admin client, see {@link #preflight()} - so a configuration that fails there (no
 * {@code policy.rest.url}, a REST timeout that is not a number, an audit destination Doris does not ship)
 * fails the constructor with its cause, as it did before the load had a thread of its own; that is what a
 * {@code CREATE CATALOG} dry run and an FE start refuse. A load that <em>threw</em> past that ends like the
 * others, whatever it had installed by then: what {@code RangerBasePlugin.init()} does about an admin it
 * cannot reach is logged and survived inside it, so such a throw is something else - a chained plugin that
 * could not start, a refresher setting only the refresher reads - and this plugin stops what the load did
 * publish, refuses every check, and says so through {@link #isFailed()}, see {@link #load()}. How long that
 * lasts is up to whoever holds the plugin: the factories hand a failed one to no further binding and build a
 * new one in its place, so a catalog bound to it recovers on the next {@code ALTER CATALOG} once the cause
 * is fixed, while the instance-scope source, bound once at start, recovers when the FE is restarted. The
 * wait is not shortened by a timeout of its own: the load is bounded by the REST timeouts the operator
 * already tunes, and answering out of an empty engine before it has ended would be refusing checks the
 * policies are about to allow - and, worse, passing ones a policy written against a group is about to deny.
 * What does cut it short is the caller having no use for the answer any more: a controller closed while
 * its check waits refuses, and {@link #awaitLoaded(BooleanSupplier)} is how it stops waiting.
 *
 * <p>Stopping it is {@link #cleanup()}, as before. Stopped while still loading - a {@code CREATE CATALOG}
 * dry run, the loser of a race in a factory - it finishes the load first and stops itself then, on the
 * loading thread: what is running is a REST call, which an interrupt does not cut short, so waiting for it
 * here would put the whole REST timeout back onto the thread closing a catalog.
 *
 * <p><b>Requests carry Ranger's own groups.</b> Doris has none to offer, and a policy item written against a
 * group matches nothing without them; the store they are read from is asked for with the policies, see
 * {@link RangerUserStoreGroups}.
 *
 * <p>A plugin that is never {@link #init() initialized} - a test's, answering out of overrides of its own -
 * has nothing to wait for, and {@link #awaitLoaded()} returns at once.
 */
public abstract class BackgroundLoadedRangerPlugin extends RangerBasePlugin {
    private static final Logger LOG = LogManager.getLogger(BackgroundLoadedRangerPlugin.class);
    /** How often {@link #awaitLoaded(BooleanSupplier)} asks whether to give up. */
    private static final long GIVE_UP_CHECK_MS = 100;

    /** Released once the first load has ended, however it ended. */
    private final CountDownLatch loaded = new CountDownLatch(1);
    /** The thread running the first load, from the moment {@link #init()} starts it. */
    private final AtomicReference<Thread> loader = new AtomicReference<>();
    /** Whether {@link #cleanup()} has been asked for; read by the loader when the load ends. */
    private volatile boolean stopRequested;
    /** Whether the stop has run. It runs once, from whichever of {@link #cleanup()} and the loader is last. */
    private final AtomicBoolean stopped = new AtomicBoolean();
    /** Whether the first load threw. Every answer is refused then; see {@link #load()}. */
    private volatile boolean failed;

    protected BackgroundLoadedRangerPlugin(String serviceType, String serviceName, String appId) {
        super(serviceType, serviceName, appId);
    }

    /**
     * Starts the first load - {@code RangerBasePlugin.init()}, roles, policies and user store - on a thread
     * of its own, and returns at once. Once per plugin.
     *
     * <p>Not before {@link #preflight()} has passed, on this thread: a configuration the load could not have
     * got anywhere with fails this call with its cause instead of failing the load, and a call this refuses
     * has started nothing - the plugin is as it was before it.
     */
    @Override
    public void init() {
        Thread thread = new Thread(this::load, "RangerPluginLoader(serviceType=" + getServiceType()
                + ", serviceName=" + getServiceName() + ")");
        if (!loader.compareAndSet(null, thread)) {
            throw new IllegalStateException("Ranger plugin for service " + getServiceName()
                    + " has already been initialized");
        }
        try {
            preflight();
        } catch (RuntimeException | Error e) {
            // Nothing has been started, so nothing is waited for: a plugin this leaves behind - none in
            // production, where the constructor throws it away with the exception - has no load to end.
            loader.set(null);
            throw e;
        }
        LOG.info(RangerUserStoreGroups.describe(getConfig()));
        // A daemon: it ends with the load, and a load still running when the FE exits is not worth waiting
        // for. What it inherits, and what the refresher threads it goes on to start inherit from it, is the
        // context classloader of the thread calling this - the plugin's own, set by the engine around the
        // factory call - which is what keeps Ranger's class-name lookups working after that call returns.
        thread.setDaemon(true);
        thread.start();
    }

    private void load() {
        long startedAtNanos = System.nanoTime();
        try {
            if (stopRequested) {
                // Stopped before the load began; there is nothing to load for.
                return;
            }
            firstLoad();
            LOG.info("Ranger service {} loaded in {} ms: policies version {}, roles version {}, user store"
                            + " version {}", getServiceName(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos), getPoliciesVersion(),
                    getRolesVersion(), getUserStoreVersion());
            if (getPolicyEngine() != null && RangerUserStoreGroups.enabledFor(getConfig())
                    && getUserStoreVersion() < 0) {
                // Policies to answer out of, but no user store to read groups from: the admin could not be
                // reached for it and none was cached. Requests carry no groups until one arrives - the
                // enricher keeps asking - which is what this source sent before groups were attached at all,
                // and which a policy item written against a group, a deny included, does not match.
                LOG.warn("Ranger service {} loaded its policies but no user store: until one arrives,"
                        + " requests carry no groups and policy items written against a group do not apply",
                        getServiceName());
            }
        } catch (Throwable e) {
            // Everything RangerBasePlugin.init() does about an admin it cannot reach is logged and survived
            // inside it, and what the load could not have got anywhere with the preflight refused before it
            // started, so this is something else - a chained plugin that could not start, a refresher
            // setting only the refresher reads. It used to fail the FE's start, or the CREATE CATALOG; now
            // it fails every check against this plugin, which RangerAccessController reports on each one as
            // an engine that is not initialized, for as long as the plugin is held: the factories build a
            // new one for the next binding of a source whose plugin has failed (isFailed), which is how a
            // fixed configuration reaches a catalog - through ALTER CATALOG - and does not reach the
            // instance-scope source, bound once at start, short of a restart. Refusing takes both of the
            // following: the flag, read by every answer, and stopping what the load had installed before it
            // threw - RangerBasePlugin.init() publishes the policy engine, refresher and all, before it
            // initializes the chained plugins, and left running that engine would answer checks with part
            // of the configured authorization missing, whatever the line below says.
            failed = true;
            LOG.error("Ranger service {} failed to load; every check against it is refused. Once the cause"
                    + " is fixed, ALTER CATALOG binds a catalog governed by it to a new plugin; an FE governed"
                    + " by it through access_controller_type has to be restarted", getServiceName(), e);
            stopOnce();
        } finally {
            loaded.countDown();
            if (stopRequested) {
                stopOnce();
            }
        }
    }

    /**
     * The load itself: {@code RangerBasePlugin.init()}, which downloads everything this plugin answers out
     * of before it returns. A test's plugin overrides it to stand in for the Ranger admin.
     */
    protected void firstLoad() {
        super.init();
    }

    /**
     * The part of the load that needs no Ranger admin, done on the thread calling {@link #init()} before the
     * load starts: the audit subsystem, when this is the first plugin in the process to need it, and the
     * admin client the load goes on to poll through. Both are what {@code RangerBasePlugin.init()} does
     * first and would otherwise do on the loader, and both fail on configuration alone - the client refuses
     * an empty {@code policy.rest.url} and a {@code policy.rest.client.*} setting that is not a number, the
     * audit factory a destination Doris does not ship - so done here they fail whoever is building the
     * plugin, with the cause, the way the constructor did before the load had a thread of its own. Neither
     * dials anything: the client opens its connection on first use, and it is kept in the plugin context,
     * where the refresher the load creates finds it rather than building a second one.
     *
     * <p>A test plugin standing in for the admin, whose {@link #firstLoad()} never polls, needs a URL for
     * this all the same, or overrides it.
     */
    protected void preflight() {
        AuditProviderFactory auditProviderFactory = AuditProviderFactory.getInstance();
        if (!auditProviderFactory.isInitDone() && getConfig().getProperties() != null) {
            auditProviderFactory.init(getConfig().getProperties(), getAppId());
        }
        getPluginContext().createAdminClient(getConfig());
    }

    /**
     * Waits until the first load has ended, if one was started; see the class comment for what that means.
     *
     * <p>Interrupted, it returns with the interrupt restored rather than throwing: what follows it is a
     * question to an engine that may not be there yet, whose null answer the caller already reads as a
     * refusal.
     */
    public void awaitLoaded() {
        if (loader.get() == null || isLoaded()) {
            return;
        }
        try {
            loaded.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * As {@link #awaitLoaded()}, for a caller that may stop needing the answer while it waits: returns once
     * the load has ended or {@code giveUp} says so, whichever is first, the latter asked every
     * {@value #GIVE_UP_CHECK_MS} ms. A controller closed while its check waits is such a caller - it
     * refuses, and the load it was waiting for goes on for whoever else holds the plugin.
     */
    public void awaitLoaded(BooleanSupplier giveUp) {
        if (loader.get() == null || isLoaded()) {
            return;
        }
        try {
            boolean ended = false;
            while (!ended && !giveUp.getAsBoolean()) {
                ended = loaded.await(GIVE_UP_CHECK_MS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Whether the first load has ended - not whether it found anything, see {@link #awaitLoaded()} - and so
     * false for a plugin that was never initialized, which has no load to end.
     */
    public boolean isLoaded() {
        return loaded.getCount() == 0;
    }

    /**
     * Whether the first load threw, after which every answer is a refusal and the plugin has stopped itself;
     * see {@link #load()}. A factory sharing this plugin reads it on the way in, to hand a failed one to
     * nobody else and build a new one instead.
     */
    public boolean isFailed() {
        return failed;
    }

    /**
     * Stops the plugin's threads, or - while the first load is still running - arranges for the loader to
     * stop them once it has ended, and returns at once.
     */
    @Override
    public void cleanup() {
        if (Thread.currentThread() == loader.get()) {
            // RangerBasePlugin.init() begins by clearing whatever state a plugin has, through this method; on
            // the loader that is what a call to it is, not a stop.
            super.cleanup();
            return;
        }
        stopRequested = true;
        if (loader.get() == null || isLoaded()) {
            stopOnce();
        }
        // Otherwise the loader stops the plugin when the load ends, see load(): between its countDown and its
        // read of stopRequested, and this method's write of stopRequested and its read of the latch, at
        // least one side sees the other's write, and stopOnce keeps it to one stop.
    }

    private void stopOnce() {
        if (stopped.compareAndSet(false, true)) {
            super.cleanup();
        }
    }

    @VisibleForTesting
    boolean isStopped() {
        return stopped.get();
    }

    /**
     * Takes the policies Ranger downloaded, and asks for the user store with them, so that the requests the
     * source over this plugin builds can carry the groups Ranger keeps for a user; see
     * {@link RangerUserStoreGroups}. Done on the way in rather than left to Ranger, because Ranger only does
     * it on its own from 2.5 on, and behind a property.
     */
    @Override
    public void setPolicies(ServicePolicies policies) {
        RangerUserStoreGroups.addUserStoreEnricher(getConfig(), policies);
        super.setPolicies(policies);
    }

    // Every answer waits for the first load to end, and a load that failed is answered with null - what
    // RangerBasePlugin answers with no engine, and what every caller reads as a refusal. These four are the
    // ones Doris asks; the one-argument isAccessAllowed overloads arrive here through RangerBasePlugin.

    @Override
    public RangerAccessResult isAccessAllowed(RangerAccessRequest request,
            RangerAccessResultProcessor resultProcessor) {
        awaitLoaded();
        return failed ? null : super.isAccessAllowed(request, resultProcessor);
    }

    @Override
    public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests,
            RangerAccessResultProcessor resultProcessor) {
        awaitLoaded();
        return failed ? null : super.isAccessAllowed(requests, resultProcessor);
    }

    @Override
    public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
            RangerAccessResultProcessor resultProcessor) {
        awaitLoaded();
        return failed ? null : super.evalDataMaskPolicies(request, resultProcessor);
    }

    @Override
    public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request,
            RangerAccessResultProcessor resultProcessor) {
        awaitLoaded();
        return failed ? null : super.evalRowFilterPolicies(request, resultProcessor);
    }
}
