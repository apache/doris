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

import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RangerHiveAccessControllerFactory implements AuthorizationPluginFactory {
    private static final Logger LOG = LogManager.getLogger(RangerHiveAccessControllerFactory.class);

    /**
     * How long a stack nothing reads is kept before it is stopped.
     *
     * <p>Long enough that ordinary DDL never pays for a rebuild: {@code ALTER CATALOG} and
     * {@code REFRESH CATALOG} both detach a catalog's access controller and attach a new one, and the second
     * half is lazy - it happens on the first statement authorized against the catalog afterwards. Short
     * enough that a mistyped service name, or a catalog that has been dropped, stops polling a Ranger admin
     * in minutes rather than never.
     *
     * <p>Not final so a case can shorten it; nothing in production writes it.
     */
    @VisibleForTesting
    static long idleStackGraceSeconds = 300;

    /** Guards everything below; held only long enough to hand a controller out or account for one going away. */
    private static final Object LOCK = new Object();

    /**
     * One audit stack per Ranger service this source has been asked about, and one entry per configuration a
     * binding asked for.
     *
     * <p>The plugin is what polling a Ranger service costs: it starts a policy refresher and a policy download
     * timer, so a second one over the same service would poll it twice and answer from a second copy of the
     * same policies. It is keyed on the service name because that is the only property deciding which policies
     * this source reads; everything else a binding configures -
     * {@link RangerAccessController#DEFER_TO_GLOBAL_SCOPE_AUTHORITY} - belongs to the controller, which is why
     * a binding configured differently gets a controller of its own over the same stack rather than being
     * refused or quietly served somebody else's configuration.
     *
     * <p>A stack outlives the binding that started it, but not the process. The last binding letting go does
     * not stop it, because a plain {@code ALTER CATALOG} detaches and re-attaches the catalog's access
     * controller and a stack stopped between those two costs a {@code cleanup()} on the DDL thread - it
     * interrupts the policy refresher and joins it without a timeout - and two synchronous admin REST calls
     * on the way back up. What happens instead is that a stack nothing reads any more is stopped after
     * {@link #idleStackGraceSeconds}, and anything asking for that service again in the meantime cancels
     * the stop. Without it the map would grow by one entry, one policy refresher thread and one download
     * timer for every distinct {@code ranger.service.name} this FE was ever asked about - including the ones
     * a rejected {@code CREATE CATALOG} asked about, and including names that resolve to nothing, which log
     * an error to fe.log every thirty seconds for as long as the process lives.
     */
    private static final Map<String, Shared> STACKS_BY_SERVICE = new HashMap<>();
    private static final Map<Map<String, String>, Held> BY_CONFIGURATION = new LinkedHashMap<>();

    /** One audit stack, and the stop scheduled for it while nothing reads it. */
    private static final class Shared {
        private final RangerHiveAuditStack stack;
        private ScheduledFuture<?> pendingStop;

        private Shared(RangerHiveAuditStack stack) {
            this.stack = stack;
        }
    }

    /** One controller and the number of bindings holding it; the last one to let go takes it out. */
    private static final class Held {
        private final RangerHiveAccessController controller;
        private int holders;

        private Held(RangerHiveAccessController controller) {
            this.controller = controller;
            this.holders = 1;
        }
    }

    @Override
    public String name() {
        return RangerHiveAccessController.NAME;
    }

    @Override
    public String description() {
        return "Authorizes one catalog against the policies of a Ranger service of type hive";
    }

    @Override
    public AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context) {
        return acquire(properties, context);
    }

    private static RangerHiveAccessController acquire(Map<String, String> properties,
            AuthorizationContext context) {
        // Settled before anything is started. The controller's constructor is what parses these properties,
        // and it refuses a value that is neither true nor false - so building the stack first would leave a
        // rejected binding behind a policy refresher that no controller, and therefore no release(), can
        // ever reach.
        RangerAccessController.validateProperties(properties);
        Map<String, String> configuration = normalize(properties);
        String serviceName = configuration.get(RangerHiveAccessController.SERVICE_NAME_PROPERTY);

        RangerHiveAuditStack built = null;
        RangerHiveAuditStack toStop = null;
        try {
            while (true) {
                synchronized (LOCK) {
                    Held held = BY_CONFIGURATION.get(configuration);
                    if (held != null) {
                        held.holders++;
                        toStop = built;
                        built = null;
                        return held.controller;
                    }
                    Shared shared = STACKS_BY_SERVICE.get(serviceName);
                    if (shared == null && built != null) {
                        shared = new Shared(built);
                        STACKS_BY_SERVICE.put(serviceName, shared);
                        built = null;
                    } else {
                        // Either something already reads this service, or nothing does and nothing was built
                        // yet. In the first case the one built here lost the race and has to be stopped.
                        toStop = built;
                        built = null;
                    }
                    if (shared != null) {
                        // Something reads this service again, so a stop scheduled when the last binding let
                        // go of it must not run.
                        cancelPendingStop(shared);
                        try {
                            held = new Held(new RangerHiveAccessController(shared.stack, properties, context));
                        } catch (RuntimeException | Error e) {
                            // Nothing else may be reading this stack - this is the binding it was built for.
                            stopUnlessStillRead(serviceName);
                            throw e;
                        }
                        BY_CONFIGURATION.put(configuration, held);
                        LOG.info("Built a Ranger controller for {} on service {} with {}; {} configuration(s)"
                                        + " of this source in use.", RangerHiveAccessController.NAME,
                                serviceName, describe(configuration), BY_CONFIGURATION.size());
                        return held.controller;
                    }
                }
                // Nothing reads this service yet, and starting to read it talks to the Ranger admin twice
                // before it returns: RangerBasePlugin.init() loads the service's roles and its policies
                // synchronously, before the refresher thread starts. Built with no lock held, so that a slow
                // or unreachable admin cannot queue every other binding's create - and close - behind it.
                // Losing the race that opens costs one plugin, stopped in the finally below.
                built = RangerHiveAuditStack.startFor(serviceName);
            }
        } finally {
            // With no lock held, for the reason RangerHiveAuditStack#stop gives.
            if (toStop != null) {
                toStop.stop();
            }
            if (built != null) {
                built.stop();
            }
        }
    }

    /**
     * Gives up one binding's hold on a controller, fencing the controller off once nothing holds it any more.
     *
     * <p>The shared audit stack does not stop here even when this was the last binding reading its service:
     * see {@link #STACKS_BY_SERVICE}, where a stop is scheduled instead. What has to stop here is this
     * controller: a query may still be holding it, and from here it must refuse rather than answer.
     *
     * @return whether this factory owned {@code controller}; false means it was built some other way and its
     *         caller has to stop it itself.
     */
    static boolean release(RangerHiveAccessController controller) {
        if (controller == null) {
            return false;
        }
        synchronized (LOCK) {
            Map<String, String> configuration = configurationOf(controller);
            if (configuration == null) {
                return false;
            }
            Held held = BY_CONFIGURATION.get(configuration);
            if (--held.holders > 0) {
                return true;
            }
            BY_CONFIGURATION.remove(configuration);
            LOG.info("Last binding of configuration {} of {} released; {} configuration(s) of this source"
                            + " still in use.", describe(configuration), RangerHiveAccessController.NAME,
                    BY_CONFIGURATION.size());
            stopUnlessStillRead(configuration.get(RangerHiveAccessController.SERVICE_NAME_PROPERTY));
        }
        // Fence with no lock held: from here nothing reaches the plugin through this controller. A query that
        // is still holding it is refused rather than answered by a controller nothing is bound to.
        controller.fenceOff();
        return true;
    }

    /**
     * Schedules the stop of the stack serving {@code serviceName}, unless a binding still reads it.
     *
     * <p>Scheduled rather than done here for the reason {@link #STACKS_BY_SERVICE} gives - the caller is on
     * the thread that detached a catalog, and a re-attach is usually moments away.
     *
     * <p>Caller holds {@link #LOCK}.
     */
    private static void stopUnlessStillRead(String serviceName) {
        Shared shared = STACKS_BY_SERVICE.get(serviceName);
        if (shared == null || shared.pendingStop != null || isStillRead(serviceName)) {
            return;
        }
        shared.pendingStop = RangerHiveAuditStack.schedule(() -> stopIfStillUnread(serviceName, shared),
                idleStackGraceSeconds, TimeUnit.SECONDS);
        LOG.info("Nothing reads Ranger service {} through {} any more; its policy refresher will be stopped"
                        + " in {}s unless something asks for it again.", serviceName,
                RangerHiveAccessController.NAME, idleStackGraceSeconds);
    }

    /**
     * Takes the stack out and stops it, on the timer thread, unless something asked for that service again.
     *
     * <p>Both conditions are re-read under the lock rather than relied on from scheduling time: a cancel that
     * arrives once this has started running returns false, so this is the side that has to notice.
     */
    private static void stopIfStillUnread(String serviceName, Shared shared) {
        synchronized (LOCK) {
            if (STACKS_BY_SERVICE.get(serviceName) != shared || isStillRead(serviceName)) {
                return;
            }
            STACKS_BY_SERVICE.remove(serviceName);
        }
        // Outside the lock: cleanup() interrupts the policy refresher and joins it without a timeout, and
        // nothing else here may wait on that.
        shared.stack.stop();
        LOG.info("Stopped the Ranger policy refresher of {} on service {}; {} service(s) still polled.",
                RangerHiveAccessController.NAME, serviceName, STACKS_BY_SERVICE.size());
    }

    /** Whether any configuration still in use names {@code serviceName}. Caller holds {@link #LOCK}. */
    private static boolean isStillRead(String serviceName) {
        for (Map<String, String> configuration : BY_CONFIGURATION.keySet()) {
            if (Objects.equals(serviceName,
                    configuration.get(RangerHiveAccessController.SERVICE_NAME_PROPERTY))) {
                return true;
            }
        }
        return false;
    }

    /** How many Ranger services this source is polling right now. */
    @VisibleForTesting
    static int polledServiceCount() {
        synchronized (LOCK) {
            return STACKS_BY_SERVICE.size();
        }
    }

    /** Caller holds {@link #LOCK}. */
    private static void cancelPendingStop(Shared shared) {
        if (shared.pendingStop != null) {
            shared.pendingStop.cancel(false);
            shared.pendingStop = null;
        }
    }

    /** The configuration {@code controller} was built for, or null when this factory did not build it. */
    private static Map<String, String> configurationOf(RangerHiveAccessController controller) {
        for (Map.Entry<Map<String, String>, Held> entry : BY_CONFIGURATION.entrySet()) {
            if (entry.getValue().controller == controller) {
                return entry.getKey();
            }
        }
        return null;
    }

    /** Sorted and null-free, so that two property maps compare on content alone. */
    private static Map<String, String> normalize(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new TreeMap<>(new HashMap<>(properties)));
    }

    private static String describe(Map<String, String> properties) {
        return properties.isEmpty() ? "no properties" : properties.toString();
    }
}
