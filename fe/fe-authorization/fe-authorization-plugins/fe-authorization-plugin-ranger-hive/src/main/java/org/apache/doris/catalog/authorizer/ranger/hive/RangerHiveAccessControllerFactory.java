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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class RangerHiveAccessControllerFactory implements AuthorizationPluginFactory {
    private static final Logger LOG = LogManager.getLogger(RangerHiveAccessControllerFactory.class);
    private static final String SERVICE_NAME_PROPERTY = "ranger.service.name";

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
     * <p>Once started a stack runs until the process ends, and the last binding letting go does not stop it.
     * It holds no per-binding state, so what an idle one costs is one policy download timer and one audit
     * flush task against one Ranger service - bounded, not a leak that grows. Stopping it would cost far more:
     * a plain {@code ALTER CATALOG} detaches and re-attaches the catalog's access controller, and a plugin
     * torn down and rebuilt between those two has no policies until its next download completes, so every
     * check against that catalog is refused and both data policy paths throw for the whole of that window -
     * with the teardown paid on the DDL thread, where {@code cleanup()} interrupts the refresher and joins it
     * without a timeout.
     */
    private static final Map<String, RangerHiveAuditStack> STACKS_BY_SERVICE = new HashMap<>();
    private static final Map<Map<String, String>, Held> BY_CONFIGURATION = new LinkedHashMap<>();

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
        synchronized (LOCK) {
            Map<String, String> configuration = normalize(properties);
            Held held = BY_CONFIGURATION.get(configuration);
            if (held != null) {
                held.holders++;
                return held.controller;
            }
            // Settle what this binding configured before anything is started. The controller's constructor is
            // what parses these properties, and it refuses a value that is neither true nor false - so
            // building the stack first would leave a rejected binding behind a policy refresher that no
            // controller, and therefore no release(), can ever reach.
            RangerAccessController.validateProperties(properties);
            String serviceName = configuration.get(SERVICE_NAME_PROPERTY);
            RangerHiveAuditStack stack = STACKS_BY_SERVICE.get(serviceName);
            if (stack == null) {
                stack = RangerHiveAuditStack.startFor(serviceName);
                STACKS_BY_SERVICE.put(serviceName, stack);
            }
            held = new Held(new RangerHiveAccessController(stack, properties, context));
            BY_CONFIGURATION.put(configuration, held);
            LOG.info("Built a Ranger controller for {} on service {} with {}; {} configuration(s) of this"
                            + " source in use.", RangerHiveAccessController.NAME, serviceName,
                    describe(configuration), BY_CONFIGURATION.size());
            return held.controller;
        }
    }

    /**
     * Gives up one binding's hold on a controller, fencing the controller off once nothing holds it any more.
     *
     * <p>The shared audit stack stays up either way - see {@link #STACKS_BY_SERVICE} for why the alternative is
     * worse than an idle policy download timer. What has to stop here is this controller: a query may still be
     * holding it, and from here it must refuse rather than answer.
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
        }
        // Fence with no lock held: from here nothing reaches the plugin through this controller. A query that
        // is still holding it is refused rather than answered by a controller nothing is bound to.
        controller.fenceOff();
        return true;
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
