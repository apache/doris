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
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class RangerDorisAccessControllerFactory implements AuthorizationPluginFactory {
    private static final Logger LOG = LogManager.getLogger(RangerDorisAccessControllerFactory.class);
    private static final String SERVICE_NAME = "doris";

    /** Guards everything below; held only long enough to hand a controller out or account for one going away. */
    private static final Object LOCK = new Object();

    /**
     * The Ranger plugin every binding of this source reads policies out of, and one entry per configuration a
     * binding asked for.
     *
     * <p>The plugin is what polling the Ranger service costs: it starts a policy refresher and a policy
     * download timer, so a second one would poll the same service twice and answer from a second copy of the
     * same policies. Everything a binding configures - {@code ranger.defer_to_global_scope_authority} - belongs
     * to the controller instead, which is why a binding configured differently gets a controller of its own
     * over that same plugin rather than being refused or quietly served somebody else's configuration.
     *
     * <p>Bindings configured alike still share one controller, so a catalog bound to this source while it also
     * governs the instance is the identical object in both places - which is what lets the engine recognise
     * that asking about instance scope would be asking this very source.
     */
    private static RangerBasePlugin sharedPlugin;
    private static final Map<Map<String, String>, Held> byConfiguration = new LinkedHashMap<>();

    /** One controller and the number of bindings holding it; the last one to let go takes it out. */
    private static final class Held {
        private final RangerDorisAccessController controller;
        private int holders;

        private Held(RangerDorisAccessController controller) {
            this.controller = controller;
            this.holders = 1;
        }
    }

    @Override
    public String name() {
        return RangerDorisAccessController.NAME;
    }

    @Override
    public String description() {
        return "Authorizes against the policies of a Ranger service of type " + SERVICE_NAME;
    }

    @Override
    public AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context) {
        return acquire(properties, context);
    }

    private static RangerDorisAccessController acquire(Map<String, String> properties,
            AuthorizationContext context) {
        synchronized (LOCK) {
            Map<String, String> configuration = normalize(properties);
            Held held = byConfiguration.get(configuration);
            if (held != null) {
                held.holders++;
                return held.controller;
            }
            if (sharedPlugin == null) {
                sharedPlugin = new RangerDorisPlugin(SERVICE_NAME);
            }
            held = new Held(new RangerDorisAccessController(sharedPlugin, properties, context));
            byConfiguration.put(configuration, held);
            LOG.info("Built a Ranger controller for {} with {}; {} configuration(s) of this source in use.",
                    RangerDorisAccessController.NAME, describe(configuration), byConfiguration.size());
            return held.controller;
        }
    }

    /**
     * Gives up one binding's hold on a controller, fencing it off and stopping the shared Ranger plugin once
     * nothing holds it any more.
     *
     * @return whether this factory owned {@code controller}; false means it was built some other way and its
     *         caller has to stop it itself.
     */
    static boolean release(RangerDorisAccessController controller) {
        if (controller == null) {
            return false;
        }
        RangerBasePlugin pluginToStop;
        synchronized (LOCK) {
            Map<String, String> configuration = configurationOf(controller);
            if (configuration == null) {
                return false;
            }
            Held held = byConfiguration.get(configuration);
            if (--held.holders > 0) {
                return true;
            }
            byConfiguration.remove(configuration);
            // The plugin outlives any single controller: it goes only when no configuration is left in use.
            pluginToStop = byConfiguration.isEmpty() ? sharedPlugin : null;
            if (pluginToStop != null) {
                sharedPlugin = null;
            }
        }
        // Fence first, and with no lock held: from here nothing reaches the plugin through this controller,
        // whether or not the plugin itself is going away. A query that is still holding it is refused rather
        // than answered out of a plugin being cleaned up.
        controller.fenceOff();
        if (pluginToStop != null) {
            LOG.info("Last binding of {} released; shutting the Ranger controller down.",
                    RangerDorisAccessController.NAME);
            try {
                pluginToStop.cleanup();
            } catch (Throwable e) {
                LOG.warn("Failed to clean up the Ranger Doris plugin", e);
            }
        }
        return true;
    }

    /** The configuration {@code controller} was built for, or null when this factory did not build it. */
    private static Map<String, String> configurationOf(RangerDorisAccessController controller) {
        for (Map.Entry<Map<String, String>, Held> entry : byConfiguration.entrySet()) {
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
