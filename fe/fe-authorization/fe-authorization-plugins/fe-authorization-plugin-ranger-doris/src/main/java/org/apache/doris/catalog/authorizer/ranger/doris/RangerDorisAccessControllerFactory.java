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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class RangerDorisAccessControllerFactory implements AuthorizationPluginFactory {
    private static final Logger LOG = LogManager.getLogger(RangerDorisAccessControllerFactory.class);
    private static final String SERVICE_NAME = "doris";

    /**
     * The one controller every binding of this source shares, and the configuration it was built with.
     *
     * <p>Shared because each controller starts a Ranger policy refresher and a policy download timer against
     * the same service: a second controller would poll the same policies twice and answer from a second copy
     * of them.
     *
     * <p>Sharing means the configuration cannot be per binding, and this source has a configuration that
     * decides who may read what - {@code ranger.defer_to_global_scope_authority} is read once, in the
     * constructor. So a binding whose configuration is not the one in force is refused rather than quietly
     * served the other one's: an operator who switched the deference off for one catalog must not get it back
     * on because another catalog was created first.
     */
    private static RangerDorisAccessController instance;
    private static Map<String, String> instanceProperties;
    /** How many bindings hold {@link #instance}; the last one to let go shuts it down. */
    private static int holders;

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
        return singleton(properties, context);
    }

    private static synchronized RangerDorisAccessController singleton(Map<String, String> properties,
            AuthorizationContext context) {
        Map<String, String> wanted = normalize(properties);
        if (instance == null) {
            instance = new RangerDorisAccessController(SERVICE_NAME, properties, context);
            instanceProperties = wanted;
            holders = 1;
            return instance;
        }
        if (!instanceProperties.equals(wanted)) {
            throw new IllegalArgumentException("Ranger Doris authorization is already configured with "
                    + describe(instanceProperties) + " and there is one controller for the whole FE, so a"
                    + " second configuration cannot be applied. This binding asks for "
                    + describe(wanted) + ". Give every binding of '" + RangerDorisAccessController.NAME
                    + "' the same access_controller.properties.*, or bind only one.");
        }
        holders++;
        return instance;
    }

    /**
     * Gives up one binding's hold on the shared controller.
     *
     * @return whether this factory owned {@code controller}; false means it was built some other way and its
     *         caller has to shut it down itself.
     */
    static synchronized boolean release(RangerDorisAccessController controller) {
        if (controller == null || controller != instance) {
            return false;
        }
        if (--holders > 0) {
            return true;
        }
        RangerDorisAccessController released = instance;
        instance = null;
        instanceProperties = null;
        LOG.info("Last binding of {} released; shutting the Ranger controller down.",
                RangerDorisAccessController.NAME);
        released.shutdown();
        return true;
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
