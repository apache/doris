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

import java.util.Map;

public class RangerDorisAccessControllerFactory implements AuthorizationPluginFactory {
    private static final Logger LOG = LogManager.getLogger(RangerDorisAccessControllerFactory.class);
    private static final String SERVICE_NAME = "doris";

    // Every controller starts a Ranger policy refresher, so all Env instances must share one controller.
    private static RangerDorisAccessController instance;

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
        if (instance == null) {
            instance = new RangerDorisAccessController(SERVICE_NAME, properties, context);
        } else if (!properties.isEmpty()) {
            // There is one refresher and therefore one controller, so the configuration it was built with is
            // the one in force. Say so rather than let a second, differently configured binding look applied.
            LOG.warn("Ranger Doris authorization is already configured; properties {} are ignored, the"
                    + " configuration the source was created with stays in force.", properties.keySet());
        }
        return instance;
    }
}
