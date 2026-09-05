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

package org.apache.doris.authorizationexample;

import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;

import java.util.Map;

/**
 * Publishes {@link ExampleAuthorizationPlugin} under the name configuration selects it by.
 *
 * <p>A jar becomes a plugin by naming a class like this one in
 * {@code META-INF/services/org.apache.doris.authorization.spi.AuthorizationPluginFactory}. That file is the
 * whole of the discovery mechanism; there is nothing to register and nothing to declare in {@code fe.conf}
 * beyond the name below.
 *
 * <p>The factory exists separately from the plugin because the plugin needs two things that only exist at
 * the moment the engine builds it: whatever configures this source, and the {@link AuthorizationContext} it
 * puts questions to the engine through.
 */
public class ExampleAuthorizationPluginFactory implements AuthorizationPluginFactory {

    /**
     * What to write in {@code fe.conf} as {@code access_controller_type} to have this source govern the
     * whole instance, or in a catalog's {@code "access_controller.class"} to have it govern that catalog.
     */
    public static final String NAME = "example-authz";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Worked example of an authorization source: privileges by Doris role, plus a row filter";
    }

    @Override
    public AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context) {
        return new ExampleAuthorizationPlugin(properties, context);
    }
}
