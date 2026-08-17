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

package org.apache.doris.connectorconf.testplugins;

import org.apache.doris.connector.ConfProbeSink;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * A connector provider whose class bytes are copied into a temporary plugin jar, so a test can load it the
 * way FE loads a shipped connector and see what {@code getConnectorConfig()} delivered.
 *
 * <p>It lives outside {@code org.apache.doris.connector.} on purpose: that prefix is parent-first, and the
 * loader reads a plugin's declared API version from the jar that <em>defines</em> its factory class — a
 * parent-first provider would always look undeclared and be refused. It reports through
 * {@link ConfProbeSink}, which <em>is</em> parent-first and therefore shared with the test.
 *
 * <p>{@link ConfProbeConnectorProviderB} is its twin, deployed as a second plugin, so that a test can prove
 * one plugin's conf never reaches the other.
 */
public class ConfProbeConnectorProviderA implements ConnectorProvider {

    public static final String TYPE = "conf_probe_a";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validatePropertiesForUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) {
        ClassLoader providerLoader = getClass().getClassLoader();
        try {
            Class<?> helper = Class.forName(
                    AlterValidationHelper.class.getName(), true,
                    Thread.currentThread().getContextClassLoader());
            if (helper.getClassLoader() != providerLoader) {
                throw new IllegalStateException("ALTER helper was resolved outside the plugin classloader");
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("ALTER helper was not visible to the plugin", e);
        }
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return ConfProbeSink.record(TYPE, context.getConnectorConfig());
    }
}
