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

package org.apache.doris.connector.spi;

/**
 * Reads one deployment-level setting for a connector, from the plugin's own {@code <name>.conf} first
 * and from fe.conf second.
 *
 * <p>Two channels exist because the plugin conf file is the newer one. Settings that predate it are
 * declared as {@code @ConfField}s in fe.conf and forwarded through
 * {@link ConnectorContext#getEnvironment()}; those {@code @ConfField}s are kept so that an existing
 * deployment keeps working untouched after an upgrade. The precedence below is what lets both be true
 * at once, and it lives here — in one place — so that the connectors reading these settings cannot
 * drift apart on it.
 *
 * <p>A setting introduced from now on has no fe.conf half: pass {@code null} for
 * {@code legacyEnvKey} and the engine needs no change to carry it.
 */
public final class ConnectorConf {

    private ConnectorConf() {
    }

    /**
     * Resolves {@code key} as: the plugin's {@code <name>.conf}, then fe.conf, then {@code defaultValue}.
     *
     * <p>A value that is absent <em>or blank</em> is "not set" at each step. Blank counts as unset
     * because an operator who writes {@code key=} in a conf file means "I have not configured this",
     * not "configure this to the empty string" — and reading it the other way would let an empty line
     * mask the fe.conf value that is actually in effect.
     *
     * <p>Values are returned as written (the conf file's were trimmed when the file was parsed; fe.conf's
     * are passed through untouched, so a connector sees exactly the string it saw before this channel
     * existed).
     *
     * @param context      the connector's context
     * @param key          the key in {@code <name>.conf}. <b>Not</b> prefixed with the connector's name —
     *                     the file name already namespaces it
     * @param legacyEnvKey the same setting's fe.conf name as forwarded through
     *                     {@link ConnectorContext#getEnvironment()}, or null for a setting that never
     *                     had one
     * @param defaultValue returned when neither channel has the setting; may be null
     */
    public static String get(ConnectorContext context, String key, String legacyEnvKey,
            String defaultValue) {
        String fromConf = context.getConnectorConfig().get(key);
        if (isSet(fromConf)) {
            return fromConf;
        }
        if (legacyEnvKey != null) {
            String fromEnv = context.getEnvironment().get(legacyEnvKey);
            if (isSet(fromEnv)) {
                return fromEnv;
            }
        }
        return defaultValue;
    }

    private static boolean isSet(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
