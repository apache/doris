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

package org.apache.doris.extension.loader;

/**
 * Validation rules for self-reported plugin names.
 *
 * <p>A plugin name is a configuration-facing identity key, so it is validated
 * at load time: non-blank after trimming, bounded length, restricted charset.
 * An invalid name is treated as a load failure by the loading side.
 */
final class PluginNames {

    static final int MAX_LENGTH = 64;

    private PluginNames() {
    }

    /**
     * Validates a self-reported plugin name.
     *
     * @param name raw value returned by {@code PluginFactory.name()}
     * @return null if valid, otherwise a human-readable rejection reason
     */
    static String validate(String name) {
        if (name == null) {
            return "plugin name is null";
        }
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            return "plugin name is blank";
        }
        if (trimmed.length() > MAX_LENGTH) {
            return "plugin name exceeds " + MAX_LENGTH + " characters";
        }
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == '-';
            if (!allowed) {
                return "plugin name contains illegal character '" + c
                        + "', allowed: letters, digits, '.', '_', '-'";
            }
        }
        return null;
    }
}
