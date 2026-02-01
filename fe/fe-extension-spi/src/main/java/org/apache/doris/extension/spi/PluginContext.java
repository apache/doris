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

package org.apache.doris.extension.spi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Runtime context passed to plugins.
 */
public final class PluginContext {

    private final PluginDescriptor descriptor;
    private final Map<String, String> properties;

    public PluginContext(PluginDescriptor descriptor, Map<String, String> properties) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.properties = properties != null
                ? Collections.unmodifiableMap(new HashMap<>(properties))
                : Collections.emptyMap();
    }

    public PluginDescriptor getDescriptor() {
        return descriptor;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
