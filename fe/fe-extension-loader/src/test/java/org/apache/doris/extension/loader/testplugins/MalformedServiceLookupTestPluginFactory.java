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

package org.apache.doris.extension.loader.testplugins;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import java.util.ServiceLoader;

/**
 * This class defines cleanly and its static initializer runs a nested {@link ServiceLoader} lookup
 * against its own classloader. The test jar registers a provider class that does not exist, so the
 * lookup fails with {@link java.util.ServiceConfigurationError} - an {@link Error} that is neither a
 * {@link LinkageError} nor a {@link RuntimeException}, and the shape a plugin's own driver discovery
 * produces when one of its service files is broken.
 */
public class MalformedServiceLookupTestPluginFactory implements PluginFactory {

    private static final Object FIRST_PROVIDER = ServiceLoader.load(NestedService.class,
            MalformedServiceLookupTestPluginFactory.class.getClassLoader()).iterator().next();

    @Override
    public String name() {
        return "malformed-service-lookup-test";
    }

    @Override
    public String description() {
        return "Plugin whose static initializer runs a malformed nested service lookup: " + FIRST_PROVIDER;
    }

    @Override
    public Plugin create() {
        return new Plugin() {
        };
    }
}
