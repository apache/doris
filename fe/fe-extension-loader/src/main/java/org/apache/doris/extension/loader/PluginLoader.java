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

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginDescriptor;
import org.apache.doris.extension.spi.PluginException;
import org.apache.doris.extension.spi.PluginFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Skeleton loader for Doris FE plugins.
 *
 * <p>This class wires classloading and factory discovery but does not perform IO.</p>
 */
public final class PluginLoader {

    private final List<String> parentFirstPackages;

    public PluginLoader(List<String> parentFirstPackages) {
        this.parentFirstPackages = parentFirstPackages != null
                ? new ArrayList<>(parentFirstPackages)
                : ChildFirstClassLoader.DEFAULT_PARENT_FIRST_PACKAGES;
    }

    public ClassLoader createClassLoader(URL[] urls, ClassLoader parent) {
        return new ChildFirstClassLoader(urls, parent, parentFirstPackages);
    }

    public PluginFactory loadFactory(PluginDescriptor descriptor, ClassLoader classLoader) {
        ServiceLoader<PluginFactory> loader = ServiceLoader.load(PluginFactory.class, classLoader);
        for (PluginFactory factory : loader) {
            if (factory.getClass().getName().equals(descriptor.getFactoryClass())) {
                return factory;
            }
        }
        throw new PluginException("No PluginFactory found for: " + descriptor.getFactoryClass());
    }

    public Plugin loadPlugin(PluginDescriptor descriptor, ClassLoader classLoader) {
        PluginFactory factory = loadFactory(descriptor, classLoader);
        Plugin plugin = factory.create(descriptor);
        plugin.initialize(descriptor);
        return plugin;
    }
}
