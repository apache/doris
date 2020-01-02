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

package org.apache.doris.plugin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.doris.common.UserException;
import org.apache.kudu.client.shaded.com.google.common.collect.Lists;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class PluginMgr {
    private final List<Map<String, PluginLoader>> plugins = Lists.newArrayListWithCapacity(PluginType.MAX_PLUGIN_SIZE);

    private String pluginDir;

    public PluginMgr(String pluginDir) {
        for (int i = 0; i < plugins.size(); i++) {
            plugins.add(Maps.newConcurrentMap());
        }

        this.pluginDir = pluginDir;
    }

    /**
     * Dynamic install plugin thought install statement
     */
    public void installPlugin(String pluginSource) throws IOException, UserException {
        PluginLoader pluginLoader = new DynamicPluginLoader(pluginDir, pluginSource);

        PluginContext ctx = pluginLoader.getPluginContext();

        {
            // already install
            PluginLoader oldRef = plugins.get(ctx.getType().ordinal()).get(ctx.getName());
            if (oldRef != null) {
                throw new UserException(
                        "plugin " + ctx.getName() + " has install version " + oldRef.getPluginContext().getVersion());
            }
        }

        {
            // check & write meta
            // ...
        }

        // install plugin
        pluginLoader.install();

        {
            PluginLoader checkRef = plugins.get(ctx.getType().ordinal()).putIfAbsent(ctx.getName(), pluginLoader);

            if (!pluginLoader.equals(checkRef)) {
                pluginLoader.uninstall();
                throw new UserException(
                        "plugin " + ctx.getName() + " has install version " + checkRef.getPluginContext().getVersion());
            }
        }
    }

    /**
     * Dynamic uninstall plugin thought install statement
     */
    public void uninstallPlugin(String pluginName) throws IOException, UserException {
        for (PluginType type : PluginType.values()) {
            if (plugins.get(type.ordinal()).containsKey(pluginName)) {
                PluginLoader ref = plugins.get(type.ordinal()).remove(pluginName);

                {
                    // check & update meta
                    // ...
                }

                // uninstall plugin
                ref.uninstall();
                return;
            }
        }
    }

    /**
     * For built-in Plugin register
     */
    public boolean registerPlugin(PluginContext pluginContext, Plugin plugin) {
        if (Objects.isNull(pluginContext) || Objects.isNull(plugin) || Objects.isNull(pluginContext.getType()) || Strings
                .isNullOrEmpty(pluginContext.getName())) {
            return false;
        }

        PluginLoader ref = new BuiltinPluginLoader(pluginDir,pluginContext, plugin);
        PluginLoader checkRef = plugins.get(pluginContext.getType().ordinal()).putIfAbsent(pluginContext.getName(), ref);

        return ref.equals(checkRef);
    }

    public final Plugin getPlugin(String name, PluginType type) {
        PluginLoader ref = plugins.get(type.ordinal()).get(name);

        if (null != ref) {
            return ref.getPlugin();
        }

        return null;
    }

    public final List<Plugin> getPluginList(PluginType type) {
        Map<String, PluginLoader> m = plugins.get(type.ordinal());

        List<Plugin> l = Lists.newArrayListWithCapacity(m.size());
        for (PluginLoader ref : m.values()) {
            l.add(ref.getPlugin());
        }

        return Collections.unmodifiableList(l);
    }

}
