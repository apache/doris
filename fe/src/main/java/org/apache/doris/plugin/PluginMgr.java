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
    private final List<Map<String, PluginRef>> plugins = Lists.newArrayListWithCapacity(PluginType.MAX_PLUGIN_SIZE);

    private PluginLoader pluginLoader;

    public PluginMgr(String pluginDir) {
        for (int i = 0; i < plugins.size(); i++) {
            plugins.add(Maps.newConcurrentMap());
        }

        pluginLoader = new PluginLoader(pluginDir);
    }

    /**
     * Dynamic install plugin thought install statement
     */
    public void installPlugin(String pluginSource) throws IOException, UserException {
        PluginInfo info = pluginLoader.readPluginInfo(pluginSource);

        {
            // already install
            PluginRef oldRef = plugins.get(info.getType().ordinal()).get(info.getName());
            if (oldRef != null) {
                throw new UserException(
                        "plugin " + info.getName() + " has install version " + oldRef.getPluginInfo().getVersion());
            }
        }

        {
            // check & write meta
            // ...
        }

        // install plugin
        Plugin plugin = pluginLoader.installPlugin(info);

        PluginRef pluginRef = new PluginRef(plugin, info);

        {
            PluginRef checkRef = plugins.get(info.getType().ordinal()).putIfAbsent(info.getName(), pluginRef);

            if (!pluginRef.equals(checkRef)) {
                pluginLoader.uninstallPlugin(info, plugin);
                throw new UserException(
                        "plugin " + info.getName() + " has install version " + checkRef.getPluginInfo().getVersion());
            }
        }
    }

    /**
     * Dynamic uninstall plugin thought install statement
     */
    public void uninstallPlugin(String pluginName) throws IOException, UserException {
        for (PluginType type : PluginType.values()) {
            if (plugins.get(type.ordinal()).containsKey(pluginName)) {
                PluginRef ref = plugins.get(type.ordinal()).remove(pluginName);

                {
                    // check & update meta
                    // ...
                }

                // uninstall plugin
                pluginLoader.uninstallPlugin(ref.pluginInfo, ref.plugin);
                return;
            }
        }
    }

    /**
     * For built-in Plugin register
     */
    public boolean registerPlugin(PluginInfo pluginInfo, Plugin plugin) {
        if (Objects.isNull(pluginInfo) || Objects.isNull(plugin) || Objects.isNull(pluginInfo.getType()) || Strings
                .isNullOrEmpty(pluginInfo.getName())) {
            return false;
        }

        PluginRef ref = new PluginRef(plugin, pluginInfo);
        PluginRef checkRef = plugins.get(pluginInfo.getType().ordinal()).putIfAbsent(pluginInfo.getName(), ref);

        return ref.equals(checkRef);
    }

    public final Plugin getPlugin(String name, PluginType type) {
        PluginRef ref = plugins.get(type.ordinal()).get(name);

        if (null != ref) {
            return ref.getPlugin();
        }

        return null;
    }

    public final List<Plugin> getPluginList(PluginType type) {
        Map<String, PluginRef> m = plugins.get(type.ordinal());

        List<Plugin> l = Lists.newArrayListWithCapacity(m.size());
        for (PluginRef ref : m.values()) {
            l.add(ref.getPlugin());
        }

        return Collections.unmodifiableList(l);
    }

    class PluginRef {

        private Plugin plugin;

        private PluginInfo pluginInfo;

        public PluginRef(Plugin plugin, PluginInfo pluginInfo) {
            this.plugin = plugin;
            this.pluginInfo = pluginInfo;
        }

        public Plugin getPlugin() {
            return plugin;
        }

        public PluginInfo getPluginInfo() {
            return pluginInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PluginRef pluginRef = (PluginRef) o;
            return Objects.equals(pluginInfo, pluginRef.pluginInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pluginInfo);
        }
    }
}
