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

import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginLoader.PluginStatus;
import org.apache.doris.qe.AuditLogBuilder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PluginMgr implements Writable {
    private final static Logger LOG = LogManager.getLogger(PluginMgr.class);

    private final Map<String, PluginLoader>[] plugins;

    public PluginMgr() {
        plugins = new Map[PluginType.MAX_PLUGIN_SIZE];
        for (int i = 0; i < PluginType.MAX_PLUGIN_SIZE; i++) {
            plugins[i] = Maps.newConcurrentMap();
        }
    }

    // create the plugin dir if missing
    public void init() throws PluginException {
        File file = new File(Config.plugin_dir);
        if (file.exists() && !file.isDirectory()) {
            throw new PluginException("FE plugin dir " + Config.plugin_dir + " is not a directory");
        }

        if (!file.exists()) {
            if (!file.mkdir()) {
                throw new PluginException("failed to create FE plugin dir " + Config.plugin_dir);
            }
        }

        registerBuiltinPlugins();
    }

    private void registerBuiltinPlugins() {
        // AuditLog
        AuditLogBuilder auditLogBuilder = new AuditLogBuilder();
        if (!registerPlugin(auditLogBuilder.getPluginInfo(), auditLogBuilder)) {
            LOG.warn("failed to register audit log builder");
        }

        // other builtin plugins
    }

    public PluginInfo installPlugin(InstallPluginStmt stmt) throws IOException, UserException {
        PluginLoader pluginLoader = new DynamicPluginLoader(Config.plugin_dir, stmt.getPluginPath());
        pluginLoader.setStatus(PluginStatus.INSTALLING);

        try {
            PluginInfo info = pluginLoader.getPluginInfo();

            if (plugins[info.getTypeId()].containsKey(info.getName())) {
                throw new UserException("plugin " + info.getName() + " has already been installed.");
            }
            
            // install plugin
            pluginLoader.install();
            pluginLoader.setStatus(PluginStatus.INSTALLED);
            
            if (plugins[info.getTypeId()].putIfAbsent(info.getName(), pluginLoader) != null) {
                pluginLoader.uninstall();
                throw new UserException("plugin " + info.getName() + " has already been installed.");
            }

            Catalog.getCurrentCatalog().getEditLog().logInstallPlugin(info);
            LOG.info("install plugin = " + info.getName());
            return info;
        } catch (IOException | UserException e) {
            pluginLoader.setStatus(PluginStatus.ERROR);
            throw e;
        }
    }

    /**
     * Dynamic uninstall plugin
     */
    public PluginInfo uninstallPlugin(String name) throws IOException, UserException {
        for (int i = 0; i < PluginType.MAX_PLUGIN_SIZE; i++) {
            if (plugins[i].containsKey(name)) {
                PluginLoader loader = plugins[i].get(name);

                if (null != loader && loader.isDynamicPlugin()) {
                    loader.pluginUninstallValid();
                    loader.setStatus(PluginStatus.UNINSTALLING);
                    // uninstall plugin
                    loader.uninstall();
                    plugins[i].remove(name);

                    loader.setStatus(PluginStatus.UNINSTALLED);
                    return loader.getPluginInfo();
                }
            }
        }

        return null;
    }

    /**
     * For built-in Plugin register
     */
    public boolean registerPlugin(PluginInfo pluginInfo, Plugin plugin) {
        if (Objects.isNull(pluginInfo) || Objects.isNull(plugin) || Objects.isNull(pluginInfo.getType())
                || Strings.isNullOrEmpty(pluginInfo.getName())) {
            return false;
        }

        PluginLoader loader = new BuiltinPluginLoader(Config.plugin_dir, pluginInfo, plugin);
        PluginLoader checkLoader = plugins[pluginInfo.getTypeId()].putIfAbsent(pluginInfo.getName(), loader);

        return checkLoader == null;
    }

    /**
     * Load plugin:
     * - if has already benn installed, return
     * - if not installed, install
     */
    public void loadDynamicPlugin(PluginInfo info) throws IOException, UserException {
        DynamicPluginLoader pluginLoader = new DynamicPluginLoader(Config.plugin_dir, info);

        try {
            PluginLoader checkLoader = plugins[info.getTypeId()].putIfAbsent(info.getName(), pluginLoader);

            if (checkLoader != null) {
                throw new UserException(
                        "plugin " + info.getName() + " has already been installed.");
            }

            pluginLoader.setStatus(PluginStatus.INSTALLING);
            // install plugin
            pluginLoader.reload();
            pluginLoader.setStatus(PluginStatus.INSTALLED);
        } catch (IOException | UserException e) {
            pluginLoader.setStatus(PluginStatus.ERROR);
            throw e;
        }
    }

    public final Plugin getActivePlugin(String name, PluginType type) {
        PluginLoader loader = plugins[type.ordinal()].get(name);

        if (null != loader && loader.getStatus() == PluginStatus.INSTALLED) {
            return loader.getPlugin();
        }

        return null;
    }

    public final List<Plugin> getActivePluginList(PluginType type) {
        Map<String, PluginLoader> m = plugins[type.ordinal()];
        List<Plugin> l = Lists.newArrayListWithCapacity(m.size());

        m.values().forEach(d -> {
            if (d.getStatus() == PluginStatus.INSTALLED) {
                l.add(d.getPlugin());
            }
        });

        return Collections.unmodifiableList(l);
    }

    public final List<PluginInfo> getAllDynamicPluginInfo() {
        List<PluginInfo> list = Lists.newArrayList();

        for (Map<String, PluginLoader> map : plugins) {
            map.values().forEach(loader -> {
                try {
                    if (loader.isDynamicPlugin()) {
                        list.add(loader.getPluginInfo());
                    }
                } catch (Exception e) {
                    LOG.warn("load plugin from {} failed", loader.source, e);
                }
            });
        }

        return list;
    }

    public final List<PluginLoader> getAllPluginLoader() {
        List<PluginLoader> list = Lists.newArrayList();

        for (Map<String, PluginLoader> map : plugins) {
            map.values().forEach(loader -> {
                try {
                    list.add(loader);
                } catch (Exception e) {
                    LOG.warn("load plugin from {} failed", loader.source, e);
                }
            });
        }

        return list;
    }

    public void readFields(DataInputStream dis) throws IOException {
        int size = dis.readInt();

        for (int i = 0; i < size; i++) {
            try {
                PluginInfo pluginInfo = PluginInfo.read(dis);
                loadDynamicPlugin(pluginInfo);
            } catch (Exception e) {
                LOG.warn("load plugin failed.", e);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        List<PluginInfo> list = getAllDynamicPluginInfo();

        int size = list.size();

        out.writeInt(size);

        for (PluginInfo pc : list) {
            pc.write(out);
        }
    }
}
