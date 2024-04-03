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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginLoader.PluginStatus;
import org.apache.doris.plugin.audit.AuditLoaderPlugin;
import org.apache.doris.plugin.audit.AuditLogBuilder;
import org.apache.doris.plugin.dialect.HttpDialectConverterPlugin;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.Set;

public class PluginMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(PluginMgr.class);

    public static final String BUILTIN_PLUGIN_PREFIX = "__builtin_";

    private final Map<String, PluginLoader>[] plugins;
    // use this for
    private final Map<String, DialectConverterPlugin>[] dialectPlugins;

    // all dynamic plugins should have unique names,
    private final Set<String> dynamicPluginNames;

    public PluginMgr() {
        plugins = new Map[PluginType.MAX_PLUGIN_TYPE_SIZE];
        for (int i = 0; i < PluginType.MAX_PLUGIN_TYPE_SIZE; i++) {
            // use synchronized wrapper for thread-safe
            plugins[i] = Collections.synchronizedSortedMap(Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER));
        }
        // use synchronized wrapper for thread-safe
        dynamicPluginNames = Collections.synchronizedSortedSet(Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER));

        dialectPlugins = new Map[Dialect.MAX_DIALECT_SIZE];
        for (int i = 0; i < Dialect.MAX_DIALECT_SIZE; i++) {
            // use synchronized wrapper for thread-safe
            dialectPlugins[i] = Collections.synchronizedSortedMap(Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER));
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

        initBuiltinPlugins();
    }

    private boolean checkDynamicPluginNameExist(String name) {
        return dynamicPluginNames.contains(name);
    }

    private boolean addDynamicPluginNameIfAbsent(String name) {
        return dynamicPluginNames.add(name);
    }

    private boolean removeDynamicPluginName(String name) {
        return dynamicPluginNames.remove(name);
    }

    private void initBuiltinPlugins() {
        // AuditLog: log audit log to file
        AuditLogBuilder auditLogBuilder = new AuditLogBuilder();
        if (!registerBuiltinPlugin(auditLogBuilder.getPluginInfo(), auditLogBuilder)) {
            LOG.warn("failed to register audit log builder");
        }

        // AuditLoader: log audit log to internal table
        AuditLoaderPlugin auditLoaderPlugin = new AuditLoaderPlugin();
        if (!registerBuiltinPlugin(auditLoaderPlugin.getPluginInfo(), auditLoaderPlugin)) {
            LOG.warn("failed to register audit log builder");
        }

        // sql dialect converter
        HttpDialectConverterPlugin httpDialectConverterPlugin = new HttpDialectConverterPlugin();
        if (!registerBuiltinPlugin(httpDialectConverterPlugin.getPluginInfo(), httpDialectConverterPlugin)) {
            LOG.warn("failed to register http dialect converter plugin");
        }

        // other builtin plugins
    }

    // install a plugin from user's command.
    // install should be successfully, or nothing should be left if failed to install.
    public PluginInfo installPlugin(InstallPluginStmt stmt) throws IOException, UserException {
        PluginLoader pluginLoader = new DynamicPluginLoader(Config.plugin_dir, stmt.getPluginPath(), stmt.getMd5sum());
        pluginLoader.setStatus(PluginStatus.INSTALLING);

        try {
            PluginInfo info = pluginLoader.getPluginInfo();
            if (stmt.getProperties() != null) {
                info.setProperties(stmt.getProperties());
            }

            if (checkDynamicPluginNameExist(info.getName())) {
                throw new UserException("plugin " + info.getName() + " has already been installed.");
            }

            // install plugin
            pluginLoader.install();
            pluginLoader.setStatus(PluginStatus.INSTALLED);

            if (!addDynamicPluginNameIfAbsent(info.getName())) {
                throw new UserException("plugin " + info.getName() + " has already been installed.");
            }
            plugins[info.getTypeId()].put(info.getName(), pluginLoader);
            // add dialect plugin
            Plugin plugin = pluginLoader.getPlugin();
            if (plugin instanceof DialectConverterPlugin) {
                addDialectPlugin((DialectConverterPlugin) plugin, info);
            }
            Env.getCurrentEnv().getEditLog().logInstallPlugin(info);
            LOG.info("install plugin {}", info.getName());
            return info;
        } catch (IOException | UserException e) {
            pluginLoader.uninstall();
            throw e;
        }
    }

    private void addDialectPlugin(DialectConverterPlugin plugin, PluginInfo info) {
        for (Dialect dialect : plugin.acceptDialects()) {
            dialectPlugins[dialect.ordinal()].put(info.getName(), plugin);
        }
    }

    private void removeDialectPlugin(String name) {
        for (int i = 0; i < Dialect.MAX_DIALECT_SIZE; i++) {
            dialectPlugins[i].remove(name);
        }
    }

    /**
     * Dynamic uninstall plugin.
     * If uninstall failed, the plugin should NOT be removed from plugin manager.
     */
    public PluginInfo uninstallPlugin(String name) throws IOException, UserException {
        if (!checkDynamicPluginNameExist(name)) {
            throw new DdlException("Plugin " + name + " does not exist");
        }

        for (int i = 0; i < PluginType.MAX_PLUGIN_TYPE_SIZE; i++) {
            if (plugins[i].containsKey(name)) {
                PluginLoader loader = plugins[i].get(name);
                if (loader == null) {
                    // this is not an atomic operation, so even if containsKey() is true,
                    // we may still get null object by get() method
                    continue;
                }

                if (!loader.isDynamicPlugin()) {
                    throw new DdlException("Only support uninstall dynamic plugins");
                }

                loader.pluginUninstallValid();
                loader.setStatus(PluginStatus.UNINSTALLING);
                // uninstall plugin
                loader.uninstall();

                // uninstall succeed, remove the plugin
                plugins[i].remove(name);
                loader.setStatus(PluginStatus.UNINSTALLED);
                removeDynamicPluginName(name);
                removeDialectPlugin(name);
                // do not get plugin info by calling loader.getPluginInfo(). That method will try to
                // reload the plugin properties from source if this plugin is not installed successfully.
                // Here we only need the plugin's name for persisting.
                // TODO(cmy): This is a bad design to couple the persist info with PluginInfo, but for
                // the compatibility, I till use this method.
                return new PluginInfo(name);
            }
        }

        throw new DdlException("Plugin " + name + " does not exist");
    }

    /**
     * For built-in Plugin register
     */
    public boolean registerBuiltinPlugin(PluginInfo pluginInfo, Plugin plugin) {
        if (Objects.isNull(pluginInfo) || Objects.isNull(plugin) || Objects.isNull(pluginInfo.getType())
                || Strings.isNullOrEmpty(pluginInfo.getName())) {
            return false;
        }

        PluginLoader loader = new BuiltinPluginLoader(Config.plugin_dir, pluginInfo, plugin);
        try {
            loader.install();
        } catch (Exception e) {
            LOG.warn("failed to register builtin plugin {}", pluginInfo.getName(), e);
            return false;
        }
        // add dialect plugin
        if (plugin instanceof DialectConverterPlugin) {
            addDialectPlugin((DialectConverterPlugin) plugin, pluginInfo);
        }
        PluginLoader checkLoader = plugins[pluginInfo.getTypeId()].putIfAbsent(pluginInfo.getName(), loader);
        return checkLoader == null;
    }

    /*
     * replay load plugin.
     * It must add the plugin to the "plugins" and "dynamicPluginNames", even if the plugin
     * is not loaded successfully.
     */
    public void replayLoadDynamicPlugin(PluginInfo info) throws IOException, UserException {
        DynamicPluginLoader pluginLoader = new DynamicPluginLoader(Config.plugin_dir, info);
        try {
            // should add to "plugins" first before loading.
            PluginLoader checkLoader = plugins[info.getTypeId()].putIfAbsent(info.getName(), pluginLoader);
            if (checkLoader != null) {
                throw new UserException("plugin " + info.getName() + " has already been installed.");
            }

            pluginLoader.setStatus(PluginStatus.INSTALLING);
            // install plugin
            pluginLoader.reload();
            pluginLoader.setStatus(PluginStatus.INSTALLED);
            // add dialect plugin
            Plugin plugin = pluginLoader.getPlugin();
            if (plugin instanceof DialectConverterPlugin) {
                addDialectPlugin((DialectConverterPlugin) plugin, info);
            }
        } catch (IOException | UserException e) {
            pluginLoader.setStatus(PluginStatus.ERROR, e.getMessage());
            throw e;
        } finally {
            // this is a replay process, so whether it is successful or not, add it's name.
            addDynamicPluginNameIfAbsent(info.getName());
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

        return ImmutableList.copyOf(l);
    }

    public final List<DialectConverterPlugin> getActiveDialectPluginList(Dialect dialect) {
        Map<String, DialectConverterPlugin> m = dialectPlugins[dialect.ordinal()];
        return ImmutableList.copyOf(m.values());
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

    public List<List<String>> getPluginShowInfos() {
        List<List<String>> rows = Lists.newArrayList();
        for (Map<String, PluginLoader> map : plugins) {
            for (Map.Entry<String, PluginLoader> entry : map.entrySet()) {
                List<String> r = Lists.newArrayList();
                PluginLoader loader = entry.getValue();

                PluginInfo pi = null;
                try {
                    pi = loader.getPluginInfo();
                } catch (Exception e) {
                    // plugin may not be loaded successfully
                    LOG.warn("failed to get plugin info for plugin: {}", entry.getKey(), e);
                }

                r.add(entry.getKey());
                r.add(pi != null ? pi.getType().name() : "UNKNOWN");
                r.add(pi != null ? pi.getDescription() : "UNKNOWN");
                r.add(pi != null ? pi.getVersion().toString() : "UNKNOWN");
                r.add(pi != null ? pi.getJavaVersion().toString() : "UNKNOWN");
                r.add(pi != null ? pi.getClassName() : "UNKNOWN");
                r.add(pi != null ? pi.getSoName() : "UNKNOWN");
                if (Strings.isNullOrEmpty(loader.source)) {
                    r.add("Builtin");
                } else {
                    r.add(loader.source);
                }

                r.add(loader.getStatus().toString());
                r.add(pi != null ? "{" + new PrintableMap<>(pi.getProperties(),
                        "=", true, false, true) + "}" : "UNKNOWN");
                rows.add(r);
            }
        }
        return rows;
    }

    public void readFields(DataInputStream dis) throws IOException {
        int size = dis.readInt();
        for (int i = 0; i < size; i++) {
            try {
                PluginInfo pluginInfo = PluginInfo.read(dis);
                replayLoadDynamicPlugin(pluginInfo);
            } catch (Exception e) {
                LOG.warn("load plugin failed.", e);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // only need to persist dynamic plugins
        List<PluginInfo> list = getAllDynamicPluginInfo();
        int size = list.size();
        out.writeInt(size);
        for (PluginInfo pc : list) {
            pc.write(out);
        }
    }
}
