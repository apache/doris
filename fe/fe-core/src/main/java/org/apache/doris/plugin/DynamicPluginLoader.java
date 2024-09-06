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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class DynamicPluginLoader extends PluginLoader {
    private static final Logger LOG = LogManager.getLogger(DynamicPluginLoader.class);

    public static final String MD5SUM_KEY = "md5sum";

    // the final dir which contains all plugin files.
    // eg:
    // Config.plugin_dir/plugin_name/
    protected Path installPath;

    protected String expectedMd5sum;

    // for processing install stmt
    DynamicPluginLoader(String pluginDir, String source, String expectedMd5sum) {
        super(pluginDir, source);
        this.expectedMd5sum = expectedMd5sum;
    }

    // for test and replay
    DynamicPluginLoader(String pluginPath, PluginInfo info) {
        super(pluginPath, info);
        this.installPath = FileSystems.getDefault().getPath(pluginDir.toString(), pluginInfo.getName());
        this.expectedMd5sum = pluginInfo.getProperties().get(MD5SUM_KEY);
    }

    @Override
    public boolean isDynamicPlugin() {
        return true;
    }

    /**
     * get Plugin .zip and read plugin.properties
     */
    @Override
    public PluginInfo getPluginInfo() throws IOException, UserException {
        // already install
        if (pluginInfo != null) {
            return pluginInfo;
        }

        Path tmpTarget = null;

        try {
            if (installPath == null) {
                // download plugin and extract
                PluginZip zip = new PluginZip(source, expectedMd5sum);
                // generation a tmp dir to extract the zip
                tmpTarget = Files.createTempDirectory(pluginDir, ".install_");
                // for now, installPath point to the temp dir which contains
                // all files extracted from zip or copied from user specified dir.
                installPath = zip.extract(tmpTarget);
            }
            pluginInfo = PluginInfo.readFromProperties(installPath, source);
        } catch (Exception e) {
            if (tmpTarget != null) {
                FileUtils.deleteQuietly(tmpTarget.toFile());
            }
            throw e;
        }
        return pluginInfo;
    }

    /**
     * move plugin to Doris's PLUGIN_DIR and dynamic load the plugin class
     */
    public void install() throws UserException, IOException {
        if (hasInstalled()) {
            throw new UserException("Plugin " + pluginInfo.getName() + " has already been installed.");
        }

        getPluginInfo();

        movePlugin();

        try {
            plugin = dynamicLoadPlugin(true);

            pluginInstallValid();

            pluginContext.setPluginPath(installPath.toString());

            plugin.init(pluginInfo, pluginContext);
        } catch (Throwable e) {
            throw new UserException(e.getMessage());
        }
    }

    private boolean hasInstalled() {
        // check already install
        if (pluginInfo != null) {
            Path targetPath = FileSystems.getDefault().getPath(pluginDir.toString(), pluginInfo.getName());
            if (Files.exists(targetPath)) {
                return true;
            }
        }
        return false;
    }

    /**
     * close plugin and delete Plugin
     */
    public void uninstall() throws IOException, UserException {
        if (plugin != null) {
            pluginUninstallValid();
            plugin.close();
        }

        if (null != installPath && Files.exists(installPath)
                && Files.isSameFile(installPath.getParent(), pluginDir)) {
            FileUtils.deleteQuietly(installPath.toFile());
        }
    }

    /**
     * reload plugin if plugin has already been installed, else will re-install.
     * Notice that this method will create a new instance of plugin.
     *
     * @throws PluginException
     */
    public void reload() throws IOException, UserException {
        if (Config.enable_check_compatibility_mode) {
            return;
        }

        if (Env.isCheckpointThread()) {
            /*
             * No need to reload the plugin when this is a checkpoint thread.
             * Because this reload() method will create a new instance of plugin and try to start it.
             * But in checkpoint thread, we do not need a instance of plugin.
             */
            return;
        }

        if (hasInstalled()) {
            plugin = dynamicLoadPlugin(true);
            pluginInstallValid();
            pluginContext.setPluginPath(installPath.toString());
            plugin.init(pluginInfo, pluginContext);
        } else {
            // re-install
            Map<String, String> properties = pluginInfo.getProperties();
            installPath = null;
            pluginInfo = null;
            pluginInfo = getPluginInfo();
            pluginInfo.setProperties(properties);
            install();
        }
    }

    /*
     * Dynamic load the plugin.
     * if closePreviousPlugin is true, we will check if there is already an instance of plugin, if yes, close it.
     */
    Plugin dynamicLoadPlugin(boolean closePreviousPlugin) throws IOException, UserException {
        if (closePreviousPlugin) {
            if (plugin != null) {
                try {
                    plugin.close();
                } catch (Exception e) {
                    LOG.warn("failed to close previous plugin {}, ignore it", e);
                } finally {
                    plugin = null;
                }
            }
        }

        Set<URL> jarList = getJarUrl(installPath);

        // create a child to load the plugin in this bundle
        ClassLoader parentLoader = PluginClassLoader.createLoader(getClass().getClassLoader(), Collections.EMPTY_LIST);
        ClassLoader loader = URLClassLoader.newInstance(jarList.toArray(new URL[0]), parentLoader);

        Class<? extends Plugin> pluginClass;
        try {
            pluginClass = loader.loadClass(pluginInfo.getClassName()).asSubclass(Plugin.class);
        } catch (ClassNotFoundException e) {
            throw new UserException("Could not find plugin class [" + pluginInfo.getClassName() + "]", e);
        }

        return loadPluginClass(pluginClass);
    }

    private Plugin loadPluginClass(Class<? extends Plugin> pluginClass) {
        final Constructor<?>[] constructors = pluginClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public constructor for [" + pluginClass.getName() + "]");
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public constructor for [" + pluginClass.getName() + "]");
        }

        final Constructor<?> constructor = constructors[0];

        try {
            if (constructor.getParameterCount() == 0) {
                return (Plugin) constructor.newInstance();
            } else {
                throw new IllegalStateException("failed to find correct constructor.");
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    private Set<URL> getJarUrl(Path path) throws IOException {
        Set<URL> urls = new LinkedHashSet<>();
        // gather urls for jar files
        try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(path, "*.jar")) {
            for (Path jar : jarStream) {
                // normalize with toRealPath to get symlinks out of our hair
                URL url = jar.toRealPath().toUri().toURL();
                if (!urls.add(url)) {
                    throw new IllegalStateException("duplicate codebase: " + url);
                }
            }
        }

        return urls;
    }

    /**
     * move plugin's temp install directory to Doris's PLUGIN_DIR/plugin_name
     */
    public void movePlugin() throws UserException, IOException {
        if (installPath == null || !Files.exists(installPath)) {
            throw new PluginException("Install plugin " + pluginInfo.getName()
                    + " failed, because install path doesn't exist.");
        }

        Path targetPath = FileSystems.getDefault().getPath(pluginDir.toString(), pluginInfo.getName());
        if (Files.exists(targetPath)) {
            if (!Files.isSameFile(installPath, targetPath)) {
                throw new PluginException(
                        "Install plugin " + pluginInfo.getName() + " failed. because " + installPath.toString()
                                + " exists");
            }
        } else {
            Files.move(installPath, targetPath, StandardCopyOption.ATOMIC_MOVE);
        }

        // move success
        installPath = targetPath;
    }

}
