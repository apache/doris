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

import java.io.File;
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
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

public class PluginLoader {
    private final static Logger LOG = LogManager.getLogger(PluginLoader.class);

    private final Path pluginDir;

    public PluginLoader(String path) {
        pluginDir = FileSystems.getDefault().getPath(path);
    }

    /**
     * get Plugin .zip and read plugin.properties
     */
    public PluginInfo readPluginInfo(String source) throws IOException, UserException {
        PluginZip zip = new PluginZip(source);
        Path target = Files.createTempDirectory(pluginDir, ".install_");

        Path tempPath = zip.extract(target);

        return PluginInfo.readFromProperties(tempPath, source);
    }

    /**
     * move plugin to Doris's PLUGIN_DIR and dynamic load the plugin class
     */
    public Plugin installPlugin(PluginInfo pluginInfo) throws UserException, IOException {

        Path realPath = movePlugin(pluginInfo);

        Plugin plugin = dynamicLoadPlugin(pluginInfo, realPath);

        pluginInstallValid(pluginInfo, plugin);

        plugin.init();

        return plugin;
    }

    /**
     * close plugin and delete Plugin
     */
    public void uninstallPlugin(PluginInfo plugininfo, Plugin plugin) throws IOException, UserException {
        pluginUninstallValid(plugininfo, plugin);

        plugin.close();

        if (StringUtils.startsWithIgnoreCase(plugininfo.getInstallPath(), pluginDir.toString())) {
            File f = new File(plugininfo.getInstallPath());
            if (f.exists()) {
                FileUtils.deleteDirectory(f);
            }
        }
    }

    private void pluginInstallValid(PluginInfo pluginInfo, Plugin plugin) throws UserException {
        // check plugin flags
        if ((plugin.flags() & Plugin.PLUGIN_NOT_DYNAMIC_INSTALL) > 0) {
            throw new  UserException("plugin " + pluginInfo + " not allow dynamic install");
        }
    }

    private void pluginUninstallValid(PluginInfo pluginInfo, Plugin plugin) throws UserException {
        // check plugin flags
        if ((plugin.flags() & Plugin.PLUGIN_NOT_DYNAMIC_UNINSTALL) > 0) {
            throw new  UserException("plugin " + pluginInfo + " not allow dynamic uninstall");
        }
    }

    Plugin dynamicLoadPlugin(PluginInfo pluginInfo, Path installPath) throws IOException, UserException {
        Set<URL> jarList = getJarUrl(installPath);

        // create a child to load the plugin in this bundle
        ClassLoader parentLoader = PluginClassLoader.createLoader(getClass().getClassLoader(), Collections.EMPTY_LIST);
        ClassLoader loader = URLClassLoader.newInstance(jarList.toArray(new URL[0]), parentLoader);

        Class<? extends Plugin> pluginClass = loadPluginClass(pluginInfo.getClassName(), loader);
        return loadPlugin(pluginClass, installPath);
    }

    private Plugin loadPlugin(Class<? extends Plugin> pluginClass, Path installPath) {
        final Constructor<?>[] constructors = pluginClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public constructor for [" + pluginClass.getName() + "]");
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public constructor for [" + pluginClass.getName() + "]");
        }

        final Constructor<?> constructor = constructors[0];

        final Class[] parameterTypes = constructor.getParameterTypes();
        try {
            if (constructor.getParameterCount() == 1 && parameterTypes[0] == Path.class) {
                return (Plugin)constructor.newInstance(installPath);
            } else if (constructor.getParameterCount() == 0) {
                return (Plugin)constructor.newInstance();
            } else {
                throw new IllegalStateException("failed to find correct constructor.");
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    private Class<? extends Plugin> loadPluginClass(String className, ClassLoader loader) throws UserException {
        try {
            return loader.loadClass(className).asSubclass(Plugin.class);
        } catch (ClassNotFoundException e) {
            throw new UserException("Could not find plugin class [" + className + "]", e);
        }
    }

    private Set<URL> getJarUrl(Path installPath) throws IOException {
        Set<URL> urls = new LinkedHashSet<>();
        // gather urls for jar files
        try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(installPath, "*.jar")) {
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
     * move plugin's temp install directory to Doris's PLUGIN_DIR
     */
    Path movePlugin(PluginInfo pluginInfo) throws UserException, IOException {

        if (Strings.isNullOrEmpty(pluginInfo.getInstallPath())) {
            throw new UserException("Install plugin " + pluginInfo.getName() + " failed.");
        }

        Path tempPath = FileSystems.getDefault().getPath(pluginInfo.getInstallPath());

        if (!Files.exists(tempPath) || !Files.isDirectory(tempPath)) {
            throw new UserException("Install plugin " + pluginInfo.getName() + " failed. cause " + tempPath.toString()
                    + " exists");
        }

        Path targetPath = FileSystems.getDefault().getPath(pluginDir.toString(), pluginInfo.getName());

        Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE);

        pluginInfo.setInstallPath(targetPath.toString());

        return targetPath;
    }

}
