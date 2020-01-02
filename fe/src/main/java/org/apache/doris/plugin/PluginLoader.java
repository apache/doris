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
import java.util.Objects;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

public abstract class PluginLoader {
    protected final Path pluginDir;

    protected String source;

    protected Plugin plugin;

    protected PluginContext pluginContext;

    protected PluginLoader(String path, String source) {
        this.pluginDir = FileSystems.getDefault().getPath(path);
        this.source = source;
        this.plugin = null;
        this.pluginContext = null;
    }

    protected PluginLoader(String path, PluginContext info) {
        this.pluginDir = FileSystems.getDefault().getPath(path);
        this.source = info.getSource();
        this.plugin = null;
        this.pluginContext = info;
    }

    public abstract void install() throws UserException, IOException;

    public abstract void uninstall() throws IOException, UserException;

    public PluginContext getPluginContext() throws IOException, UserException {
        return pluginContext;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    protected void pluginInstallValid() throws UserException {

    }

    protected void pluginUninstallValid() throws UserException {
        // check plugin flags
        if ((plugin.flags() & Plugin.PLUGIN_NOT_DYNAMIC_UNINSTALL) > 0) {
            throw new UserException("plugin " + pluginContext + " not allow dynamic uninstall");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PluginLoader that = (PluginLoader) o;
        return pluginContext.equals(that.pluginContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pluginContext);
    }
}

class DynamicPluginLoader extends PluginLoader {
    private final static Logger LOG = LogManager.getLogger(PluginLoader.class);

    protected DynamicPluginLoader(String path, String source) {
        super(path, source);
    }

    public DynamicPluginLoader(String path, PluginContext info) {
        super(path, info);
    }

    /**
     * get Plugin .zip and read plugin.properties
     */
    @Override
    public PluginContext getPluginContext() throws IOException, UserException {
        // already install
        if (pluginContext != null) {
            return pluginContext;
        }

        // download plugin and extract
        PluginZip zip = new PluginZip(source);
        Path target = Files.createTempDirectory(pluginDir, ".install_");

        Path tempPath = zip.extract(target);

        return PluginContext.readFromProperties(tempPath, source);
    }

    /**
     * move plugin to Doris's PLUGIN_DIR and dynamic load the plugin class
     */
    public void install() throws UserException, IOException {

        Path realPath = movePlugin();

        plugin = dynamicLoadPlugin(realPath);

        pluginInstallValid();

        plugin.init(pluginContext);
    }

    /**
     * close plugin and delete Plugin
     */
    public void uninstall() throws IOException, UserException {
        if (plugin == null) {
            return;
        }

        pluginUninstallValid();

        plugin.close();

        if (StringUtils.startsWithIgnoreCase(pluginContext.getInstallPath(), pluginDir.toString())) {
            File f = new File(pluginContext.getInstallPath());
            if (f.exists()) {
                FileUtils.deleteDirectory(f);
            }
        }
    }

    Plugin dynamicLoadPlugin(Path installPath) throws IOException, UserException {
        Set<URL> jarList = getJarUrl(installPath);

        // create a child to load the plugin in this bundle
        ClassLoader parentLoader = PluginClassLoader.createLoader(getClass().getClassLoader(), Collections.EMPTY_LIST);
        ClassLoader loader = URLClassLoader.newInstance(jarList.toArray(new URL[0]), parentLoader);

        Class<? extends Plugin> pluginClass = loadPluginClass(pluginContext.getClassName(), loader);
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
    Path movePlugin() throws UserException, IOException {

        if (Strings.isNullOrEmpty(pluginContext.getInstallPath())) {
            throw new UserException("Install plugin " + pluginContext.getName() + " failed.");
        }

        Path tempPath = FileSystems.getDefault().getPath(pluginContext.getInstallPath());

        if (!Files.exists(tempPath) || !Files.isDirectory(tempPath)) {
            throw new UserException(
                    "Install plugin " + pluginContext.getName() + " failed. cause " + tempPath.toString()
                            + " exists");
        }

        Path targetPath = FileSystems.getDefault().getPath(pluginDir.toString(), pluginContext.getName());

        Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE);

        pluginContext.installPath = targetPath.toString();

        return targetPath;
    }

}

class BuiltinPluginLoader extends PluginLoader {

    protected BuiltinPluginLoader(String path, PluginContext info, Plugin plugin) {
        super(path, info);
        this.plugin = plugin;
    }

    @Override
    public void install() throws UserException, IOException {
        pluginInstallValid();
        plugin.init(pluginContext);
    }

    @Override
    public void uninstall() throws IOException, UserException {
        if (plugin == null) {
            return;
        }

        pluginUninstallValid();

        plugin.close();
    }
}