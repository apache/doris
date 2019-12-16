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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

public class PluginLoader {
    private final static Logger LOG = LogManager.getLogger(PluginLoader.class);

    private static final List<String> DEFAULT_PROTOCOL = ImmutableList.of("https://", "http://");

    private final Path pluginDir;

    public PluginLoader(String path) {
        pluginDir = FileSystems.getDefault().getPath(path);
    }

    /**
     * download .zip and read plugin.properties
     */
    public PluginInfo getPluginInfo(String path) throws IOException, UserException {

        Path zipPath = download(path);

        Path actualPath = unzip(zipPath);

        return PluginInfo.readFromProperties(actualPath, path);
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
     * close plugin and delete Plugin path
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

        if (StringUtils.isBlank(pluginInfo.getInstallPath())) {
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

    /**
     * unzip .zip file and delete it
     */
    Path unzip(Path zip) throws IOException, UserException {
        Path target = Files.createTempDirectory(pluginDir, ".install_");

        try (ZipInputStream zipInput = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zipInput.getNextEntry()) != null) {
                Path targetFile = target.resolve(entry.getName());
                if (entry.getName().startsWith("doris/")) {
                    throw new UserException("Not use \"doris\" directory within the plugin zip.");
                }
                // Using the entry name as a path can result in an entry outside of the plugin dir,
                // either if the name starts with the root of the filesystem, or it is a relative
                // entry like ../whatever. This check attempts to identify both cases by first
                // normalizing the path (which removes foo/..) and ensuring the normalized entry
                // is still rooted with the target plugin directory.
                if (!targetFile.normalize().startsWith(target)) {
                    throw new UserException("Zip contains entry name '" +
                            entry.getName() + "' resolving outside of plugin directory");
                }

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                if (!Files.isSymbolicLink(targetFile.getParent())) {
                    Files.createDirectories(targetFile.getParent());
                }
                if (!entry.isDirectory()) {
                    try (OutputStream out = Files.newOutputStream(targetFile)) {
                        int len;
                        while ((len = zipInput.read(buffer)) >= 0) {
                            out.write(buffer, 0, len);
                        }
                    }
                }
                zipInput.closeEntry();
            }

        } catch (UserException e) {
            //            Files.deleteIfExists(target);
            throw e;
        }

        Files.deleteIfExists(zip);

        return target;
    }

    /**
     * download zip if the path is remote source, or copy zip if the path is local source
     **/
    Path download(String path) throws IOException, UserException {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Plugin library path: " + path);
        }

        boolean isLocal = true;
        for (String p : DEFAULT_PROTOCOL) {
            if (StringUtils.startsWithIgnoreCase(path, p)) {
                isLocal = false;
                break;
            }
        }

        if (!isLocal) {
            return downloadAndValidateZip(path);
        } else {
            return copyLocalZip(path);
        }
    }

    Path copyLocalZip(String path) throws IOException {
        Path sourceZip = FileSystems.getDefault().getPath(path);
        Path targetZip = Files.createTempFile(pluginDir, ".plugin_", ".zip");

        return Files.copy(sourceZip, targetZip, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * download zip and check md5
     **/
    Path downloadAndValidateZip(String path) throws IOException, UserException {
        LOG.info("download plugin zip from: " + path);

        Path zip = Files.createTempFile(pluginDir, ".plugin_", ".zip");

        // download zip
        try (InputStream in = getRemoteInputStream(path)) {
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }

        // .md5 check
        String expectedChecksum = "";
        try (InputStream in = getRemoteInputStream(path + ".md5")) {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            expectedChecksum = br.readLine();
        }

        DigestUtils.md5Hex(Files.readAllBytes(zip));
        final String actualChecksum = DigestUtils.md5Hex(Files.readAllBytes(zip));

        if (!StringUtils.equalsIgnoreCase(expectedChecksum, actualChecksum)) {
            Files.delete(zip);
            throw new UserException(
                    "MD5 check mismatch, expected " + expectedChecksum + " but actual " + actualChecksum);
        }

        return zip;
    }

    InputStream getRemoteInputStream(String url) throws IOException {
        URL u = new URL(url);
        return u.openConnection().getInputStream();
    }

}
