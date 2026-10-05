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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;
import org.apache.doris.mysql.authenticate.AuthenticatorFactory;
import org.apache.doris.mysql.privilege.AccessControllerFactory;

import com.google.common.base.Splitter;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility class for loading service implementations from external JAR files in specific plugin directories.
 * <p>
 * This class provides a mechanism to dynamically load service implementations from JAR files located in
 * plugin directories, which are mapped by the service type's simple name. It uses a child-first class loading
 * strategy to ensure that plugins in the JAR files are prioritized over classes loaded by the parent class loader.
 * <p>
 * It is particularly useful in scenarios where the system needs to support modular or pluggable architectures,
 * such as dynamically loading authenticators, access controllers, or other pluggable services from external
 * directories without requiring the services to be bundled with the core application.
 * <p>
 * Plugin directory mappings are maintained in a static map where the key is the simple name of the service class,
 * and the value is the relative path to the directory containing the plugin JARs.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * List<AuthenticatorFactory> authenticators = ClassLoaderUtils.loadServicesFromDirectory(AuthenticatorFactory.class);
 * }
 * </pre>
 *
 * @see ServiceLoader
 * @see ChildFirstClassLoader
 */
public class ClassLoaderUtils {
    private static final Logger LOG = LogManager.getLogger(ClassLoaderUtils.class);
    // A mapping of service class simple names to their respective plugin directories.
    private static final Map<String, Supplier<String>> pluginDirMapping = new HashedMap<>();

    static {
        pluginDirMapping.put(AuthenticatorFactory.class.getSimpleName(), () -> Config.authentication_plugins_dir);
        pluginDirMapping.put(AccessControllerFactory.class.getSimpleName(), () -> Config.authorization_plugins_dir);
    }

    public static List<Path> parsePluginRootDirectories(String pluginRootsConfig) {
        if (pluginRootsConfig == null) {
            return Collections.emptyList();
        }
        return Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(pluginRootsConfig)
                .stream()
                .map(Paths::get)
                .collect(Collectors.toList());
    }

    /**
     * Loads service implementations from JAR files in the specified plugin directory.
     * <p>
     * The method first looks up the directory for the given service class type from the {@code pluginDirMapping}.
     * If a directory exists and contains JAR files, it will load the service implementations from those JAR files
     * using a child-first class loader to prioritize the plugin classes.
     * <p>
     * If no directory is found for the service type, or the directory is invalid, an exception is thrown. If the
     * directory does not contain any JAR files, an empty list is returned.
     *
     * @param serviceClass The class type of the service to load. This should be the interface or
     *                     base class of the service.
     * @param <T>          The type of the service.
     * @return A list of service instances loaded from JAR files. If no services are found, an empty list is returned.
     * @throws IOException      If there is an error reading the JAR files or the directory is invalid.
     * @throws RuntimeException If there is a problem with the directory mapping or JAR file URL creation.
     */
    public static <T> List<T> loadServicesFromDirectory(Class<T> serviceClass) throws IOException {
        String pluginDirKey = serviceClass.getSimpleName();
        Supplier<String> pluginDirSupplier = pluginDirMapping.get(pluginDirKey);
        if (pluginDirSupplier == null) {
            throw new RuntimeException("No mapping found for plugin directory key: " + pluginDirKey);
        }
        List<Path> pluginRoots = parsePluginRootDirectories(pluginDirSupplier.get());
        if (pluginRoots.isEmpty()) {
            return new ArrayList<>();
        }

        List<T> services = new ArrayList<>();
        for (Path pluginRoot : pluginRoots) {
            File jarDir = pluginRoot.toFile();
            // If the directory does not exist, skip it.
            if (!jarDir.exists()) {
                continue;
            }
            if (!jarDir.isDirectory()) {
                throw new IOException("The specified path is not a directory: " + pluginRoot);
            }

            File[] jarFiles = jarDir.listFiles((dir, name) -> name.endsWith(".jar"));
            if (jarFiles == null || jarFiles.length == 0) {
                LOG.info("No JAR files found in the plugin directory: {}", pluginRoot);
                continue;
            }

            for (File jarFile : jarFiles) {
                URL[] jarURLs;
                jarURLs = new URL[]{jarFile.toURI().toURL()};

                try (ChildFirstClassLoader urlClassLoader = new ChildFirstClassLoader(jarURLs,
                        Thread.currentThread().getContextClassLoader())) {
                    ServiceLoader<T> serviceLoader = ServiceLoader.load(serviceClass, urlClassLoader);
                    for (T service : serviceLoader) {
                        services.add(service);
                    }
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return services;
    }
}
