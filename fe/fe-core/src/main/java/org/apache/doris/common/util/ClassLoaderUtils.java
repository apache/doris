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

import org.apache.commons.collections.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

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
    private static final Map<String, String> pluginDirMapping = new HashedMap();

    static {
        pluginDirMapping.put(AuthenticatorFactory.class.getSimpleName(), Config.authentication_plugins_dir);
        pluginDirMapping.put(AccessControllerFactory.class.getSimpleName(), Config.authorization_plugins_dir);
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
        String pluginDir = pluginDirMapping.get(pluginDirKey);
        if (pluginDir == null) {
            throw new RuntimeException("No mapping found for plugin directory key: " + pluginDirKey);
        }
        File jarDir = new File(pluginDir);
        // If the directory does not exist, return an empty list.
        if (!jarDir.exists()) {
            return new ArrayList<>();
        }
        if (!jarDir.isDirectory()) {
            throw new IOException("The specified path is not a directory: " + pluginDir);
        }

        File[] jarFiles = jarDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jarFiles == null || jarFiles.length == 0) {
            LOG.info("No JAR files found in the plugin directory: {}", pluginDir);
            return new ArrayList<>();
        }

        List<T> services = new ArrayList<>();
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
        return services;
    }
}
