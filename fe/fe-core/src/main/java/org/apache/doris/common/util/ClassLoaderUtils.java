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

import org.apache.doris.common.EnvUtils;
import org.apache.doris.mysql.authenticate.AuthenticatorFactory;

import org.apache.commons.collections.map.HashedMap;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public class ClassLoaderUtils {

    private static final Map<String, String> pluginDirMapping = new HashedMap();

    static {
        pluginDirMapping.put(AuthenticatorFactory.class.getSimpleName(), "auth-lib");
    }

    /**
     * Loads service implementations from JAR files in the specified plugin directory.
     *
     * @param serviceClass The class type of the service to load.
     * @param <T>          The type of the service.
     * @return A list of service instances loaded from JAR files.
     * @throws IOException      If there is an error reading the JAR files or directory.
     * @throws RuntimeException If there is a problem with the directory mapping or URL creation.
     */
    public static <T> List<T> loadServicesFromDirectory(Class<T> serviceClass) throws IOException {
        String pluginDirKey = serviceClass.getSimpleName();
        String pluginDir = pluginDirMapping.get(pluginDirKey);
        if (pluginDir == null) {
            throw new RuntimeException("No mapping found for plugin directory key: " + pluginDirKey);
        }

        String jarDirPath = EnvUtils.getDorisHome() + File.separator + pluginDir;
        File jarDir = new File(jarDirPath);

        if (!jarDir.isDirectory()) {
            throw new IOException("The specified path is not a directory: " + jarDirPath);
        }

        File[] jarFiles = jarDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jarFiles == null || jarFiles.length == 0) {
            throw new IOException("No JAR files found in the specified directory: " + jarDirPath);
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
