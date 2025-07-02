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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * ChildFirstClassLoader is a custom class loader designed to load classes from
 * plugin JAR files. It uses a child-first class loading strategy, where the loader
 * first attempts to load classes from its own URLs (plugin JARs), and if the class
 * is not found, it delegates the loading to its parent class loader.
 * <p>
 * This class is intended for plugin-based systems where classes defined in plugins
 * might override or replace standard library classes.
 * <p>
 * Key features:
 * - Child-First loading mechanism.
 * - Support for loading classes from multiple JAR files.
 * - Efficient caching of JAR file resources to avoid repeated file access.
 */
public class ChildFirstClassLoader extends URLClassLoader {

    // A list of URLs pointing to JAR files
    private final List<URL> jarURLs;

    /**
     * Constructs a new ChildFirstClassLoader with the given URLs and parent class loader.
     * This constructor stores the URLs for class loading.
     *
     * @param urls   The URLs pointing to the plugin JAR files.
     * @param parent The parent class loader to use for delegation if class is not found.
     * @throws IOException        If there is an error opening the JAR files.
     * @throws URISyntaxException If there is an error converting the URL to URI.
     */
    public ChildFirstClassLoader(URL[] urls, ClassLoader parent) throws IOException, URISyntaxException {
        super(urls, parent);
        this.jarURLs = new ArrayList<>();
        for (URL url : urls) {
            if ("file".equals(url.getProtocol())) {
                this.jarURLs.add(url);
            }
        }
    }

    /**
     * Attempts to load the class with the specified name.
     * This method first tries to find the class using the current class loader (child-first strategy),
     * and if the class is not found, it delegates the loading to the parent class loader.
     *
     * @param name    The fully qualified name of the class to be loaded.
     * @param resolve If true, the class will be resolved after being loaded.
     * @return The resulting Class object.
     * @throws ClassNotFoundException If the class cannot be found by either the child or parent loader.
     */
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // Child-First mechanism: try to find the class locally first
        try {
            return findClass(name);
        } catch (ClassNotFoundException e) {
            // If the class is not found locally, delegate to the parent class loader
            return super.loadClass(name, resolve);
        }
    }

    /**
     * Searches for the class in the loaded plugin JAR files.
     * If the class is found in one of the JAR files, it will be defined and returned.
     *
     * @param name The fully qualified name of the class to find.
     * @return The resulting Class object.
     * @throws ClassNotFoundException If the class cannot be found in the JAR files.
     */
    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        String classFile = name.replace('.', '/') + ".class";  // Convert class name to path

        // Iterate over all the JAR URLs to find the class
        for (URL jarURL : jarURLs) {
            try (JarFile jarFile = new JarFile(Paths.get(jarURL.toURI()).toFile())) {
                JarEntry entry = jarFile.getJarEntry(classFile);
                if (entry != null) {
                    try (InputStream inputStream = jarFile.getInputStream(entry)) {
                        byte[] classData = readAllBytes(inputStream);
                        // Define the class from the byte array
                        return defineClass(name, classData, 0, classData.length);
                    }
                }
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        // If the class was not found in any JAR file, throw ClassNotFoundException
        throw new ClassNotFoundException(name);
    }

    /**
     * Reads all bytes from the given InputStream.
     * This method reads the entire content of the InputStream and returns it as a byte array.
     *
     * @param inputStream The InputStream to read from.
     * @return A byte array containing the data from the InputStream.
     * @throws IOException If an I/O error occurs while reading the stream.
     */
    private byte[] readAllBytes(InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        }
    }

    /**
     * Closes all open JAR files and releases any resources held by this class loader.
     * This method should be called when the class loader is no longer needed to avoid resource leaks.
     *
     * @throws IOException If an I/O error occurs while closing the JAR files.
     */
    @Override
    public void close() throws IOException {
        super.close();  // Call the superclass close method
    }
}
