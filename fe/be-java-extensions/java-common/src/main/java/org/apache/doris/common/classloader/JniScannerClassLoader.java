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

package org.apache.doris.common.classloader;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Vector;

/**
 * A child-first URLClassLoader with parent-first exceptions for platform classes,
 * and child-first resource lookup.
 *
 * Class loading order:
 * 1. Return already loaded class from this loader.
 * 2. Delegate well-known JDK/platform classes to parent (parent-first).
 * 3. Try to find the class from this loader's URLs (child-first).
 * 4. Fallback to parent if not found locally.
 *
 * Resource lookup order:
 * - getResource: child-first, then parent.
 * - getResources: merge in order child -> parent.
 *
 * Notes:
 * - Child-first for non-platform classes prioritizes extension jars.
 * - Parent-first for platform namespaces avoids linkage conflicts.
 * - Resource overrides make SPI (META-INF/services) honor child-first.
 */
public class JniScannerClassLoader extends URLClassLoader {

    /**
     * Constructs the JniScannerClassLoader.
     *
     * @param name   logical loader name (for caller-side identification)
     * @param urls   classpath URLs for this loader
     * @param parent parent ClassLoader to delegate to
     */
    public JniScannerClassLoader(String name, URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    /**
     * Child-first loading with parent-first exception for platform classes.
     */
    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // 1) Return already loaded class
        Class<?> clazz = findLoadedClass(name);
        if (clazz != null) {
            return clazz;
        }

        // 2) Parent-first for platform/framework namespaces (including Trino/Hadoop)
        if (isPlatformClass(name)) {
            return super.loadClass(name, resolve);
        }

        // 3) Child-first: try to load from this loader's URLs
        try {
            clazz = findClass(name);
            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        } catch (ClassNotFoundException ignored) {
            // Continue to parent
        }

        // 4) Fallback to parent ClassLoader
        return super.loadClass(name, resolve);
    }

    /**
     * Child-first single resource lookup: try local, then parent.
     */
    @Override
    public URL getResource(String name) {
        URL url = findResource(name);
        if (url == null && getParent() != null) {
            url = getParent().getResource(name);
        }
        return url;
    }

    /**
     * Child-first resource enumeration: merge child then parent.
     */
    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Vector<URL> all = new Vector<>();

        Enumeration<URL> child = findResources(name);
        while (child.hasMoreElements()) {
            all.add(child.nextElement());
        }

        if (getParent() != null) {
            Enumeration<URL> parent = getParent().getResources(name);
            while (parent.hasMoreElements()) {
                all.add(parent.nextElement());
            }
        }

        return all.elements();
    }

    /**
     * Platform namespaces that must remain parent-first to avoid conflicts.
     */
    private boolean isPlatformClass(String name) {
        return name.startsWith("java.")
                || name.startsWith("javax.")
                || name.startsWith("sun.")
                || name.startsWith("jdk.")
                || name.startsWith("org.w3c.")
                || name.startsWith("org.xml.")
                // Trino core/server APIs
                || name.startsWith("io.trino.")
                // Common frameworks shared with app loader
                || name.startsWith("com.google.inject.")
                || name.startsWith("javax.inject.")
                || name.startsWith("com.fasterxml.jackson.")
                || name.startsWith("org.slf4j.")
                || name.startsWith("ch.qos.logback.")
                || name.startsWith("org.apache.logging.log4j.")
                || name.startsWith("org.apache.commons.")
                || name.startsWith("com.google.common.");
    }
}
