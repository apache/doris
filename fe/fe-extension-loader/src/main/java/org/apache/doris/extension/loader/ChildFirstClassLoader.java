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

package org.apache.doris.extension.loader;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Child-first classloader with parent-first allowlist.
 *
 * <p>Both class loading and resource lookup are child-first: the plugin's own JARs are
 * searched first, and the parent classloader is consulted only when the child has no
 * result.  This prevents third-party service-registration files in the FE parent
 * classloader (e.g. {@code software/amazon/awssdk/.../execution.interceptors} from
 * {@code s3-transfer-manager.jar}) from leaking into the plugin's view and triggering
 * classloader split-brain errors.
 */
public class ChildFirstClassLoader extends URLClassLoader {

    public static final List<String> DEFAULT_PARENT_FIRST_PACKAGES;

    static {
        List<String> packages = new ArrayList<>();
        packages.add("java.");
        packages.add("javax.");
        packages.add("sun.");
        packages.add("com.sun.");
        packages.add("org.slf4j.");
        packages.add("org.apache.logging.");
        packages.add("org.apache.doris.extension.spi.");
        packages.add("org.apache.doris.connector.api.");
        DEFAULT_PARENT_FIRST_PACKAGES = Collections.unmodifiableList(packages);
    }

    private final List<String> parentFirstPackages;

    public ChildFirstClassLoader(URL[] urls, ClassLoader parent, List<String> parentFirstPackages) {
        super(urls, parent);
        this.parentFirstPackages = parentFirstPackages != null
                ? Collections.unmodifiableList(new ArrayList<>(parentFirstPackages))
                : DEFAULT_PARENT_FIRST_PACKAGES;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded != null) {
                return loaded;
            }
            if (isParentFirst(name)) {
                return super.loadClass(name, resolve);
            }
            try {
                Class<?> clazz = findClass(name);
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            } catch (ClassNotFoundException ignored) {
                return super.loadClass(name, resolve);
            }
        }
    }

    private boolean isParentFirst(String className) {
        for (String prefix : parentFirstPackages) {
            if (className.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Override resource lookup to be child-first: return only this classloader's own
     * resources.  Fall back to the parent only when the child has none.
     *
     * <p>The standard {@link URLClassLoader#getResources} returns resources from
     * <em>both</em> child and parent.  That causes problems when the parent has
     * service-registration files (e.g. AWS SDK {@code execution.interceptors}) whose
     * implementation classes cannot be cast to the corresponding interfaces loaded by
     * the child classloader.
     */
    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Enumeration<URL> childResources = findResources(name);
        if (childResources.hasMoreElements()) {
            return childResources;
        }
        return super.getResources(name);
    }
}
