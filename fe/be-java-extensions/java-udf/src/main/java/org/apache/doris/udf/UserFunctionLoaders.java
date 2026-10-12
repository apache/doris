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

package org.apache.doris.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds the classloader a user function is loaded from. There are two kinds and they differ only
 * in where the jar comes from; both hang off {@link UdfContractClassLoader}, which is what keeps a
 * user function from reaching into Doris.
 *
 * <ul>
 *   <li><b>Per function</b> - CREATE FUNCTION named a jar, BE downloaded it, and this function gets
 *       a loader of its own over that one file. It is closed when the function is dropped.</li>
 *   <li><b>Pre-installed</b> - CREATE FUNCTION named no jar, so the class is expected in
 *       {@code plugins/java_extensions} or the deprecated {@code custom_lib}. Those directories are
 *       read once, as they were when they were part of BE's start-up classpath: adding a jar to
 *       them still requires restarting BE.</li>
 * </ul>
 */
final class UserFunctionLoaders {

    private static final Logger LOG = LoggerFactory.getLogger(UserFunctionLoaders.class);

    /** Where a function with no jar of its own is looked for, in search order. */
    private static final String[] PREINSTALLED_DIRS = {"plugins/java_extensions", "custom_lib"};

    private static volatile ClassLoader contract;
    private static volatile ClassLoader preinstalled;

    private UserFunctionLoaders() {
    }

    /** A loader over one downloaded jar. The caller owns it and closes it with the function. */
    static URLClassLoader forJar(String jarPath) throws MalformedURLException, FileNotFoundException {
        File file = new File(jarPath);
        if (!file.exists()) {
            throw new FileNotFoundException("Can not find local file: " + jarPath);
        }
        return new UserFunctionClassLoader(new URL[] {file.toURI().toURL()}, contract());
    }

    /**
     * The shared loader over the pre-installed directories. Never closed - several functions are
     * loaded from it and there is no moment at which the last one is known to be gone.
     */
    static ClassLoader preinstalled() {
        ClassLoader current = preinstalled;
        if (current == null) {
            synchronized (UserFunctionLoaders.class) {
                if (preinstalled == null) {
                    List<URL> jars = preinstalledJars();
                    LOG.info("Java functions with no jar of their own are loaded from {} jar(s) in {}",
                            jars.size(), Arrays.toString(PREINSTALLED_DIRS));
                    preinstalled = new UserFunctionClassLoader(jars.toArray(new URL[0]), contract());
                }
                current = preinstalled;
            }
        }
        return current;
    }

    /** Where {@link #preinstalled} looks, in search order. */
    static String[] preinstalledDirs() {
        return PREINSTALLED_DIRS.clone();
    }

    private static ClassLoader contract() {
        ClassLoader current = contract;
        if (current == null) {
            synchronized (UserFunctionLoaders.class) {
                if (contract == null) {
                    contract = new UdfContractClassLoader(UserFunctionLoaders.class.getClassLoader());
                }
                current = contract;
            }
        }
        return current;
    }

    private static List<URL> preinstalledJars() {
        List<URL> jars = new ArrayList<>();
        String dorisHome = System.getenv("DORIS_HOME");
        if (dorisHome == null || dorisHome.trim().isEmpty()) {
            LOG.warn("DORIS_HOME is not set, so {} cannot be found; a Java function created without a"
                    + " jar will not resolve", Arrays.toString(PREINSTALLED_DIRS));
            return jars;
        }
        for (String dir : PREINSTALLED_DIRS) {
            File[] files = new File(dorisHome.trim(), dir).listFiles(
                    (unused, name) -> name.endsWith(".jar"));
            if (files == null) {
                continue;
            }
            // Sorted so that a duplicate class resolves the same way on every node, the same
            // reason the plugin directories themselves are sorted.
            Arrays.sort(files);
            for (File file : files) {
                try {
                    jars.add(file.toURI().toURL());
                } catch (MalformedURLException e) {
                    LOG.warn("Skipping {}: {}", file, e.toString());
                }
            }
        }
        return jars;
    }
}
