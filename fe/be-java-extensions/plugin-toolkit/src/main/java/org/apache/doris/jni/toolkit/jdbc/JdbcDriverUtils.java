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

package org.apache.doris.jni.toolkit.jdbc;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads a user-supplied JDBC driver jar into a classloader of its own.
 *
 * <h2>Why the driver gets its own classloader</h2>
 *
 * <p>The jar is named by the catalog and downloaded at query time, so it cannot be part of any
 * plugin's build. Giving it a child of the plugin's classloader means the driver can see the JDBC
 * API and the plugin's classes, the plugin cannot accidentally compile against driver internals,
 * and two catalogs pointing at different drivers stay independent.
 *
 * <h2>Why the classloader is cached</h2>
 *
 * <p>One per driver jar, for the life of the process. Building a fresh one per scan looks harmless
 * but is not: connection pools outlive a single scan, so a pooled connection keeps the driver
 * classes from the loader that created it while the next scan loads a second copy of the same
 * classes. Everything then works until something compares two driver types and finds them
 * unrelated. Caching also bounds a leak that is otherwise proportional to query count - a
 * classloader holding a jar's worth of classes is not cheap.
 *
 * <p>The cache is keyed by jar <em>and</em> parent, because "the same jar under two plugins" is two
 * different driver worlds. Note that a driver jar replaced in place at the same URL is not picked
 * up until BE restarts; verifying the jar (see {@link DriverJarVerifier}) is what turns that from a
 * silent stale read into an error.
 */
public final class JdbcDriverUtils {

    private static final ConcurrentHashMap<DriverKey, ClassLoader> DRIVER_CLASS_LOADERS =
            new ConcurrentHashMap<>();

    private JdbcDriverUtils() {
    }

    /**
     * Checks a driver jar before it is loaded for the first time. Callers that know what the jar
     * should be - Doris ships an MD5 with the catalog definition - pass one; the check then runs
     * exactly once per jar, when its classloader is created, rather than on every query.
     */
    public interface DriverJarVerifier {
        /** Throws to reject the jar; the classloader is then not created or cached. */
        void verify(URL driverJar);
    }

    /** The classloader for one driver jar, creating it on first use. */
    public static ClassLoader driverClassLoader(String driverUrl, ClassLoader parent) {
        return driverClassLoader(driverUrl, parent, null);
    }

    /**
     * @param driverUrl where the driver jar lives, as a URL
     * @param parent    the classloader of the code that will use the driver
     * @param verifier  optional; runs once, before the classloader for this jar exists
     */
    public static ClassLoader driverClassLoader(String driverUrl, ClassLoader parent,
            DriverJarVerifier verifier) {
        DriverKey key = new DriverKey(toUrl(driverUrl), parent);
        // computeIfAbsent, not get-then-put: the verifier must run once and exactly one classloader
        // per jar may ever be published, or the problem described above comes back through the
        // race instead of through the retry.
        return DRIVER_CLASS_LOADERS.computeIfAbsent(key, entry -> {
            if (verifier != null) {
                verifier.verify(entry.driverUrl);
            }
            return URLClassLoader.newInstance(new URL[] {entry.driverUrl}, entry.parent);
        });
    }

    /**
     * Forgets the classloader for one driver jar, so the next request builds and re-verifies it.
     *
     * <p>For the case where the driver loaded but turned out to be unusable - a wrong jar for the
     * database, say. Without this the first failure would be cached for the life of the process and
     * fixing the catalog would not help until restart. The classloader is deliberately not closed:
     * another query may still be using classes from it, and closing it would turn their next lazy
     * class resolution into a NoClassDefFoundError.
     */
    public static void invalidate(String driverUrl, ClassLoader parent) {
        DRIVER_CLASS_LOADERS.remove(new DriverKey(toUrl(driverUrl), parent));
    }

    private static URL toUrl(String driverUrl) {
        Objects.requireNonNull(driverUrl, "driverUrl");
        try {
            return new URL(driverUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid JDBC driver URL: " + driverUrl, e);
        }
    }

    private static final class DriverKey {
        private final URL driverUrl;
        private final ClassLoader parent;

        private DriverKey(URL driverUrl, ClassLoader parent) {
            this.driverUrl = driverUrl;
            this.parent = parent;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DriverKey)) {
                return false;
            }
            DriverKey that = (DriverKey) other;
            // Identity for the parent: classloaders have no value equality, and two distinct
            // loaders are two distinct class spaces even if they were built from the same jars.
            // toString on URL is deliberate too - URL.equals resolves host names.
            return this.parent == that.parent
                    && this.driverUrl.toString().equals(that.driverUrl.toString());
        }

        @Override
        public int hashCode() {
            return 31 * driverUrl.toString().hashCode() + System.identityHashCode(parent);
        }
    }
}
