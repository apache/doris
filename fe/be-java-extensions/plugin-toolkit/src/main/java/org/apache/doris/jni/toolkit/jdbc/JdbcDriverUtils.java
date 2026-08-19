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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
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
 * up on its own; {@link #checksumVerifier}, which the JDBC scanner, writer and connection tester
 * all pass, is what turns that from a silent stale read into an error - and, once the operator
 * states the jar's new checksum, into a reload: an expectation that differs from the one the
 * cached loader was verified under discards it, because otherwise the check would report success
 * against bytes the process is not running.
 *
 * <p>Whether a jar has been checked is remembered SEPARATELY from the classloader, keyed by jar and
 * expected checksum rather than by jar and parent. Folding the two together looks equivalent and is
 * not: the connection tester runs first, under the same parent as the scanner, so once it had built
 * the loader every later scan took the cache's early return and never reached its own verifier -
 * one un-checksummed CREATE CATALOG disabled the check for that jar for the life of the process.
 */
public final class JdbcDriverUtils {

    /** Same 10s the executor this replaced used, for both connect and read. */
    private static final int CHECKSUM_TIMEOUT_MS = 10000;

    private static final ConcurrentHashMap<DriverKey, ClassLoader> DRIVER_CLASS_LOADERS =
            new ConcurrentHashMap<>();

    /** One lock per driver jar, so that loading two different drivers does not serialize. */
    private static final ConcurrentHashMap<DriverKey, Object> LOAD_LOCKS = new ConcurrentHashMap<>();

    /**
     * Jars already checked, as "<url>\0<expectation>". Not keyed by parent: the bytes behind a URL
     * are the same bytes whichever plugin asked, so one read answers for all of them - which is the
     * whole reason this is remembered at all. Two catalogs naming the same URL with DIFFERENT
     * checksums are two entries and both get checked; exactly one of them can pass.
     */
    private static final Set<String> VERIFIED = ConcurrentHashMap.newKeySet();

    /**
     * Per driver jar URL, the expectation the classloaders cached for it were built under.
     *
     * <p>This is what connects the two caches above, which are otherwise deliberately independent.
     * A new expectation for a URL that already has a loader means the operator replaced the jar in
     * place and told Doris its new checksum; the bytes just verified are then NOT the bytes the
     * cached loader was built from. Without this the check reports success against the current jar
     * while every query keeps using the old driver until BE restarts - a verification that passes
     * for a driver the process is not running.
     */
    private static final ConcurrentHashMap<String, String> LOADED_UNDER = new ConcurrentHashMap<>();

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

        /**
         * What this verifier expects of the jar, or null when it cannot say.
         *
         * <p>It is what lets "already checked" be remembered without holding on to the verifier:
         * two calls naming the same jar and the same expectation ask the same question, so the
         * second is skipped even when the classloader was built by somebody else. A verifier that
         * returns null keeps the older, weaker rule - it runs only when the classloader is created
         * - because nothing can tell two of them apart.
         */
        default String expectation() {
            return null;
        }
    }

    /**
     * The verifier Doris ships with: the MD5 the catalog definition carries, compared against the
     * jar actually behind the driver URL.
     *
     * <p>Returns {@code null} - "do not verify" - when the expected checksum is blank, which is
     * what a catalog defined without one produces. Only what Doris was told to expect is checked;
     * this never invents an expectation of its own.
     *
     * <p>The read is what makes it worth caching: {@code driverClassLoader} runs the verifier
     * exactly once per jar and parent, when the classloader for it is created, so a remote driver
     * jar is downloaded for checksumming once per process and not once per query.
     */
    public static DriverJarVerifier checksumVerifier(String expectedChecksum) {
        if (expectedChecksum == null || expectedChecksum.trim().isEmpty()) {
            return null;
        }
        String expected = expectedChecksum.trim();
        return new DriverJarVerifier() {
            @Override
            public void verify(URL driverJar) {
                String actual = md5Of(driverJar);
                if (!expected.equalsIgnoreCase(actual)) {
                    throw new IllegalStateException("Checksum mismatch for JDBC driver " + driverJar
                            + ": the catalog expects " + expected + " but the jar is " + actual
                            + ". The driver jar behind this URL is not the one the catalog was defined"
                            + " with; replace the jar or redefine the catalog with the new checksum");
                }
            }

            @Override
            public String expectation() {
                // Lower-cased because the comparison above is case-insensitive: the same MD5 in two
                // spellings is one question, and must not be asked twice.
                return expected.toLowerCase(Locale.ROOT);
            }
        };
    }

    private static String md5Of(URL driverJar) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            URLConnection connection = driverJar.openConnection();
            connection.setConnectTimeout(CHECKSUM_TIMEOUT_MS);
            connection.setReadTimeout(CHECKSUM_TIMEOUT_MS);
            try (InputStream in = connection.getInputStream()) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            StringBuilder hex = new StringBuilder(32);
            for (byte b : digest.digest()) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot checksum the JDBC driver at " + driverJar
                    + ": " + e.getMessage(), e);
        }
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
        ClassLoader cached = DRIVER_CLASS_LOADERS.get(key);
        if (cached != null) {
            // Before the early return, not after the cache miss: a cached loader says nothing about
            // whether THIS caller's expectation was ever checked against the jar.
            verifyOnce(key.driverUrl, verifier, false);
            // Re-read rather than return `cached`: verifyOnce discards the loaders for this jar when
            // the expectation it just checked is not the one they were built under, and returning
            // the loader read a moment ago would hand back exactly the stale driver that discovery
            // was for. A miss here falls through and builds a fresh loader from the new bytes.
            ClassLoader stillCached = DRIVER_CLASS_LOADERS.get(key);
            if (stillCached != null) {
                return stillCached;
            }
        }
        // Per-key lock plus a second look, rather than computeIfAbsent: verifying a driver jar
        // reads and checksums it and creating the loader opens it, and neither may run while a
        // ConcurrentHashMap bin lock is held - two catalogs whose keys share a bin would then
        // serialize on each other's jar download, and a nested load would be a recursive update.
        // What computeIfAbsent bought is kept: exactly one classloader per jar is ever published.
        synchronized (LOAD_LOCKS.computeIfAbsent(key, entry -> new Object())) {
            ClassLoader loaded = DRIVER_CLASS_LOADERS.get(key);
            // Before the loader exists, so that a jar that fails the check is neither opened nor
            // cached. Throwing here leaves nothing behind and the next request checks again.
            verifyOnce(key.driverUrl, verifier, loaded == null);
            if (loaded == null) {
                loaded = URLClassLoader.newInstance(new URL[] {key.driverUrl}, key.parent);
                DRIVER_CLASS_LOADERS.put(key, loaded);
            }
            return loaded;
        }
    }

    /**
     * Runs the verifier unless this exact question - this jar, this expectation - was already
     * answered in this process.
     *
     * @param loaderIsNew whether the classloader is about to be created, which is the only thing a
     *                    verifier that cannot state its expectation can be keyed on
     */
    private static void verifyOnce(URL driverJar, DriverJarVerifier verifier, boolean loaderIsNew) {
        if (verifier == null) {
            return;
        }
        String expectation = verifier.expectation();
        if (expectation == null) {
            if (loaderIsNew) {
                verifier.verify(driverJar);
            }
            return;
        }
        String token = driverJar.toString() + '\0' + expectation;
        if (VERIFIED.contains(token)) {
            return;
        }
        // Recorded after it passes, so a throw is not remembered as a pass. Two threads racing here
        // both read the jar once, which is a cheap price for not holding a lock across the read.
        verifier.verify(driverJar);
        VERIFIED.add(token);

        // A DIFFERENT expectation than the cached loaders for this jar were built under means the
        // jar behind the URL changed and the operator said so (ALTER CATALOG ... driver_checksum).
        // The bytes just verified are the new ones; the loaders hold the old ones. Drop them so the
        // next request rebuilds from what was actually checked.
        String previous = LOADED_UNDER.put(driverJar.toString(), expectation);
        if (previous != null && !previous.equals(expectation)) {
            dropLoaders(driverJar);
        }
    }

    /**
     * Forgets every cached classloader for one driver jar, whatever parent it was built under.
     *
     * <p>Deliberately does not close them: another query may still be resolving classes lazily
     * through one, and closing it would turn that into a NoClassDefFoundError. They go when the
     * last reference does. Shared with {@link #invalidate}, which is the same discard reached
     * deliberately rather than by a changed checksum.
     */
    private static void dropLoaders(URL driverJar) {
        String url = driverJar.toString();
        DRIVER_CLASS_LOADERS.keySet().removeIf(key -> key.driverUrl.toString().equals(url));
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
        DriverKey key = new DriverKey(toUrl(driverUrl), parent);
        DRIVER_CLASS_LOADERS.remove(key);
        // ...and re-verifies it, which is what this promises. The jar behind the URL may well be a
        // different one by now - that is a reason someone invalidates.
        String prefix = key.driverUrl.toString() + '\0';
        VERIFIED.removeIf(token -> token.startsWith(prefix));
        // Forgotten too, so that the next verification is a first one rather than a change: with a
        // stale entry left here, re-verifying the same expectation would look unchanged and
        // re-verifying a new one would drop loaders that no longer exist.
        LOADED_UNDER.remove(key.driverUrl.toString());
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
