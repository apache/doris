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

package org.apache.doris.cluster;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * Tests for {@link ClusterGuardFactory}.
 *
 * <p>The decision matrix under test:
 * <pre>
 *  Sentinel file | SPI impl | Expected behaviour
 *  ------------- | -------- | ------------------
 *  absent        | absent   | NoOpClusterGuard returned
 *  absent        | present  | real impl returned
 *  present       | present  | real impl returned
 *  present       | absent   | RuntimeException thrown (hard exit)
 * </pre>
 *
 * <p>Because {@link ClusterGuardFactory} uses a static singleton, the
 * {@code instance} field is reset via reflection before and after each test
 * to ensure isolation.
 *
 * <p>Sentinel-file simulation is achieved by injecting a custom
 * {@link ClassLoader} into the package-private {@code loadGuard(ClassLoader)}
 * overload. An in-process {@link URLStreamHandler} supplies a synthetic URL
 * that returns an empty byte stream, faithfully replicating a real zero-byte
 * marker file on the classpath.
 */
public class ClusterGuardFactoryTest {

    private Field instanceField;

    @Before
    public void setUp() throws Exception {
        instanceField = ClusterGuardFactory.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null); // reset singleton
    }

    @After
    public void tearDown() throws Exception {
        instanceField.set(null, null); // clean up after each test
    }

    // -----------------------------------------------------------------------
    // Decision-matrix scenarios
    // -----------------------------------------------------------------------

    /**
     * Scenario: sentinel absent, no SPI impl.
     * Community build with no extensions installed.
     * Expected: NoOpClusterGuard is returned silently.
     */
    @Test
    public void testNoSentinelNoImpl_returnsNoOp() {
        ClassLoader cl = new SentinelClassLoader(false, false);
        ClusterGuard guard = ClusterGuardFactory.loadGuard(cl);
        Assert.assertSame(NoOpClusterGuard.INSTANCE, guard);
    }

    /**
     * Scenario: sentinel absent, SPI impl present.
     * Community build with an optional cluster-guard plugin installed.
     * Expected: real implementation is returned (not NoOpClusterGuard).
     *
     * <p>Note: {@link ServiceLoader} always creates a fresh instance via the no-arg
     * constructor, so we verify the type rather than reference identity.
     */
    @Test
    public void testNoSentinelWithImpl_returnsImpl() {
        ClassLoader cl = new SentinelClassLoader(false, true);
        ClusterGuard guard = ClusterGuardFactory.loadGuard(cl);
        Assert.assertTrue(
                "Expected a StubClusterGuard instance, got: " + guard.getClass(),
                guard instanceof StubClusterGuard);
        Assert.assertNotSame(NoOpClusterGuard.INSTANCE, guard);
    }

    /**
     * Scenario: sentinel present, SPI impl present.
     * Correct distribution build — guard enforcement required and satisfied.
     * Expected: real implementation is returned, no exception thrown.
     *
     * <p>Note: {@link ServiceLoader} always creates a fresh instance via the no-arg
     * constructor, so we verify the type rather than reference identity.
     */
    @Test
    public void testSentinelPresentWithImpl_returnsImpl() {
        ClassLoader cl = new SentinelClassLoader(true, true);
        ClusterGuard guard = ClusterGuardFactory.loadGuard(cl);
        Assert.assertTrue(
                "Expected a StubClusterGuard instance, got: " + guard.getClass(),
                guard instanceof StubClusterGuard);
        Assert.assertNotSame(NoOpClusterGuard.INSTANCE, guard);
    }

    /**
     * Scenario: sentinel present, no SPI impl.
     * Distribution build where the implementation JAR is missing — misconfiguration.
     * Expected: RuntimeException is thrown so FE startup is aborted.
     */
    @Test
    public void testSentinelPresentNoImpl_throwsRuntimeException() {
        ClassLoader cl = new SentinelClassLoader(true, false);
        try {
            ClusterGuardFactory.loadGuard(cl);
            Assert.fail("Expected RuntimeException when sentinel is present but no impl found");
        } catch (RuntimeException e) {
            Assert.assertTrue(
                    "Error message should mention ClusterGuard",
                    e.getMessage().contains("ClusterGuard"));
        }
    }

    // -----------------------------------------------------------------------
    // Singleton behaviour
    // -----------------------------------------------------------------------

    @Test
    public void testGetGuardReturnsNonNull() {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertNotNull(guard);
    }

    @Test
    public void testGetGuardReturnsSameInstance() {
        ClusterGuard first = ClusterGuardFactory.getGuard();
        ClusterGuard second = ClusterGuardFactory.getGuard();
        Assert.assertSame(first, second);
    }

    @Test
    public void testGetGuardReturnsNoOpWhenNoSpiProviderFound() {
        // In the test classpath there is no META-INF/services/org.apache.doris.cluster.ClusterGuard
        // and no sentinel file, so the factory must fall back to NoOpClusterGuard.
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertSame(NoOpClusterGuard.INSTANCE, guard);
    }

    // -----------------------------------------------------------------------
    // NoOpClusterGuard behaviour
    // -----------------------------------------------------------------------

    @Test
    public void testNoOpGuardAllowsUnlimitedNodes() throws ClusterGuardException {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        guard.checkNodeLimit(0);
        guard.checkNodeLimit(100);
        guard.checkNodeLimit(Integer.MAX_VALUE);
    }

    @Test
    public void testNoOpGuardTimeValidityAlwaysPasses() throws ClusterGuardException {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        guard.checkTimeValidity();
    }

    @Test
    public void testNoOpGuardInfoIsEmptyJson() {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertEquals("{}", guard.getGuardInfo());
    }

    // -----------------------------------------------------------------------
    // Injection-based tests (simulate SPI via reflection)
    // -----------------------------------------------------------------------

    @Test
    public void testCustomGuardIsReturnedWhenInjected() throws Exception {
        ClusterGuard custom = new StubClusterGuard("custom-info");
        instanceField.set(null, custom);

        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertSame(custom, guard);
        Assert.assertEquals("custom-info", guard.getGuardInfo());
    }

    @Test
    public void testCustomGuardCheckNodeLimitEnforcesLimit() throws Exception {
        int limit = 3;
        ClusterGuard limitedGuard = new StubClusterGuard("{}") {
            @Override
            public void checkNodeLimit(int currentNodeCount) throws ClusterGuardException {
                if (currentNodeCount > limit) {
                    throw new ClusterGuardException(
                            "Node limit exceeded: max=" + limit + ", current=" + currentNodeCount);
                }
            }
        };
        instanceField.set(null, limitedGuard);

        ClusterGuard guard = ClusterGuardFactory.getGuard();
        guard.checkNodeLimit(3); // within limit — must not throw

        try {
            guard.checkNodeLimit(4);
            Assert.fail("Expected ClusterGuardException");
        } catch (ClusterGuardException e) {
            Assert.assertTrue(e.getMessage().contains("Node limit exceeded"));
        }
    }

    @Test
    public void testCustomGuardOnStartupPropagatesException() throws Exception {
        ClusterGuard failingGuard = new StubClusterGuard("{}") {
            @Override
            public void onStartup(String dorisHomeDir) throws ClusterGuardException {
                throw new ClusterGuardException("startup failed");
            }
        };
        instanceField.set(null, failingGuard);

        try {
            ClusterGuardFactory.getGuard().onStartup("/doris/home");
            Assert.fail("Expected ClusterGuardException");
        } catch (ClusterGuardException e) {
            Assert.assertEquals("startup failed", e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * A {@link ClassLoader} that can:
     * <ul>
     *   <li>Optionally expose a synthetic {@code META-INF/cluster-guard-required}
     *       resource (sentinel simulation).</li>
     *   <li>Optionally expose a {@link StubClusterGuard} via the standard
     *       {@code META-INF/services/} mechanism (SPI simulation).</li>
     * </ul>
     *
     * <p>{@link ServiceLoader} discovers SPI implementations by calling
     * {@link ClassLoader#getResources(String)} for the services registration file,
     * then reading each returned URL as a stream. Therefore we must override both
     * {@code findResources} (to inject the synthetic URL into the enumeration) and
     * provide a readable stream behind that URL via a custom {@link URLStreamHandler}.
     *
     * <p>All other resource and class lookups are delegated to the parent loader.
     */
    private static class SentinelClassLoader extends ClassLoader {

        private static final String SPI_FILE =
                "META-INF/services/" + ClusterGuard.class.getName();

        private final boolean hasSentinel;
        private final boolean hasImpl;

        SentinelClassLoader(boolean hasSentinel, boolean hasImpl) {
            super(ClusterGuardFactoryTest.class.getClassLoader());
            this.hasSentinel = hasSentinel;
            this.hasImpl = hasImpl;
        }

        /** Intercepts the sentinel probe done by {@link ClusterGuardFactory#loadGuard}. */
        @Override
        public URL getResource(String name) {
            if (ClusterGuardFactory.SENTINEL_RESOURCE.equals(name)) {
                return hasSentinel ? emptyUrl(name) : null;
            }
            return super.getResource(name);
        }

        /**
         * Intercepts the SPI services-file enumeration performed by {@link ServiceLoader}.
         * When {@code hasImpl} is true, inject a synthetic URL that delivers the stub
         * class name as its content; otherwise return only what the parent knows about.
         */
        @Override
        public java.util.Enumeration<URL> getResources(String name) throws IOException {
            if (SPI_FILE.equals(name)) {
                if (hasImpl) {
                    return java.util.Collections.enumeration(
                            java.util.Collections.singletonList(spiUrl()));
                }
                return java.util.Collections.emptyEnumeration();
            }
            return super.getResources(name);
        }

        // ------------------------------------------------------------------
        // Synthetic URL helpers
        // ------------------------------------------------------------------

        /**
         * A zero-byte URL signalling "this resource exists" — used for the sentinel.
         */
        private static URL emptyUrl(String path) {
            return makeUrl(path, new byte[0]);
        }

        /**
         * A URL whose content is the fully-qualified class name of the stub impl,
         * which is exactly what {@link ServiceLoader} reads from a services file.
         */
        private static URL spiUrl() {
            byte[] content = StubClusterGuard.class.getName().getBytes();
            return makeUrl(SPI_FILE, content);
        }

        private static URL makeUrl(String path, byte[] content) {
            try {
                URLStreamHandler handler = new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL u) {
                        return new URLConnection(u) {
                            @Override
                            public void connect() {
                            }

                            @Override
                            public InputStream getInputStream() {
                                return new ByteArrayInputStream(content);
                            }
                        };
                    }
                };
                return new URL("synthetic", "", -1, path, handler);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Minimal stub implementation used for injection-based and SPI-simulation tests.
     *
     * <p>The no-arg constructor is required so that {@link ServiceLoader} can
     * instantiate this class via reflection during SPI-simulation tests.
     */
    public static class StubClusterGuard implements ClusterGuard {

        private final String guardInfo;

        /** No-arg constructor required by {@link ServiceLoader}. */
        public StubClusterGuard() {
            this("stub");
        }

        StubClusterGuard(String guardInfo) {
            this.guardInfo = guardInfo;
        }

        @Override
        public void onStartup(String dorisHomeDir) throws ClusterGuardException {
            // no-op by default
        }

        @Override
        public void checkTimeValidity() throws ClusterGuardException {
            // no-op by default
        }

        @Override
        public void checkNodeLimit(int currentNodeCount) throws ClusterGuardException {
            // unlimited by default
        }

        @Override
        public String getGuardInfo() {
            return guardInfo;
        }
    }
}
