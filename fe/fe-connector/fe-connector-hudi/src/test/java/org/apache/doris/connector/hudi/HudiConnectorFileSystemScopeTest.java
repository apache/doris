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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The lifecycle of the per-configuration {@code UserGroupInformation} that keeps two catalogs from
 * sharing one cached {@code FileSystem}.
 *
 * <p>This is a destructive mechanism - the last release closes every filesystem Hadoop cached under
 * the UGI - so what it must never do is act on somebody else's entry. The cases below pin the two
 * halves of that: a connector acquires exactly one hold and releases exactly the hold it took, and a
 * configuration shared by two catalogs survives until BOTH of them are closed.
 *
 * <p>Each case uses a marker property of its own, so the keys are disjoint and the shared static map
 * cannot carry state from one case into the next.
 */
public class HudiConnectorFileSystemScopeTest {

    @Test
    public void twoCatalogsOnOneConfigurationShareOneScopeAndTheLastCloseReleasesIt() throws Exception {
        Map<String, String> props = propertiesFor("shared");
        String key = HudiConnector.fileSystemScopeKey(props);
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "nothing holds this key yet");

        HudiConnector first = connector(props, 1L);
        HudiConnector second = connector(props, 2L);

        UserGroupInformation firstScope = first.fileSystemScope();
        UserGroupInformation secondScope = second.fileSystemScope();
        Assertions.assertNotNull(firstScope, "a non-Kerberos catalog must get a scope of its own");
        Assertions.assertSame(firstScope, secondScope,
                "two catalogs defined on byte-identical properties may share a filesystem, so they share the "
                        + "UGI that keys it - one per configuration, not one per catalog");
        Assertions.assertEquals(2, HudiConnector.scopeOwners(key), "both connectors hold it");

        first.close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "the entry outlives the first close: the other catalog is still reading through it");
        Assertions.assertSame(secondScope, second.fileSystemScope(),
                "closing a sibling must not take the surviving catalog's scope away");

        second.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key),
                "the last holder releases the entry, which is what closes the filesystems cached under it");
    }

    @Test
    public void connectorAcquiresOneHoldNoMatterHowOftenItIsAsked() throws Exception {
        Map<String, String> props = propertiesFor("memoized");
        String key = HudiConnector.fileSystemScopeKey(props);
        HudiConnector connector = connector(props, 3L);

        UserGroupInformation scope = connector.fileSystemScope();
        for (int i = 0; i < 5; i++) {
            Assertions.assertSame(scope, connector.fileSystemScope(), "the scope is memoized per connector");
        }
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "the hold is taken when the scope is built, not on every read - otherwise the count could "
                        + "never come back down");

        connector.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "one hold, one release");
    }

    @Test
    public void closingTwiceReleasesOnce() throws Exception {
        Map<String, String> props = propertiesFor("double-close");
        String key = HudiConnector.fileSystemScopeKey(props);

        HudiConnector holder = connector(props, 4L);
        HudiConnector closedTwice = connector(props, 5L);
        holder.fileSystemScope();
        closedTwice.fileSystemScope();
        Assertions.assertEquals(2, HudiConnector.scopeOwners(key));

        closedTwice.close();
        closedTwice.close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "a second close must not decrement again - it would close the filesystems the other catalog "
                        + "is still reading through");

        holder.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key));
    }

    @Test
    public void connectorThatNeverBuiltAScopeReleasesNothing() throws Exception {
        Map<String, String> props = propertiesFor("never-used");
        String key = HudiConnector.fileSystemScopeKey(props);

        HudiConnector user = connector(props, 6L);
        user.fileSystemScope();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key));

        // The throwaway connector CatalogFactory builds during checkWhenCreating is exactly this: created,
        // never queried, closed. It must not touch an entry it never took.
        connector(props, 7L).close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "closing a connector that never computed a scope must leave the live entry alone");

        user.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key));
    }

    @Test
    public void closedConnectorDoesNotAcquireAgain() throws Exception {
        Map<String, String> props = propertiesFor("closed-then-asked");
        String key = HudiConnector.fileSystemScopeKey(props);

        HudiConnector connector = connector(props, 8L);
        connector.close();

        // A statement still holding a connector the FE has replaced can reach this. Acquiring here would
        // take a hold nothing is ever going to release, since close() has already happened.
        Assertions.assertNull(connector.fileSystemScope(),
                "after close the connector falls back to the FE-injected authenticator");
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "and it takes no new hold");
    }

    @Test
    public void differentConfigurationsGetDifferentScopes() throws Exception {
        Map<String, String> left = propertiesFor("distinct-left");
        Map<String, String> right = propertiesFor("distinct-right");
        Assertions.assertNotEquals(HudiConnector.fileSystemScopeKey(left),
                HudiConnector.fileSystemScopeKey(right),
                "the key is the configuration digest, so different properties are different keys");

        HudiConnector one = connector(left, 9L);
        HudiConnector other = connector(right, 10L);
        Assertions.assertNotSame(one.fileSystemScope(), other.fileSystemScope(),
                "two catalogs that may NOT share a filesystem must not share the UGI that keys it");

        one.close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(HudiConnector.fileSystemScopeKey(right)),
                "releasing one configuration says nothing about another");
        other.close();
    }

    @Test
    public void theKeyIsTheConfigurationAndNotTheCatalogItBelongsTo() throws Exception {
        Map<String, String> props = propertiesFor("id-independent");
        // Two catalog ids, one configuration. Prefixing the key with the id would double the UGIs, the
        // filesystems cached under them and the SDK client threads this whole mechanism exists to bound.
        HudiConnector low = connector(props, 11L);
        HudiConnector high = connector(props, 4242L);
        Assertions.assertSame(low.fileSystemScope(), high.fileSystemScope());
        low.close();
        high.close();
    }

    @Test
    public void theLastReleaseClosesTheFilesystemsOffTheCallersThread() throws Exception {
        // close() is reached from the FE's journal replay thread: ALTER CATALOG replays under
        // CatalogMgr.writeLock() (one process-wide lock over EVERY catalog) inside the same
        // synchronized(this) that makeSureInitialized() needs, and DROP CATALOG - which holds neither - is
        // still that same thread, which a follower may never let fall behind. The teardown itself is
        // unbounded: FileSystem.closeAll(UGI) holds the process-wide FileSystem.CACHE monitor throughout, and
        // one S3AFileSystem.close() spends up to ~180s shutting three pools down. So the release must settle
        // the count and hand the filesystems off, not close them inline.
        // MUTATION: call FileSystem.closeAllForUGI(orphaned) on the calling thread -> assertNotSame red.
        Map<String, String> props = propertiesFor("async-teardown");
        String key = HudiConnector.fileSystemScopeKey(props);
        HudiConnector connector = connector(props, 12L);

        UserGroupInformation scope = connector.fileSystemScope();
        Assertions.assertNotNull(scope, "a non-Kerberos catalog must get a scope of its own");
        RecordingFileSystem cached = cacheFileSystemUnder(scope);

        Thread caller = Thread.currentThread();
        connector.close();

        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "the last holder released the entry");
        Assertions.assertTrue(cached.closed.await(60, TimeUnit.SECONDS),
                "the orphaned UGI's filesystems must still be closed - asynchronously, but not never");
        Assertions.assertNotSame(caller, cached.closingThread,
                "the teardown must not run on the thread that called close() - on the ALTER path that is the "
                        + "journal replay thread, holding the CatalogMgr write lock");
        Assertions.assertEquals("hudi-fs-scope-closer", cached.closingThread.getName(),
                "and it must be the dedicated closer, so a stuck teardown is identifiable in a thread dump");
    }

    @Test
    public void releaseThatIsNotTheLastClosesNothing() throws Exception {
        // The count is what decides. A catalog sharing a configuration with another one keeps its
        // filesystems until both are gone - closing them on the first release would break the survivor.
        Map<String, String> props = propertiesFor("shared-teardown");
        HudiConnector first = connector(props, 13L);
        HudiConnector second = connector(props, 14L);
        UserGroupInformation scope = first.fileSystemScope();
        Assertions.assertSame(scope, second.fileSystemScope(), "one configuration, one scope, two holders");
        RecordingFileSystem cached = cacheFileSystemUnder(scope);

        first.close();

        Assertions.assertFalse(cached.closed.await(2, TimeUnit.SECONDS),
                "the surviving catalog still reads through these filesystems");

        second.close();
        Assertions.assertTrue(cached.closed.await(60, TimeUnit.SECONDS),
                "the last release hands them off");
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────────

    /** The scheme the recording filesystem is bound to; disjoint from every real one. */
    private static final String SCHEME = "hudi-fs-scope-test";

    /**
     * Puts one {@link RecordingFileSystem} into Hadoop's global {@code FileSystem.CACHE} keyed by
     * {@code scope}, which is exactly what {@code FileSystem.closeAllForUGI(scope)} then acts on. The
     * {@code doAs} matters: the cache key carries {@code UserGroupInformation.getCurrentUser()}.
     */
    private static RecordingFileSystem cacheFileSystemUnder(UserGroupInformation scope) throws Exception {
        Configuration conf = new Configuration();
        conf.setClass("fs." + SCHEME + ".impl", RecordingFileSystem.class, FileSystem.class);
        URI uri = URI.create(SCHEME + "://scope/");
        FileSystem fs = scope.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(uri, conf));
        Assertions.assertInstanceOf(RecordingFileSystem.class, fs,
                "the fs.<scheme>.impl binding must win, or this case would be testing nothing");
        return (RecordingFileSystem) fs;
    }

    /**
     * A cacheable Hadoop {@code FileSystem} that records WHERE its {@code close()} ran. Everything else
     * throws: nothing in these cases reads or writes through it, and a silent no-op would hide a case that
     * accidentally started to.
     */
    public static final class RecordingFileSystem extends FileSystem {
        final CountDownLatch closed = new CountDownLatch(1);
        volatile Thread closingThread;
        private URI uri;
        private Path workingDirectory = new Path("/");

        @Override
        public void initialize(URI name, Configuration conf) throws IOException {
            super.initialize(name, conf);
            this.uri = name;
        }

        @Override
        public void close() throws IOException {
            closingThread = Thread.currentThread();
            try {
                super.close();
            } finally {
                // Last, so the awaiting test sees both fields published (the latch is the happens-before).
                closed.countDown();
            }
        }

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public URI getUri() {
            return uri;
        }

        @Override
        public Path getWorkingDirectory() {
            return workingDirectory;
        }

        @Override
        public void setWorkingDirectory(Path dir) {
            this.workingDirectory = dir;
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize) {
            throw new UnsupportedOperationException("no case reads through this filesystem");
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                short replication, long blockSize, Progressable progress) {
            throw new UnsupportedOperationException("no case writes through this filesystem");
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) {
            throw new UnsupportedOperationException("no case writes through this filesystem");
        }

        @Override
        public boolean rename(Path src, Path dst) {
            throw new UnsupportedOperationException("no case writes through this filesystem");
        }

        @Override
        public boolean delete(Path f, boolean recursive) {
            throw new UnsupportedOperationException("no case writes through this filesystem");
        }

        @Override
        public FileStatus[] listStatus(Path f) {
            throw new UnsupportedOperationException("no case lists through this filesystem");
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission) {
            throw new UnsupportedOperationException("no case writes through this filesystem");
        }

        @Override
        public FileStatus getFileStatus(Path f) {
            throw new UnsupportedOperationException("no case stats through this filesystem");
        }
    }


    /** The minimal catalog properties plus a marker, so each case owns a disjoint key. */
    private static Map<String, String> propertiesFor(String marker) {
        Map<String, String> props = HudiTestProperties.minimalMap();
        props.put("hudi.fs.scope.test.marker", marker);
        return props;
    }

    private static HudiConnector connector(Map<String, String> props, long catalogId) {
        return new HudiConnector(props, new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "catalog_" + catalogId;
            }

            @Override
            public long getCatalogId() {
                return catalogId;
            }
        });
    }
}
