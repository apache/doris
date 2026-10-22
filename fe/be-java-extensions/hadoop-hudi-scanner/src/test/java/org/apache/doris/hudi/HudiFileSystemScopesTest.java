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

package org.apache.doris.hudi;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The lifecycle of the per-configuration UGI the hudi scanner reads under.
 *
 * <p>The mechanism is destructive - a sweep closes every filesystem Hadoop cached under a UGI - so
 * what it must never do is close a filesystem a scanner is reading through, and what it must
 * eventually do is close the ones nobody is. Every case here drives the clock and the sweep by hand;
 * the process-wide instance's timer thread is not involved.
 */
public class HudiFileSystemScopesTest {

    private static final long TTL = TimeUnit.MINUTES.toNanos(10);

    private final AtomicLong clock = new AtomicLong();
    private final HudiFileSystemScopes scopes = new HudiFileSystemScopes(clock::get, TTL);

    @Test
    public void scannersOnOneConfigurationShareOneScope() {
        HudiFileSystemScopes.Hold first = scopes.acquire("k", "hadoop");
        HudiFileSystemScopes.Hold second = scopes.acquire("k", "hadoop");
        Assertions.assertSame(first.ugi(), second.ugi(),
                "one configuration, one UGI: that is what lets two scanners share a cached filesystem");
        Assertions.assertEquals(2, scopes.owners("k"));
        Assertions.assertNotSame(first.ugi(), scopes.acquire("other", "hadoop").ugi(),
                "different configurations must not share the UGI that keys their filesystems");
        Assertions.assertEquals("hadoop", first.ugi().getUserName(),
                "the scope is a second Subject for the same user, not another user");
    }

    @Test
    public void heldScopeIsNeverSwept() throws Exception {
        HudiFileSystemScopes.Hold held = scopes.acquire("k", "hadoop");
        RecordingFileSystem cached = cacheFileSystemUnder(held.ugi());

        clock.addAndGet(TTL * 3);
        Assertions.assertEquals(0, scopes.sweep(), "a scope with a holder is not idle, however old");
        Assertions.assertTrue(scopes.holds("k"));
        Assertions.assertFalse(cached.isClosed(), "the scan still reads through this filesystem");
    }

    @Test
    public void idleScopeIsSweptOnlyAfterTheTtl() throws Exception {
        HudiFileSystemScopes.Hold scope = scopes.acquire("k", "hadoop");
        RecordingFileSystem cached = cacheFileSystemUnder(scope.ugi());
        scope.release();

        clock.addAndGet(TTL - 1);
        Assertions.assertEquals(0, scopes.sweep(), "released, but not for long enough");
        Assertions.assertFalse(cached.isClosed());

        clock.addAndGet(1);
        Assertions.assertEquals(1, scopes.sweep());
        Assertions.assertFalse(scopes.holds("k"), "the entry is gone");
        Assertions.assertTrue(cached.isClosed(), "and so is what Hadoop cached under its UGI");
    }

    @Test
    public void holdTakenDuringTheIdleWindowKeepsTheScope() throws Exception {
        HudiFileSystemScopes.Hold first = scopes.acquire("k", "hadoop");
        RecordingFileSystem cached = cacheFileSystemUnder(first.ugi());
        first.release();
        clock.addAndGet(TTL / 2);

        // The next query arrives before the window closes: same UGI, same cached filesystem.
        HudiFileSystemScopes.Hold second = scopes.acquire("k", "hadoop");
        Assertions.assertSame(first.ugi(), second.ugi());
        clock.addAndGet(TTL);
        Assertions.assertEquals(0, scopes.sweep(), "held again, so not idle");
        Assertions.assertFalse(cached.isClosed());

        // Idleness is measured from the LAST release, not the first.
        second.release();
        clock.addAndGet(TTL - 1);
        Assertions.assertEquals(0, scopes.sweep());
        clock.addAndGet(1);
        Assertions.assertEquals(1, scopes.sweep());
        Assertions.assertTrue(cached.isClosed());
    }

    @Test
    public void scanArrivingAfterTheSweepGetsAFreshUgiAndNeverTheClosedFilesystems() throws Exception {
        HudiFileSystemScopes.Hold old = scopes.acquire("k", "hadoop");
        RecordingFileSystem closed = cacheFileSystemUnder(old.ugi());
        old.release();
        clock.addAndGet(TTL);
        Assertions.assertEquals(1, scopes.sweep());

        HudiFileSystemScopes.Hold fresh = scopes.acquire("k", "hadoop");
        Assertions.assertNotSame(old.ugi(), fresh.ugi(),
                "UGI equality is Subject identity: a fresh UGI keys fresh cache entries");
        FileSystem reopened = fileSystemUnder(fresh.ugi());
        Assertions.assertNotSame(closed, reopened, "the closed filesystem is not handed to the new scan");
        Assertions.assertFalse(((RecordingFileSystem) reopened).isClosed());
    }

    @Test
    public void releasingAHoldTwiceDoesNotFreeSomebodyElses() {
        HudiFileSystemScopes.Hold mine = scopes.acquire("k", "hadoop");
        scopes.acquire("k", "hadoop");
        mine.release();
        mine.release();
        Assertions.assertEquals(1, scopes.owners("k"),
                "a scanner's close may run twice; the other scanner's hold must survive it");
    }

    /**
     * The race the design has to survive: the sweep deciding a scope is idle at the same moment a
     * scan takes a hold on it. Whatever the interleaving, the scan must end up with a UGI whose
     * filesystems are not the ones being closed - either the hold lands first and the sweep skips
     * the entry, or the sweep removed it first and the scan builds a fresh one.
     */
    @Test
    public void scanRacingTheSweepIsNeverStranded() throws Exception {
        for (int round = 0; round < 50; round++) {
            String key = "race-" + round;
            HudiFileSystemScopes.Hold old = scopes.acquire(key, "hadoop");
            RecordingFileSystem cached = cacheFileSystemUnder(old.ugi());
            old.release();
            clock.addAndGet(TTL);

            CyclicBarrier start = new CyclicBarrier(2);
            AtomicReference<HudiFileSystemScopes.Hold> taken = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread scan = new Thread(() -> {
                try {
                    start.await();
                    taken.set(scopes.acquire(key, "hadoop"));
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            scan.start();
            start.await();
            scopes.sweep();
            scan.join(TimeUnit.SECONDS.toMillis(30));
            Assertions.assertNull(failure.get());
            HudiFileSystemScopes.Hold live = taken.get();
            Assertions.assertNotNull(live, "the acquire must complete");

            Assertions.assertEquals(1, scopes.owners(key), "whichever won, the scan holds a live entry");
            if (live.ugi() == old.ugi()) {
                Assertions.assertFalse(cached.isClosed(),
                        "the hold landed first: the sweep must have left the filesystems alone");
            } else {
                Assertions.assertTrue(cached.isClosed(),
                        "the sweep landed first: it closed the old set and the scan got a fresh UGI");
            }
            // A sweep after the race changes nothing: the scan still holds its scope.
            clock.addAndGet(TTL);
            Assertions.assertEquals(0, scopes.sweep());
            live.release();
        }
    }

    @Test
    public void eachSweepCountsOnlyWhatItClosed() throws Exception {
        List<RecordingFileSystem> cached = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            HudiFileSystemScopes.Hold scope = scopes.acquire("k" + i, "hadoop");
            cached.add(cacheFileSystemUnder(scope.ugi()));
            scope.release();
        }
        HudiFileSystemScopes.Hold held = scopes.acquire("held", "hadoop");
        RecordingFileSystem heldFs = cacheFileSystemUnder(held.ugi());

        clock.addAndGet(TTL);
        Assertions.assertEquals(3, scopes.sweep());
        for (RecordingFileSystem fs : cached) {
            Assertions.assertTrue(fs.isClosed());
        }
        Assertions.assertFalse(heldFs.isClosed());
        Assertions.assertEquals(0, scopes.sweep(), "nothing left to close");
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────

    /** The scheme the recording filesystem is bound to; disjoint from every real one. */
    private static final String SCHEME = "hudi-fs-scope-test";

    /**
     * Puts one {@link RecordingFileSystem} into Hadoop's global {@code FileSystem.CACHE} keyed by
     * {@code ugi}, which is exactly what {@code FileSystem.closeAllForUGI(ugi)} then acts on. The
     * {@code doAs} matters: the cache key carries {@code UserGroupInformation.getCurrentUser()}.
     */
    private static RecordingFileSystem cacheFileSystemUnder(UserGroupInformation ugi) throws Exception {
        FileSystem fs = fileSystemUnder(ugi);
        Assertions.assertInstanceOf(RecordingFileSystem.class, fs,
                "the fs.<scheme>.impl binding must win, or this case would be testing nothing");
        Assertions.assertFalse(((RecordingFileSystem) fs).isClosed(),
                "a closed filesystem must never come out of the cache");
        return (RecordingFileSystem) fs;
    }

    private static FileSystem fileSystemUnder(UserGroupInformation ugi) throws Exception {
        Configuration conf = new Configuration();
        conf.setClass("fs." + SCHEME + ".impl", RecordingFileSystem.class, FileSystem.class);
        URI uri = URI.create(SCHEME + "://scope/");
        return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(uri, conf));
    }

    /**
     * A cacheable Hadoop {@code FileSystem} that records that it was closed. Everything else throws:
     * nothing in these cases reads or writes through it, and a silent no-op would hide a case that
     * accidentally started to.
     */
    public static final class RecordingFileSystem extends FileSystem {
        private final CountDownLatch closed = new CountDownLatch(1);
        private URI uri;
        private Path workingDirectory = new Path("/");

        boolean isClosed() {
            return closed.getCount() == 0;
        }

        @Override
        public void initialize(URI name, Configuration conf) throws IOException {
            super.initialize(name, conf);
            this.uri = name;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
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
}
