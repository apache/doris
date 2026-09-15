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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * One {@link UserGroupInformation} per distinct filesystem configuration, held by the scanners
 * reading through it and closed - together with every filesystem Hadoop cached under it - once
 * nobody has for a while.
 *
 * <p>What the UGI is for. {@link HadoopHudiJniScanner} keeps Hadoop's {@code FileSystem} cache ON,
 * because with it off every {@code FileSystem.get()} inside hudi-hadoop-mr built a fresh
 * S3AFileSystem and a fresh AWS SDK client that nobody closed (see the scanner). A cached filesystem
 * lives in {@code FileSystem.CACHE}, a static strong map keyed on (scheme, authority, UGI, and the
 * {@code doris.fs.cache.key.<scheme>} fingerprint of the Doris-patched class), and the only handle
 * Hadoop offers on "everything one configuration opened" is the UGI:
 * {@link FileSystem#closeAllForUGI}. So each configuration reads under a UGI of its own - a second
 * {@code createRemoteUser} Subject for the same user name, not another user - which makes its
 * filesystems closable as a set. The credential separation itself no longer depends on this: the
 * plugin carries the patched {@code FileSystem} (hadoop-deps), whose cache key already tells two
 * catalogs apart by the fingerprint FE sends.
 *
 * <p>Why it is bounded. The first version of this map was never evicted, so every configuration a
 * BE had ever read through - each ALTER CATALOG that rotated a credential, each CREATE/query/DROP
 * cycle - kept its filesystem, its SDK client and that client's executor threads until the BE was
 * restarted: a per-query leak turned into a per-configuration one. Now a scope is reference-counted
 * by the scanners holding it, and a scope with no holders is closed once it has been idle for
 * {@link #DEFAULT_IDLE_TTL_NANOS}. The idle window is what keeps the cache a cache: scanners are
 * per split, so counting to zero happens between every two queries, and closing on zero would
 * rebuild the S3 client - and its thread pools - for each of them.
 *
 * <p>Why eviction cannot strand a live scan. A scope is removed only while its count is zero, under
 * the same lock {@link #acquire} takes, so a scanner either holds the entry (count above zero, the
 * sweep skips it) or arrives after removal and gets a fresh UGI. {@code UserGroupInformation}
 * equality is Subject identity, so the filesystems the sweep closes are keyed on the old UGI and can
 * never be handed to a scan under the new one; the close itself runs outside the lock, because
 * {@code closeAllForUGI} holds the process-wide {@code FileSystem.CACHE} monitor for its whole
 * duration and one S3AFileSystem close can spend minutes draining its pools.
 *
 * <p>Kerberos has no scope. {@code createRemoteUser} would drop the ticket's credentials, so a
 * Kerberos scan keeps the authenticator's own UGI, which is cached per principal and shared by every
 * catalog on that principal - there is no per-configuration set to close, and those filesystems live
 * as long as the process, as they always have.
 *
 * <p>The process-wide instance ({@link #shared()}) sweeps from one daemon thread that starts on the
 * first acquire, so a BE that never reads hudi never has it. Tests build their own instance with a
 * clock they control and call {@link #sweep()} themselves.
 */
final class HudiFileSystemScopes {
    private static final Logger LOG = LoggerFactory.getLogger(HudiFileSystemScopes.class);

    /**
     * How long a configuration nobody reads through keeps its filesystems. Long enough that a
     * stream of queries against one catalog never rebuilds them; short enough that the clients of
     * a rotated credential are gone within minutes rather than at the next restart.
     */
    static final long DEFAULT_IDLE_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);

    private static final long SWEEP_PERIOD_SECONDS = 60;

    private static volatile HudiFileSystemScopes shared;

    /** A configuration's UGI and the number of scanners reading under it. Guarded by the registry. */
    private static final class Scope {
        private final UserGroupInformation ugi;
        private int owners;
        // Meaningful only while owners == 0: when the last holder let go.
        private long idleSinceNanos;

        private Scope(UserGroupInformation ugi) {
            this.ugi = ugi;
        }
    }

    /**
     * One scanner's hold on a scope: what {@link #acquire} hands out and what the scanner gives back
     * in its close. Releasing is idempotent per hold, so a close that runs twice - which the scanner
     * contract allows - cannot free a hold that belongs to another scanner on the same configuration.
     */
    static final class Hold {
        private final HudiFileSystemScopes registry;
        private final Scope scope;
        private boolean released;

        private Hold(HudiFileSystemScopes registry, Scope scope) {
            this.registry = registry;
            this.scope = scope;
        }

        /** The UGI to read under while this hold is live. */
        UserGroupInformation ugi() {
            return scope.ugi;
        }

        void release() {
            synchronized (registry.scopes) {
                if (released) {
                    return;
                }
                released = true;
                registry.release(scope);
            }
        }
    }

    private final Map<String, Scope> scopes = new HashMap<>();
    private final LongSupplier clock;
    private final long idleTtlNanos;

    HudiFileSystemScopes(LongSupplier clock, long idleTtlNanos) {
        this.clock = clock;
        this.idleTtlNanos = idleTtlNanos;
    }

    /** The registry every scanner in this process shares, sweeping on its own thread. */
    static HudiFileSystemScopes shared() {
        HudiFileSystemScopes local = shared;
        if (local == null) {
            synchronized (HudiFileSystemScopes.class) {
                local = shared;
                if (local == null) {
                    local = new HudiFileSystemScopes(System::nanoTime, DEFAULT_IDLE_TTL_NANOS);
                    local.startSweeper();
                    shared = local;
                }
            }
        }
        return local;
    }

    private void startSweeper() {
        ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "hudi-fs-scope-sweeper");
            thread.setDaemon(true);
            return thread;
        });
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                sweep();
            } catch (RuntimeException | LinkageError e) {
                // A failed sweep must not cancel the schedule: the next one retries.
                LOG.warn("sweeping idle hudi filesystem scopes failed", e);
            }
        }, SWEEP_PERIOD_SECONDS, SWEEP_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Takes a hold on the scope of {@code key}, creating it - as a {@code createRemoteUser} of
     * {@code userName} - when there is none. Two scanners with the same key share one UGI, which is
     * exactly when they may share a filesystem.
     */
    Hold acquire(String key, String userName) {
        synchronized (scopes) {
            Scope scope = scopes.get(key);
            if (scope == null) {
                scope = new Scope(UserGroupInformation.createRemoteUser(userName));
                scopes.put(key, scope);
            }
            scope.owners++;
            return new Hold(this, scope);
        }
    }

    // Called under the scopes monitor, from Hold.release.
    private void release(Scope scope) {
        scope.owners--;
        if (scope.owners == 0) {
            scope.idleSinceNanos = clock.getAsLong();
        }
    }

    /**
     * Closes every scope that has had no holder for the idle window, and returns how many. Removal
     * happens under the lock; the closing does not, see the class comment.
     */
    int sweep() {
        long now = clock.getAsLong();
        List<Scope> idle = new ArrayList<>();
        synchronized (scopes) {
            Iterator<Scope> it = scopes.values().iterator();
            while (it.hasNext()) {
                Scope scope = it.next();
                if (scope.owners == 0 && now - scope.idleSinceNanos >= idleTtlNanos) {
                    it.remove();
                    idle.add(scope);
                }
            }
        }
        for (Scope scope : idle) {
            try {
                FileSystem.closeAllForUGI(scope.ugi);
            } catch (Exception | LinkageError e) {
                LOG.warn("failed to close the filesystems of an idle hudi filesystem scope", e);
            }
        }
        return idle.size();
    }

    /** How many scanners hold the scope of {@code key}, or 0 when there is none. For tests. */
    int owners(String key) {
        synchronized (scopes) {
            Scope scope = scopes.get(key);
            return scope == null ? 0 : scope.owners;
        }
    }

    /** Whether {@code key} has a scope at all, held or idle. For tests. */
    boolean holds(String key) {
        synchronized (scopes) {
            return scopes.containsKey(key);
        }
    }
}
