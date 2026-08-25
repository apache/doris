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

package org.apache.doris.datasource.iceberg;

import java.net.URI;
import java.util.Locale;
import java.util.concurrent.locks.StampedLock;

/** Serializes destructive maintenance with Doris-coordinated commits for one Iceberg table. */
public final class IcebergCommitCoordinator {
    private static final int STRIPE_COUNT = 1024;
    private static final StampedLock[] TABLE_LOCKS = new StampedLock[STRIPE_COUNT];

    static {
        for (int i = 0; i < TABLE_LOCKS.length; i++) {
            TABLE_LOCKS[i] = new StampedLock();
        }
    }

    private IcebergCommitCoordinator() {
    }

    public static Guard beginCommit(String tableLocation) {
        StampedLock lock = lockFor(tableLocation);
        return new Guard(lock, lock.readLock(), false);
    }

    public static Guard beginMaintenance(String tableLocation) {
        StampedLock lock = lockFor(tableLocation);
        return new Guard(lock, lock.writeLock(), true);
    }

    static StampedLock lockFor(String tableLocation) {
        URI uri = URI.create(tableLocation).normalize();
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("s3a") || scheme.equals("s3n")) {
            scheme = "s3";
        }
        String authority = uri.getAuthority() == null
                ? "" : uri.getAuthority().toLowerCase(Locale.ROOT);
        String path = uri.getPath() == null ? "" : uri.getPath();
        while (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        String normalizedLocation = scheme + "://" + authority + path;
        return TABLE_LOCKS[Math.floorMod(normalizedLocation.hashCode(), STRIPE_COUNT)];
    }

    public static final class Guard implements AutoCloseable {
        private final StampedLock lock;
        private final long stamp;
        private final boolean write;
        private boolean closed;

        private Guard(StampedLock lock, long stamp, boolean write) {
            this.lock = lock;
            this.stamp = stamp;
            this.write = write;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                // Transaction begin and completion may run on different FE worker threads, so the
                // fence must be released by its stamp instead of by thread ownership.
                if (write) {
                    lock.unlockWrite(stamp);
                } else {
                    lock.unlockRead(stamp);
                }
            }
        }
    }
}
