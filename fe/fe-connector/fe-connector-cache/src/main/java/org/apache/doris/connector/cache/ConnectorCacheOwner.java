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

package org.apache.doris.connector.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/** Cache ownership of one connector context, including siblings created through that same context. */
public final class ConnectorCacheOwner {
    private static final ThreadLocal<ConnectorCacheOwner> CURRENT = new ThreadLocal<>();
    private final Set<CatalogMetaCache> caches = new HashSet<>();
    private volatile boolean retired;

    public <T> T create(Supplier<T> factory) {
        checkActive();
        ConnectorCacheOwner previous = CURRENT.get();
        CURRENT.set(this);
        try {
            return factory.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    static ConnectorCacheOwner current() {
        return CURRENT.get();
    }

    synchronized void register(CatalogMetaCache cache) {
        // Retirement can win while a provider constructs its connector outside FE metadata locks.
        checkActive();
        caches.add(cache);
    }

    synchronized void unregister(CatalogMetaCache cache) {
        caches.remove(cache);
    }

    public void retire() {
        List<CatalogMetaCache> snapshot;
        synchronized (this) {
            if (retired) {
                return;
            }
            retired = true;
            snapshot = new ArrayList<>(caches);
            caches.clear();
        }
        Throwable failure = null;
        for (CatalogMetaCache cache : snapshot) {
            try {
                cache.close();
            } catch (RuntimeException | Error e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    private void checkActive() {
        if (retired) {
            throw new IllegalStateException("Connector cache owner has been retired");
        }
    }
}
