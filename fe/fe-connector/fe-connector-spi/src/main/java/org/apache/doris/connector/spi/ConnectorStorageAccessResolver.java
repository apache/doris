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

package org.apache.doris.connector.spi;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Request-local storage resolver with an immutable, case-insensitive snapshot of its bound providers.
 *
 * <p>Provider lookup does not resolve a URI or access credentials. A connector can select the relevant
 * storage contract without requiring its metadata root to be a data-file location. Resolution remains
 * engine-owned, and this object must not be cached across requests or credential generations.
 */
public final class ConnectorStorageAccessResolver implements Function<String, ConnectorStorageAccess> {
    private final Set<String> providerNames;
    private final Function<String, ConnectorStorageAccess> resolver;

    public ConnectorStorageAccessResolver(Set<String> providerNames,
            Function<String, ConnectorStorageAccess> resolver) {
        Set<String> snapshot = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        snapshot.addAll(Objects.requireNonNull(providerNames, "providerNames is required"));
        this.providerNames = Collections.unmodifiableSet(snapshot);
        this.resolver = Objects.requireNonNull(resolver, "resolver is required");
    }

    /** Returns whether this request has the named provider, without resolving any location. */
    public boolean hasProvider(String providerName) {
        return providerNames.contains(Objects.requireNonNull(providerName, "providerName is required"));
    }

    /** Resolves the supplied location through the captured engine resolver. */
    @Override
    public ConnectorStorageAccess apply(String rawUri) {
        return resolver.apply(rawUri);
    }
}
