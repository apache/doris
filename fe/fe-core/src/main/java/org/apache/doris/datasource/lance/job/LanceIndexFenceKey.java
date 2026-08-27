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

package org.apache.doris.datasource.lance.job;

import java.util.Objects;

/**
 * The durable same-name target/fence key:
 * <pre>
 * ( persisted catalog identity, provider = DIRECTORY,
 *   normalized stable dataset locator, persisted normalized logical-index-name bytes )
 * </pre>
 * The display name is not part of the key; it is persisted on the job itself.
 * This class is a derived in-memory index key and is not persisted directly.
 */
public final class LanceIndexFenceKey {
    /** Provider of every job in this delivery slice, mapped from a filesystem (Directory) Lance catalog. */
    public static final String PROVIDER_DIRECTORY = "DIRECTORY";

    private final long catalogId;
    private final String provider;
    private final String normalizedLocator;
    private final String normalizedIndexName;

    public LanceIndexFenceKey(long catalogId, String provider, String normalizedLocator, String normalizedIndexName) {
        this.catalogId = catalogId;
        this.provider = Objects.requireNonNull(provider, "provider");
        this.normalizedLocator = Objects.requireNonNull(normalizedLocator, "normalizedLocator");
        this.normalizedIndexName = Objects.requireNonNull(normalizedIndexName, "normalizedIndexName");
    }

    public long getCatalogId() {
        return catalogId;
    }

    public String getProvider() {
        return provider;
    }

    public String getNormalizedLocator() {
        return normalizedLocator;
    }

    public String getNormalizedIndexName() {
        return normalizedIndexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceIndexFenceKey)) {
            return false;
        }
        LanceIndexFenceKey that = (LanceIndexFenceKey) o;
        return catalogId == that.catalogId
                && provider.equals(that.provider)
                && normalizedLocator.equals(that.normalizedLocator)
                && normalizedIndexName.equals(that.normalizedIndexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, provider, normalizedLocator, normalizedIndexName);
    }

    /**
     * Deliberately omits the locator: fence-conflict messages may surface to
     * users without target privileges and must not disclose it.
     */
    @Override
    public String toString() {
        return "LanceIndexFenceKey{catalogId=" + catalogId + ", provider=" + provider
                + ", normalizedIndexName=" + normalizedIndexName + '}';
    }
}
