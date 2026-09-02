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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.property.storage.StorageProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the Lance object-store options for one dataset.
 *
 * <p>Both the FE, which opens the dataset through the Lance Java SDK, and the BE, which opens it
 * through lance-c, consume the map produced here, so neither can reach a dataset by a
 * configuration the other never saw.
 *
 * <p>The option vocabulary belongs to {@link LanceStorageProvider}, chosen from the dataset's URL
 * the same way Lance chooses one. This class only decides what happens when the catalog and the
 * namespace both describe a dataset: the namespace wins, because it just described the table.
 */
public final class LanceStorageOptions {

    private LanceStorageOptions() {
    }

    /**
     * Doris's own storage configuration, in the vocabulary of the provider Lance routes
     * {@code uri} to.
     *
     * <p>Used wherever no namespace is involved: the storage a namespace client reads itself, and
     * the {@code s3()} table-valued function.
     */
    public static Map<String, String> fromDorisStorageProperties(
            String uri, List<StorageProperties> storageProperties) {
        return buildStorageOptions(uri, storageProperties, null);
    }

    /**
     * The same, plus whatever a namespace vended for one table, which wins on any option both
     * sides name - it just described the table.
     *
     * <p>Putting both halves in one provider's vocabulary is what makes that possible: otherwise
     * two spellings of one option reach Lance as separate entries, and object_store keeps
     * whichever its HashMap yields last - independently in the FE and in the BE.
     *
     * <p>{@code vendedOptions} may be null or empty; a namespace that describes a table without
     * vending storage options is ordinary, and this then degenerates to
     * {@link #fromDorisStorageProperties}.
     */
    public static Map<String, String> fromDorisAndVendedStorageOptions(String datasetUri,
            List<StorageProperties> storageProperties, Map<String, String> vendedOptions) {
        return buildStorageOptions(datasetUri, storageProperties, vendedOptions);
    }

    private static Map<String, String> buildStorageOptions(String datasetUri,
            List<StorageProperties> storageProperties, Map<String, String> vendedOptions) {
        LanceStorageProvider provider = LanceStorageProvider.forDataset(datasetUri);
        Map<String, String> result = new HashMap<>(
                provider.normalizeDorisStorageOptions(storageProperties));
        result.forEach((key, value) -> rejectUntransportable(key, value,
                "Doris storage configuration"));
        Map<String, String> normalizedVended = new HashMap<>();
        if (vendedOptions != null && !vendedOptions.isEmpty()) {
            vendedOptions.forEach(LanceStorageOptions::validateVendedOption);
            // Safe to validate before normalizing: normalization only renames a key to a provider
            // constant or passes it through, so it cannot introduce a NUL missed above.
            normalizedVended = provider.normalizeVendedStorageOptions(vendedOptions);
        }
        result.putAll(normalizedVended);
        // The overlay above is key by key, which can leave OSS's authentication options - a key
        // pair, its token, and the signing choice - disagreeing with one another.
        // Only OSS couples its options this way, so this is a direct call rather than a hook on
        // LanceStorageProvider that one implementation would honour and the others ignore.
        if (provider instanceof LanceOssStorageProvider) {
            LanceOssStorageProvider.reconcileAuth(result, normalizedVended);
        }
        provider.inferStorageOptions(result).forEach(result::putIfAbsent);
        result.forEach((key, value) -> rejectUntransportable(key, value,
                "Lance storage configuration"));
        return result;
    }

    /**
     * Rejects an option a namespace had no business sending.
     *
     * <p>Null is not expressible at all, and a NUL cannot survive the boundary - see
     * {@link #rejectUntransportable}.
     */
    private static void validateVendedOption(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Lance namespace vended a storage option with a null key or value");
        }
        rejectUntransportable(key, value, "Lance namespace");
    }

    /**
     * Rejects what cannot cross into Lance unchanged, whichever side it came from.
     *
     * <p>These options are handed to lance-c and to the Lance Java SDK as C strings, so a NUL
     * truncates one there while the FE goes on using the whole thing, and the two halves open the
     * dataset with different configuration. That has to fail loudly: dropping the option instead
     * only moves the divergence, since a component that drops it and one that does not disagree in
     * exactly the same way. The BE repeats this check as its own last line of defence.
     */
    private static void rejectUntransportable(String key, String value, String source) {
        if (key.indexOf('\0') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(source + " supplied the storage option '"
                    + key.replace('\0', '?') + "' with a NUL in its key or value, which cannot "
                    + "reach Lance intact");
        }
    }
}
