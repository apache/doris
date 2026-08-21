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
     * Builds the options for one dataset, from Doris's storage properties and whatever a namespace
     * vended for it. Pass {@code null} for {@code vendedOptions} where nothing is vended - the
     * namespace client's own storage, or the {@code s3()} table-valued function.
     *
     * <p>Both halves are put in the vocabulary of the provider Lance will route {@code datasetUri}
     * to, so a vended option lands on the same key as the catalog's and replaces it outright.
     * Without that they would reach Lance as two entries for one config key, and object_store
     * would keep whichever its HashMap yielded last - independently in the FE and in the BE.
     */
    public static Map<String, String> forDataset(String datasetUri,
            List<StorageProperties> storageProperties, Map<String, String> vendedOptions) {
        LanceStorageProvider provider = LanceStorageProvider.forDataset(datasetUri);
        Map<String, String> result = new HashMap<>(provider.fromDorisProperties(storageProperties));
        if (vendedOptions == null) {
            return result;
        }
        vendedOptions.forEach(LanceStorageOptions::validateVendedOption);
        result.putAll(provider.normalizeVended(vendedOptions));
        return result;
    }

    /**
     * Rejects what the FE cannot hand to the BE unchanged.
     *
     * <p>These options reach lance-c as C strings, so a NUL would truncate one there while the FE
     * kept reading the whole thing, leaving the two halves opening the dataset with different
     * configuration. That has to fail loudly: dropping the option instead just moves the
     * divergence, since an FE that drops it and a BE that does not disagree in the same way.
     */
    private static void validateVendedOption(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Lance namespace vended a storage option with a null key or value");
        }
        if (key.indexOf('\0') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "Lance namespace vended the storage option '" + key.replace('\0', '?')
                            + "' with a NUL in its key or value, which cannot reach the backend");
        }
    }
}
