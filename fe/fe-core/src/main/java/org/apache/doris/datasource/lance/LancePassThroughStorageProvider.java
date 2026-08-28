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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Every provider Lance routes somewhere other than S3 - Azure, GCS, OSS, Tencent COS, local files,
 * and anything Lance adds later.
 *
 * <p>Both halves are deliberately inert, so such a dataset is reachable only through what its
 * namespace vends. Those options are the namespace's to name: Lance's OSS provider reads
 * {@code access_key_id} and requires {@code endpoint}, object_store's Azure parser reads
 * {@code endpoint} and takes {@code token} as a bearer token. Rewriting any of that onto the S3
 * spellings would leave the dataset unreachable, which is what this class exists to prevent.
 *
 * <p>{@link #fromDorisProperties} is empty for want of a translation, not for want of input: Doris
 * does model these - {@code OSSProperties} and {@code COSProperties} carry an endpoint and
 * credentials like any other. Writing that translation means committing to a vocabulary per
 * provider with no way to exercise it here, which is how the rewriting bug above got in, so it
 * waits for a backend that can be tested against.
 */
final class LancePassThroughStorageProvider implements LanceStorageProvider {

    static final LancePassThroughStorageProvider INSTANCE = new LancePassThroughStorageProvider();

    private LancePassThroughStorageProvider() {
    }

    @Override
    public Map<String, String> fromDorisProperties(List<StorageProperties> storageProperties) {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> normalizeVended(Map<String, String> vendedOptions) {
        return vendedOptions == null ? new HashMap<>() : new HashMap<>(vendedOptions);
    }
}
