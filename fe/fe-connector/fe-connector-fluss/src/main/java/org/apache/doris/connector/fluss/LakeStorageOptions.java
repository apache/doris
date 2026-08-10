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

package org.apache.doris.connector.fluss;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Which of a lake configuration's settings describe the STORAGE the lake sits on, and what Doris calls
 * them.
 *
 * <p>Storage is the one part of a lake configuration that does not belong to the paimon sibling connector.
 * Doris configures storage once per catalog and hands the same answer to the FE (which reads manifests
 * while planning) and to the BE (which reads the data files); a storage setting carried to the sibling
 * instead reaches at most the FE, and only for the keys that happen to be spelled the way Hadoop spells
 * them. That is worse than not carrying it at all: those keys are applied AFTER the catalog's own storage
 * configuration and win over it, so a stale value in the fluss cluster silently sends the FE to one
 * endpoint while the BE reads from another.
 *
 * <p>So these keys are taken out of the sibling's configuration and handed to the engine's storage layer
 * instead, under the names it knows ({@code Connector.deriveStorageProperties}). A setting NOT named here
 * is not storage as far as this connector is concerned and travels to the sibling unchanged.
 *
 * <p><b>A table, not a rule.</b> The spellings differ per cloud and follow no pattern — paimon writes
 * {@code s3.access-key} but {@code fs.oss.accessKeyId} and {@code fs.obs.access.key} — so a rule like
 * "replace every dash with an underscore" would leave two of the three wrong and corrupt unrelated keys
 * ({@code paimon.catalog.type}) on the way. The left column is what paimon's own filesystem loaders
 * declare as their required options; the right column is the canonical name of the matching
 * {@code *FileSystemProperties} field, written out because a connector plugin cannot import fe-filesystem.
 */
final class LakeStorageOptions {

    /** Paimon's spelling of a storage setting, mapped to the name Doris's storage layer binds it under. */
    private static final Map<String, String> DORIS_NAMES = dorisNames();

    private LakeStorageOptions() {
    }

    private static Map<String, String> dorisNames() {
        Map<String, String> names = new LinkedHashMap<>();
        // S3 (org.apache.paimon.s3.S3Loader; every s3.* key is re-keyed to fs.s3a.* by paimon's S3FileIO).
        // Doris: S3FileSystemProperties.ACCESS_KEY / SECRET_KEY / ENDPOINT / REGION.
        names.put("s3.access-key", "s3.access_key");
        names.put("s3.access.key", "s3.access_key");
        names.put("s3.secret-key", "s3.secret_key");
        names.put("s3.secret.key", "s3.secret_key");
        names.put("s3.endpoint", "s3.endpoint");
        names.put("s3.region", "s3.region");
        // OSS (org.apache.paimon.oss.OSSLoader). Doris: OssFileSystemProperties.
        names.put("fs.oss.accessKeyId", "oss.access_key");
        names.put("fs.oss.accessKeySecret", "oss.secret_key");
        names.put("fs.oss.endpoint", "oss.endpoint");
        // OBS (org.apache.paimon.obs.OBSLoader). Doris: ObsFileSystemProperties.
        names.put("fs.obs.access.key", "obs.access_key");
        names.put("fs.obs.access-key", "obs.access_key");
        names.put("fs.obs.secret.key", "obs.secret_key");
        names.put("fs.obs.secret-key", "obs.secret_key");
        names.put("fs.obs.endpoint", "obs.endpoint");
        return Collections.unmodifiableMap(names);
    }

    /**
     * Whether {@code lakeOption} (a lake setting under paimon's own name) configures storage, and so must
     * be given to the engine's storage layer rather than to the paimon sibling.
     */
    static boolean isStorageOption(String lakeOption) {
        return DORIS_NAMES.containsKey(lakeOption);
    }

    /**
     * The storage settings among {@code lakeOptions}, under the names Doris binds them by. Options that
     * are not storage are left out entirely — this is what the catalog's storage map is fed with, and a
     * paimon catalog option in there would be bound by nothing and reported by nothing.
     *
     * <p>Sorted, and a new map every time: it is folded into the catalog's storage properties, which are
     * read on every scan.
     */
    static Map<String, String> toStorageProperties(Map<String, String> lakeOptions) {
        Map<String, String> storage = new TreeMap<>();
        for (Map.Entry<String, String> entry : lakeOptions.entrySet()) {
            String dorisName = DORIS_NAMES.get(entry.getKey());
            if (dorisName != null) {
                storage.put(dorisName, entry.getValue());
            }
        }
        return storage;
    }
}
