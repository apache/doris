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

package org.apache.doris.datasource.lance.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Access parameters resolved for one read; credentials are not versioned dataset metadata.
 *
 * <p>A storage-versioned dataset is opened by URI: Lance resolves versions from the
 * {@code _versions/} directory of the dataset itself. A namespace-managed dataset
 * ({@code DescribeTableResponse.managed_versioning = true}) is opened through the namespace
 * client instead, so the SDK asks the namespace for the manifest of the latest or requested
 * version and copies a still-staged manifest to its canonical path. A reader which only knows
 * the dataset URI, such as the BE lance-c reader, can then open the same version afterwards.
 *
 * <p>The dataset URI and storage options are kept in both modes; they are what the BE receives.
 * In namespace mode the SDK starts from {@link #getSdkStorageOptions()} instead and adds the
 * options the namespace vends when the SDK describes the table itself.
 */
public final class LanceTableAccess {
    private final String datasetUri;
    private final Map<String, String> storageOptions;
    private final Map<String, String> sdkStorageOptions;
    private final List<String> namespaceTableId;
    private final String branch;

    /** A dataset whose versions live in its own {@code _versions/} directory. */
    public LanceTableAccess(String datasetUri, Map<String, String> storageOptions) {
        this(datasetUri, storageOptions, storageOptions, null, null);
    }

    private LanceTableAccess(String datasetUri, Map<String, String> storageOptions,
            Map<String, String> sdkStorageOptions, List<String> namespaceTableId, String branch) {
        this.datasetUri = Objects.requireNonNull(datasetUri, "datasetUri");
        this.storageOptions = Collections.unmodifiableMap(new HashMap<>(storageOptions));
        this.sdkStorageOptions = Collections.unmodifiableMap(new HashMap<>(sdkStorageOptions));
        this.namespaceTableId = namespaceTableId == null
                ? null : Collections.unmodifiableList(new ArrayList<>(namespaceTableId));
        this.branch = branch;
    }

    /**
     * The same table on one of its branches. A branch is a separate manifest chain with its own
     * root directory, which the SDK reports as the checked-out dataset's URI; a reader that opens
     * by URI, such as the BE, addresses the branch by that root. The storage options and namespace
     * identity are unchanged.
     */
    public LanceTableAccess onBranch(String branchName, String branchUri) {
        return new LanceTableAccess(Objects.requireNonNull(branchUri, "branchUri"), storageOptions,
                sdkStorageOptions, namespaceTableId, Objects.requireNonNull(branchName, "branchName"));
    }

    /** The branch this access addresses, if not the main chain. */
    public Optional<String> getBranch() {
        return Optional.ofNullable(branch);
    }

    /**
     * A dataset whose versions are recorded by the namespace that owns {@code namespaceTableId}.
     * {@code sdkStorageOptions} are the options the SDK opens it with before adding what the
     * namespace vends to the SDK itself.
     */
    public static LanceTableAccess managedByNamespace(String datasetUri, Map<String, String> storageOptions,
            Map<String, String> sdkStorageOptions, List<String> namespaceTableId) {
        return new LanceTableAccess(datasetUri, storageOptions, sdkStorageOptions,
                Objects.requireNonNull(namespaceTableId, "namespaceTableId"), null);
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public Map<String, String> getStorageOptions() {
        return storageOptions;
    }

    /** The options the FE opens the dataset with through the SDK; the storage options unless managed. */
    public Map<String, String> getSdkStorageOptions() {
        return sdkStorageOptions;
    }

    /** Whether versions are resolved through the namespace rather than the dataset directory. */
    public boolean isManagedVersioning() {
        return namespaceTableId != null;
    }

    /** The namespace table identifier the SDK opens a managed dataset with; null otherwise. */
    public List<String> getNamespaceTableId() {
        return namespaceTableId;
    }
}
