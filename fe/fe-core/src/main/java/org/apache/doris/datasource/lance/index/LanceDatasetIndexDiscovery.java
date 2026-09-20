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

package org.apache.doris.datasource.lance.index;

import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Shared index discovery on an already-open snapshot, independent of SHOW INDEX formatting and execution. */
public final class LanceDatasetIndexDiscovery {
    private static final Logger LOG = LogManager.getLogger(LanceDatasetIndexDiscovery.class);
    static final int MAX_LOGICAL_INDEXES = 256;
    static final int MAX_PHYSICAL_INDEX_ENTRIES = 16 * 1024;
    static final int MAX_EXTERNAL_STRING_BYTES = 1024;
    static final Set<String> SYSTEM_INDEX_NAMES = ImmutableSet.of("__lance_frag_reuse", "__lance_mem_wal");

    private LanceDatasetIndexDiscovery() {
    }

    /**
     * Describes only user-created indexes. The Lance JNI bulk describe path also tries to
     * materialize details for internal indexes, whose details are not supported by the SDK.
     * Legacy indexes that cannot be described because all their segments lack details are
     * omitted. This disables their FE segment planning, not the scanner's automatic index use.
     */
    public static List<IndexDescription> describeUserIndexes(Dataset dataset) {
        List<String> listedNames = dataset.listIndexes();
        if (listedNames == null) {
            throw new IllegalArgumentException(
                    "Lance index names must not be null: Dataset.listIndexes() returned null");
        }
        if (listedNames.size() > MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "Lance physical index entry count exceeds limit "
                            + MAX_PHYSICAL_INDEX_ENTRIES + " (actual=" + listedNames.size() + ")");
        }

        Set<String> userIndexNames = new LinkedHashSet<>();
        for (String listedName : listedNames) {
            if (SYSTEM_INDEX_NAMES.contains(listedName)) {
                continue;
            }
            String name = requireExternalString(listedName, "Lance logical index name");
            // nativeListIndexes returns physical entries, so one logical name can repeat.
            userIndexNames.add(name);
            if (userIndexNames.size() > MAX_LOGICAL_INDEXES) {
                throw new IllegalArgumentException(
                        "Lance logical index count exceeds limit " + MAX_LOGICAL_INDEXES
                                + " (observed=" + userIndexNames.size()
                                + ", listed_entries=" + listedNames.size() + ", last_index='" + name
                                + "'); the limit counts distinct user index names, not physical segments");
            }
        }

        List<IndexDescription> descriptions = new ArrayList<>(userIndexNames.size());
        Set<String> indexesWithoutDetails = null;
        for (String name : userIndexNames) {
            IndexCriteria criteria = new IndexCriteria.Builder().hasName(name).build();
            List<IndexDescription> matching = dataset.describeIndices(criteria);
            if (matching == null) {
                throw new IllegalArgumentException(
                        "Lance index descriptions must not be null: describeIndices returned null for index '"
                                + name + "' at dataset version " + dataset.version());
            }
            // Lance attempts to infer legacy vector details before describing indexes. Only
            // check raw metadata after that attempt: skipping them upfront would also discard
            // vector segments whose details Lance can successfully recover from index files.
            if (matching.isEmpty()) {
                if (indexesWithoutDetails == null) {
                    indexesWithoutDetails = loadIndexesWithoutDetails(dataset);
                }
                if (indexesWithoutDetails.contains(name)) {
                    LOG.warn("Skipping FE metadata and segment planning for legacy Lance index '{}' "
                            + "at dataset version {}: all physical segments lack index details; "
                            + "scanner index selection remains unchanged", name, dataset.version());
                    continue;
                }
            }
            // Keep all other missing or ambiguous descriptions strict, including mixed
            // legacy/current segments. An empty description alone is not evidence of age.
            if (matching.size() != 1) {
                throw new IllegalArgumentException(
                        "Lance index criteria must return exactly one description (actual="
                                + matching.size() + ") for index '" + name
                                + "' at dataset version " + dataset.version()
                                + "; expected one logical index description, which may contain multiple segments. "
                                + (matching.isEmpty()
                                        ? "The listed index could not be described and was not confirmed to have "
                                                + "missing details on every segment; inspect the Lance native warnings "
                                                + "in fe.out for the underlying reason."
                                        : "Multiple descriptions matched the same exact index name; "
                                                + "check the index metadata for this snapshot."));
            }
            IndexDescription description = matching.get(0);
            if (description == null) {
                throw new IllegalArgumentException(
                        "Lance logical index description must not be null for index '" + name
                                + "' at dataset version " + dataset.version());
            }
            String describedName = requireExternalString(
                    description.getName(), "Lance logical index description name requested for '" + name + "'");
            if (!name.equals(describedName)) {
                throw new IllegalArgumentException(
                        "Lance index description name does not match requested name (expected='"
                                + name + "', actual='" + describedName + "', dataset_version="
                                + dataset.version() + ")");
            }
            descriptions.add(description);
        }
        return descriptions;
    }

    /** Names with at least one physical entry and no segment carrying index details. */
    private static Set<String> loadIndexesWithoutDetails(Dataset dataset) {
        List<Index> indexes = dataset.getIndexes();
        if (indexes == null) {
            throw new IllegalArgumentException("Lance physical index entries must not be null: "
                    + "Dataset.getIndexes() returned null while checking legacy indexes without details");
        }
        if (indexes.size() > MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "Lance physical index entry count exceeds limit "
                            + MAX_PHYSICAL_INDEX_ENTRIES + " (actual=" + indexes.size() + ")");
        }
        Set<String> withoutDetails = new HashSet<>();
        Set<String> withDetails = new HashSet<>();
        for (int position = 0; position < indexes.size(); ++position) {
            Index index = indexes.get(position);
            if (index == null) {
                throw new IllegalArgumentException("Lance physical index entry must not be null "
                        + "while checking legacy indexes (entry_position=" + position
                        + ", total_entries=" + indexes.size() + ", positions are zero-based)");
            }
            String name = requireExternalString(index.name(),
                    "Lance physical index entry name at position " + position);
            if (index.indexDetails().isPresent()) {
                withDetails.add(name);
            } else {
                withoutDetails.add(name);
            }
        }
        withoutDetails.removeAll(withDetails);
        return withoutDetails;
    }

    static String requireExternalString(String value, String valueType) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(valueType + " must not be null or empty (actual="
                    + (value == null ? "null" : "empty string") + ")");
        }
        if (utf8Length(value) > MAX_EXTERNAL_STRING_BYTES) {
            throw new IllegalArgumentException(valueType + " exceeds limit "
                    + MAX_EXTERNAL_STRING_BYTES + " UTF-8 bytes (actual="
                    + utf8Length(value) + ")");
        }
        return value;
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

}
