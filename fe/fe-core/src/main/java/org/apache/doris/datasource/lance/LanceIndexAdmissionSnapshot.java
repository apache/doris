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

import org.lance.schema.LanceField;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * One pinned latest-snapshot view of everything index admission needs from a Lance dataset:
 * the dataset version, the user logical indexes, the physical index entries (including the
 * physical index type name), and the top-level schema fields the schema contract is built
 * from. Everything is read inside a single {@code Dataset.open} call by
 * {@link LanceIndexMetadataLoader#loadAdmissionSnapshot}, and every list is materialized into
 * plain Java values before the Dataset closes — the snapshot holds no native handle, Arrow
 * allocator, or Dataset reference ({@link LanceField} is a pure POJO).
 *
 * <p>All bounds mirror the limits already enforced by {@link LanceIndexMetadataLoader}.
 * Construction validates eagerly so a malformed or oversized provider result fails closed at
 * the boundary. In particular, two physical entries sharing one name — a stale post-REPLACE
 * entry coexisting with its replacement until VACUUM — make the whole snapshot ambiguous, so
 * the constructor rejects them instead of silently keeping whichever entry the provider
 * returned first (design section 3.4).
 */
public final class LanceIndexAdmissionSnapshot {
    private final long datasetVersion;
    private final String datasetUri;
    private final List<LanceLogicalIndex> logicalIndexes;
    private final List<PhysicalIndexInfo> physicalIndexes;
    private final List<LanceField> topLevelFields;

    public LanceIndexAdmissionSnapshot(long datasetVersion, String datasetUri,
            List<LanceLogicalIndex> logicalIndexes, List<PhysicalIndexInfo> physicalIndexes,
            List<LanceField> topLevelFields) {
        if (datasetVersion <= 0) {
            throw new IllegalArgumentException("Lance dataset version must be positive");
        }
        if (datasetUri == null || datasetUri.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset uri must not be null or empty");
        }
        this.datasetVersion = datasetVersion;
        // Never echoed into exception messages: the uri can carry credential userinfo, and the
        // catalog boundary sanitizes it out of provider failures.
        this.datasetUri = datasetUri;
        this.logicalIndexes = copyLogicalIndexes(logicalIndexes);
        this.physicalIndexes = copyPhysicalIndexes(physicalIndexes);
        this.topLevelFields = copyTopLevelFields(topLevelFields);
    }

    private static List<LanceLogicalIndex> copyLogicalIndexes(List<LanceLogicalIndex> source) {
        if (source == null) {
            throw new IllegalArgumentException("Lance logical indexes must not be null");
        }
        if (source.size() > LanceIndexMetadataLoader.MAX_LOGICAL_INDEXES) {
            throw new IllegalArgumentException("Lance logical index count exceeds limit "
                    + LanceIndexMetadataLoader.MAX_LOGICAL_INDEXES);
        }
        List<LanceLogicalIndex> copy = new ArrayList<>(source.size());
        for (LanceLogicalIndex index : source) {
            if (index == null) {
                throw new IllegalArgumentException("Lance logical index must not be null");
            }
            copy.add(index);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<PhysicalIndexInfo> copyPhysicalIndexes(List<PhysicalIndexInfo> source) {
        if (source == null) {
            throw new IllegalArgumentException("Lance physical index entries must not be null");
        }
        if (source.size() > LanceIndexMetadataLoader.MAX_PHYSICAL_INDEX_ENTRIES) {
            throw new IllegalArgumentException("Lance physical index entry count exceeds limit "
                    + LanceIndexMetadataLoader.MAX_PHYSICAL_INDEX_ENTRIES);
        }
        List<PhysicalIndexInfo> copy = new ArrayList<>(source.size());
        Set<String> names = new HashSet<>();
        for (PhysicalIndexInfo entry : source) {
            if (entry == null) {
                throw new IllegalArgumentException("Lance physical index entry must not be null");
            }
            // The entry name is bounded by PhysicalIndexInfo's own constructor, so echoing it
            // here keeps this failure bounded.
            if (!names.add(entry.getName())) {
                throw new IllegalArgumentException("Duplicate Lance physical index name '"
                        + entry.getName() + "' in one dataset snapshot");
            }
            copy.add(entry);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<LanceField> copyTopLevelFields(List<LanceField> source) {
        if (source == null) {
            throw new IllegalArgumentException("Lance schema fields must not be null");
        }
        if (source.size() > LanceIndexMetadataLoader.MAX_SCHEMA_FIELDS) {
            throw new IllegalArgumentException("Lance schema field count exceeds limit "
                    + LanceIndexMetadataLoader.MAX_SCHEMA_FIELDS);
        }
        List<LanceField> copy = new ArrayList<>(source.size());
        for (LanceField field : source) {
            if (field == null) {
                throw new IllegalArgumentException("Lance schema field must not be null");
            }
            copy.add(field);
        }
        return Collections.unmodifiableList(copy);
    }

    public long getDatasetVersion() {
        return datasetVersion;
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public List<LanceLogicalIndex> getLogicalIndexes() {
        return logicalIndexes;
    }

    public List<PhysicalIndexInfo> getPhysicalIndexes() {
        return physicalIndexes;
    }

    public List<LanceField> getTopLevelFields() {
        return topLevelFields;
    }

    /**
     * One physical index entry of the snapshot. Unlike the logical view, a physical entry
     * carries its manifest umbrella/concrete {@code IndexType} name (for example VECTOR or
     * IVF_PQ), which the section 3.4 family comparison needs.
     */
    public static final class PhysicalIndexInfo {
        private final String name;
        private final String uuid;
        private final long indexDatasetVersion;
        private final String indexTypeName;

        public PhysicalIndexInfo(String name, String uuid, long indexDatasetVersion,
                String indexTypeName) {
            this.name = requireBoundedString(name, "Lance physical index name");
            this.uuid = requireBoundedString(uuid, "Lance physical index uuid");
            if (indexDatasetVersion <= 0) {
                throw new IllegalArgumentException(
                        "Lance physical index dataset version must be positive");
            }
            this.indexDatasetVersion = indexDatasetVersion;
            this.indexTypeName = requireBoundedString(indexTypeName, "Lance physical index type");
        }

        private static String requireBoundedString(String value, String valueType) {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(valueType + " must not be null or empty");
            }
            if (value.getBytes(StandardCharsets.UTF_8).length
                    > LanceIndexMetadataLoader.MAX_EXTERNAL_STRING_BYTES) {
                throw new IllegalArgumentException(valueType + " exceeds limit "
                        + LanceIndexMetadataLoader.MAX_EXTERNAL_STRING_BYTES + " UTF-8 bytes");
            }
            return value;
        }

        public String getName() {
            return name;
        }

        public String getUuid() {
            return uuid;
        }

        public long getIndexDatasetVersion() {
            return indexDatasetVersion;
        }

        public String getIndexTypeName() {
            return indexTypeName;
        }
    }
}
