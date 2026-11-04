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

import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.datasource.lance.index.LanceDatasetIndexDiscovery;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics.Stage;
import org.apache.doris.datasource.lance.storage.LanceStorageOptions;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;

/** Loads one fixed Lance dataset snapshot through the Lance Java SDK. */
public final class LanceMetadataLoader {
    private static final Logger LOG = LogManager.getLogger(LanceMetadataLoader.class);
    public static final long READ_ALLOCATOR_LIMIT = 256L * 1024 * 1024;

    private LanceMetadataLoader() {
    }

    /** Loading cost only; callers separately decide whether missing field IDs permit fallback. */
    public enum MetadataScope {
        /** Version, Arrow schema and fragments; used when FE does not select index segments. */
        BASIC,
        /** BASIC plus field IDs and discovered index segments, including legacy handling. */
        WITH_INDEXES
    }

    /** Standalone S3 TVFs own their allocator and do not participate in catalog Session sharing. */
    public static LanceTableMetadata loadLatestForTvf(
            String datasetUri, List<StorageProperties> storageProperties) throws Exception {
        LanceTableAccess access = new LanceTableAccess(datasetUri,
                LanceStorageOptions.fromDorisStorageProperties(datasetUri, storageProperties));
        try (BufferAllocator allocator = new RootAllocator(READ_ALLOCATOR_LIMIT);
                Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                        .readOptions(LanceReadOptions.forIndependentRead(
                                access.getStorageOptions(), OptionalLong.empty())).build()) {
            return read(dataset, access, MetadataScope.BASIC);
        }
    }

    /** Reads planning metadata from the caller's fixed snapshot; never opens or closes a Dataset. */
    public static LanceTableMetadata read(Dataset dataset, LanceTableAccess access,
            MetadataScope mode) {
        return read(dataset, access, mode, LanceMetadataMetrics.disabled());
    }

    public static LanceTableMetadata read(Dataset dataset, LanceTableAccess access,
            MetadataScope mode, LanceMetadataMetrics metrics) {
        long resolvedVersion = dataset.version();
        List<LanceFragmentInfo> fragments = metrics.measure(Stage.FRAGMENTS, () -> readFragments(dataset));
        if (mode == MetadataScope.BASIC) {
            return LanceTableMetadata.createBasicSnapshot(access, resolvedVersion,
                    metrics.measure(Stage.SCHEMA, dataset::getSchema), fragments);
        }
        Map<String, Integer> lanceFieldIds = metrics.measure(Stage.FIELD_IDS, () -> loadTopLevelFieldIds(dataset));
        // Keep index validation even when the known schema conversion error disables field-ID mapping.
        List<LanceIndexSegmentInfo> indexSegments = metrics.measure(Stage.INDEXES, () -> loadIndexSegments(dataset));
        if (lanceFieldIds == null) {
            return LanceTableMetadata.createSnapshotWithUnavailableFieldIds(access, resolvedVersion,
                    metrics.measure(Stage.SCHEMA, dataset::getSchema), fragments, indexSegments);
        }
        return LanceTableMetadata.createSnapshotWithIndexes(access, resolvedVersion,
                metrics.measure(Stage.SCHEMA, dataset::getSchema), fragments, lanceFieldIds,
                indexSegments);
    }

    private static List<LanceFragmentInfo> readFragments(Dataset dataset) {
        List<LanceFragmentInfo> fragments = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragments.add(new LanceFragmentInfo(
                    Integer.toUnsignedLong(fragment.getId()), fragment.metadata().getNumRows(),
                    fragment.metadata().getPhysicalRows()));
        }
        return fragments;
    }

    private static Map<String, Integer> loadTopLevelFieldIds(Dataset dataset) {
        LanceSchema schema;
        try {
            schema = dataset.getLanceSchema();
        } catch (IllegalArgumentException e) {
            if (!"ArrowSchema conversion error".equals(e.getMessage())) {
                throw e;
            }
            // Lance v11's JNI converter cannot represent some types (notably Dictionary),
            // even though getSchema() can import the dataset's Arrow schema. The unavailable-ID
            // state makes LanceScalarIndexPlanner choose fragment scans; filters still reach
            // Lance. This does not add support for reading Dictionary values in Doris.
            // Restrict the catch to the SDK call: invalid IDs and duplicate names below must
            // remain errors. Legacy indexes without details are handled separately by the
            // index loader; this schema workaround must not suppress other index errors.
            LOG.warn("Lance SDK schema conversion failed at dataset version {}; "
                    + "disabling FE scalar index segment planning for this snapshot: {}",
                    dataset.version(), e.getMessage());
            return null;
        }
        Map<String, Integer> result = new LinkedHashMap<>();
        for (LanceField field : schema.fields()) {
            if (field.getId() < 0) {
                throw new IllegalStateException(
                        "Lance field '" + field.getName() + "' has invalid id " + field.getId());
            }
            if (result.put(field.getName(), field.getId()) != null) {
                throw new IllegalStateException(
                        "Duplicate top-level Lance field name '" + field.getName() + "'");
            }
        }
        return result;
    }

    private static List<LanceIndexSegmentInfo> loadIndexSegments(Dataset dataset) {
        List<LanceIndexSegmentInfo> result = new ArrayList<>();
        for (IndexDescription description : LanceDatasetIndexDiscovery.describeUserIndexes(dataset)) {
            String metric = parseMetric(description.getDetailsJson());
            for (Index segment : description.getSegments()) {
                if (segment.indexType() == null) {
                    continue;
                }
                List<Long> fragmentIds = segment.fragments()
                        .map(ids -> {
                            List<Long> values = new ArrayList<>(ids.size());
                            for (Integer id : ids) {
                                values.add(Integer.toUnsignedLong(id));
                            }
                            return values;
                        })
                        .orElse(null);
                result.add(new LanceIndexSegmentInfo(segment.uuid(), description.getName(),
                        description.getFieldIds(), fragmentIds, segment.indexType(), metric));
            }
        }
        return result;
    }

    private static String parseMetric(String detailsJson) {
        if (detailsJson == null || detailsJson.isEmpty()) {
            return null;
        }
        try {
            JsonNode metric = JsonUtil.readTree(detailsJson).get("metric_type");
            return metric == null || !metric.isTextual() ? null : metric.asText().toUpperCase(Locale.ROOT);
        } catch (RuntimeException e) {
            // Index details are optional metadata. Malformed details disable metric-sensitive
            // segment planning rather than preventing ordinary table access.
            return null;
        }
    }
}
