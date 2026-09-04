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

import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Loads one fixed Lance dataset snapshot through the Lance Java SDK. */
public final class LanceMetadataLoader {
    private static final long ALLOCATOR_LIMIT = 256L * 1024 * 1024;

    private LanceMetadataLoader() {
    }

    /**
     * Loads the latest metadata for a directly addressed dataset and owns a short-lived allocator.
     *
     * <p>Called by {@link org.apache.doris.tablefunction.S3TableValuedFunction} when reading a
     * Lance dataset through an S3 TVF.
     */
    public static LanceTableMetadata loadLatestForTvf(
            String datasetUri, List<StorageProperties> storageProperties)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT)) {
            return loadLatest(datasetUri,
                    LanceStorageOptions.fromDorisStorageProperties(datasetUri, storageProperties), allocator);
        }
    }

    /**
     * Loads the latest metadata with the allocator and storage options owned by a catalog.
     *
     * <p>Called by
     * {@link LanceExternalCatalog#loadTableMetadata(String, String, java.util.Optional)} when no
     * time-travel version is requested. Schema, version, and fragments are read from the same
     * opened dataset snapshot.
     */
    public static LanceTableMetadata loadLatest(String datasetUri,
            Map<String, String> lanceStorageOptions, BufferAllocator allocator) throws Exception {
        return loadInternal(
                datasetUri, lanceStorageOptions, OptionalLong.empty(), allocator, false);
    }

    /** Loads the latest fixed snapshot together with vector index segment coverage. */
    public static LanceTableMetadata loadLatestWithIndexSegments(
            String datasetUri, Map<String, String> lanceStorageOptions, BufferAllocator allocator) throws Exception {
        return loadInternal(
                datasetUri, lanceStorageOptions, OptionalLong.empty(), allocator, true);
    }

    /**
     * Loads metadata from an explicitly selected Lance version.
     *
     * <p>Called by
     * {@link LanceExternalCatalog#loadTableMetadata(String, String, java.util.Optional)} for both
     * {@code FOR VERSION AS OF} and the version resolved from {@code FOR TIME AS OF}.
     */
    public static LanceTableMetadata loadVersion(String datasetUri,
            Map<String, String> lanceStorageOptions, long version, BufferAllocator allocator)
            throws Exception {
        return loadInternal(
                datasetUri, lanceStorageOptions, OptionalLong.of(version), allocator, false);
    }

    /** Shared implementation for the latest-version and explicit-version public entry points. */
    private static LanceTableMetadata loadInternal(String datasetUri,
            Map<String, String> lanceStorageOptions, OptionalLong version,
            BufferAllocator allocator, boolean loadIndexSegments) throws Exception {
        try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetUri)
                .readOptions(LanceReadOptions.build(lanceStorageOptions, version)).build()) {
            long resolvedVersion = dataset.version();
            List<LanceFragmentInfo> fragments = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                fragments.add(new LanceFragmentInfo(
                        Integer.toUnsignedLong(fragment.getId()), fragment.metadata().getNumRows(),
                        fragment.metadata().getPhysicalRows()));
            }
            Map<String, Integer> lanceFieldIds = loadIndexSegments
                    ? loadTopLevelFieldIds(dataset) : Collections.emptyMap();
            List<LanceIndexSegmentInfo> indexSegments = loadIndexSegments
                    ? loadVectorIndexSegments(dataset) : Collections.emptyList();
            return loadIndexSegments
                    ? LanceTableMetadata.withIndexSegments(datasetUri, resolvedVersion,
                            dataset.getSchema(), fragments, lanceFieldIds,
                            indexSegments, lanceStorageOptions)
                    : LanceTableMetadata.withoutIndexSegments(datasetUri, resolvedVersion,
                            dataset.getSchema(), fragments, lanceStorageOptions);
        }
    }

    private static Map<String, Integer> loadTopLevelFieldIds(Dataset dataset) {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (LanceField field : dataset.getLanceSchema().fields()) {
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

    private static List<LanceIndexSegmentInfo> loadVectorIndexSegments(Dataset dataset) {
        List<LanceIndexSegmentInfo> result = new ArrayList<>();
        for (IndexDescription description : dataset.describeIndices()) {
            String metric = parseMetric(description.getDetailsJson());
            for (Index segment : description.getSegments()) {
                if (segment.indexType() == null || segment.indexType().getValue() < 100) {
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
                        description.getFieldIds(), fragmentIds, metric));
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
            return metric == null || !metric.isTextual() ? null : metric.asText().toUpperCase();
        } catch (RuntimeException e) {
            // Index details are optional compatibility metadata. An unknown legacy encoding should
            // disable metric-sensitive segment planning rather than prevent ordinary table access.
            return null;
        }
    }
}
