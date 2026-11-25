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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.LanceMetadataMetrics.Stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Supplier;

/**
 * The top-level field ids and the user indexes of one fixed dataset version.
 *
 * <p>Read only for a scan that can use an index segment: a scan with a pushed predicate and no debug
 * grouping, or a vector or full-text search. Schema reads and plain scans never read it, so an index the SDK
 * cannot describe does not stop them.
 */
final class LanceIndexMetadata {
    private static final Logger LOG = LogManager.getLogger(LanceIndexMetadata.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final boolean fieldIdsAvailable;
    private final Map<String, Integer> lanceFieldIds;
    private final List<LanceIndexSegmentGroup> indexes;

    LanceIndexMetadata(Map<String, Integer> lanceFieldIds, List<LanceIndexSegmentInfo> indexSegments) {
        this(true, lanceFieldIds, indexSegments);
    }

    private LanceIndexMetadata(boolean fieldIdsAvailable, Map<String, Integer> lanceFieldIds,
            List<LanceIndexSegmentInfo> indexSegments) {
        this.fieldIdsAvailable = fieldIdsAvailable;
        this.lanceFieldIds = Collections.unmodifiableMap(new HashMap<>(lanceFieldIds));
        this.indexes = LanceIndexSegmentGroup.groupByName(indexSegments);
    }

    /** Keeps the discovered indexes when the SDK cannot map the schema's fields to Lance field ids. */
    static LanceIndexMetadata withUnavailableFieldIds(List<LanceIndexSegmentInfo> indexSegments) {
        return new LanceIndexMetadata(false, Collections.emptyMap(), indexSegments);
    }

    /** Reads the field ids and the index segments of the caller's open snapshot, timing each into {@code timer}. */
    static LanceIndexMetadata read(Dataset dataset, LanceMetadataMetrics.Timer timer) {
        return read(dataset::getLanceSchema, () -> LanceDatasetIndexDiscovery.describeUserIndexes(dataset),
                dataset.version(), timer);
    }

    /**
     * {@link #read(Dataset, LanceMetadataMetrics.Timer)} over the two SDK reads it makes: the Lance schema, and the
     * validated user indexes.
     */
    static LanceIndexMetadata read(Supplier<LanceSchema> lanceSchema, Supplier<List<IndexDescription>> userIndexes,
            long version, LanceMetadataMetrics.Timer timer) {
        Map<String, Integer> lanceFieldIds = timer.measure(Stage.FIELD_IDS,
                () -> loadTopLevelFieldIds(lanceSchema, version));
        // Keep index validation even when the known schema conversion error disables field-ID mapping.
        List<LanceIndexSegmentInfo> indexSegments = timer.measure(Stage.INDEXES,
                () -> loadIndexSegments(userIndexes.get()));
        return lanceFieldIds == null
                ? withUnavailableFieldIds(indexSegments)
                : new LanceIndexMetadata(lanceFieldIds, indexSegments);
    }

    /** Whether index segments can be planned: the field ids that name the indexed columns are known. */
    boolean canPlanIndexSegments() {
        return fieldIdsAvailable;
    }

    List<LanceIndexSegmentGroup> getIndexes() {
        return indexes;
    }

    OptionalInt getLanceFieldId(String fieldName) {
        Integer fieldId = lanceFieldIds.get(fieldName);
        return fieldId == null ? OptionalInt.empty() : OptionalInt.of(fieldId);
    }

    private static Map<String, Integer> loadTopLevelFieldIds(Supplier<LanceSchema> lanceSchema, long version) {
        LanceSchema schema;
        try {
            schema = lanceSchema.get();
        } catch (IllegalArgumentException e) {
            if (!"ArrowSchema conversion error".equals(e.getMessage())) {
                throw e;
            }
            // Lance's JNI converter cannot represent some types (notably Dictionary), even though
            // getSchema() can import the dataset's Arrow schema. Without field ids no index segment is
            // planned and the scan reads fragments; the filter still reaches Lance. Restrict the catch to
            // the SDK call: invalid ids and duplicate names below must remain errors.
            LOG.warn("Lance SDK schema conversion failed at dataset version {}; "
                    + "disabling FE scalar index segment planning for this snapshot: {}",
                    version, e.getMessage());
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

    private static List<LanceIndexSegmentInfo> loadIndexSegments(List<IndexDescription> userIndexes) {
        List<LanceIndexSegmentInfo> result = new ArrayList<>();
        for (IndexDescription description : userIndexes) {
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

    /**
     * The {@code metric_type} a vector index's details name, upper case; null when they name none. A vector
     * search uses only the segments of an index built with the metric it asks for, so an index whose details
     * are missing or malformed is not used for one; the details never stop an ordinary scan.
     */
    static String parseMetric(String detailsJson) {
        if (detailsJson == null || detailsJson.isEmpty()) {
            return null;
        }
        try {
            JsonNode metric = JSON.readTree(detailsJson).get("metric_type");
            return metric == null || !metric.isTextual() ? null : metric.asText().toUpperCase(Locale.ROOT);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
