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

package org.apache.doris.iceberg;

import org.apache.doris.common.classloader.ThreadClassLoaderContext;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * JniScanner for Doris internal iceberg metadata planning system table.
 *
 * <p>The scanner consumes FE-sharded manifest paths and emits file-level metadata
 * rows that FE will later convert into scan tasks / splits.
 */
public class IcebergMetadataPlanningJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataPlanningJniScanner.class);
    private static final String HADOOP_OPTION_PREFIX = "hadoop.";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String COL_CONTENT = "content";
    private static final String COL_FILE_PATH = "file_path";
    private static final String COL_FILE_FORMAT = "file_format";
    private static final String COL_SPEC_ID = "spec_id";
    private static final String COL_PARTITION_DATA = "partition_data";
    private static final String COL_RECORD_COUNT = "record_count";
    private static final String COL_FILE_SIZE = "file_size_in_bytes";
    private static final String COL_SPLIT_OFFSETS = "split_offsets";
    private static final String COL_FIRST_ROW_ID = "first_row_id";
    private static final String COL_FILE_SEQUENCE_NUMBER = "file_sequence_number";
    private static final String COL_PREDICATE = "predicate";

    private final ClassLoader classLoader;
    private final PreExecutionAuthenticator preExecutionAuthenticator;
    private final String timezone;
    private final PlanningScanInput scanInput;
    private final Table table;
    private final Expression predicate;
    private Iterator<ContentFile<?>> reader = Collections.emptyIterator();
    private List<CloseableIterable<? extends ContentFile<?>>> readers = Collections.emptyList();
    private List<CloseableIterator<? extends ContentFile<?>>> iterators = Collections.emptyList();

    public IcebergMetadataPlanningJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        String serializedSplitParams = params.get("serialized_split");
        Preconditions.checkArgument(serializedSplitParams != null && !serializedSplitParams.isEmpty(),
                "serialized_split should not be empty");
        this.scanInput = parseScanInput(serializedSplitParams);
        Preconditions.checkArgument(scanInput.snapshotId > 0, "snapshotId should be positive");
        Preconditions.checkArgument(scanInput.serializedTable != null && !scanInput.serializedTable.isEmpty(),
                "serializedTable should not be empty");
        Preconditions.checkArgument(scanInput.serializedPredicate != null
                        && !scanInput.serializedPredicate.isEmpty(),
                "serializedPredicate should not be empty");
        Preconditions.checkArgument(scanInput.serializedManifests != null && !scanInput.serializedManifests.isEmpty(),
                "serializedManifests should not be empty");
        this.table = SerializationUtil.deserializeFromBase64(scanInput.serializedTable);
        this.predicate = SerializationUtil.deserializeFromBase64(scanInput.serializedPredicate);
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        Map<String, String> hadoopOptionParams = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_OPTION_PREFIX)) {
                hadoopOptionParams.put(entry.getKey().substring(HADOOP_OPTION_PREFIX.length()),
                        entry.getValue());
            }
        }
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
        String[] requiredFields = params.get("required_fields").split(",");
        ColumnType[] requiredTypes = parseRequiredTypes(params.get("required_types").split("#"), requiredFields);
        initTableInfo(requiredTypes, requiredFields, batchSize);
    }

    @Override
    public void open() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            preExecutionAuthenticator.execute(() -> {
                initReader();
                return null;
            });
        } catch (Exception e) {
            close();
            throw new IOException("Failed to open Iceberg metadata planning scanner", e);
        }
    }

    @Override
    protected int getNext() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            int rows = 0;
            while (rows < getBatchSize() && reader.hasNext()) {
                ContentFile<?> file = reader.next();
                for (int i = 0; i < fields.length; i++) {
                    Object fieldData = getFieldValue(fields[i], file);
                    ColumnValue fieldValue = fieldData == null ? null
                            : new IcebergSysTableColumnValue(fieldData, timezone);
                    appendData(i, fieldValue);
                }
                rows++;
            }
            return rows;
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            IOException closeException = null;
            for (CloseableIterator<? extends ContentFile<?>> iterator : iterators) {
                try {
                    iterator.close();
                } catch (IOException e) {
                    if (closeException == null) {
                        closeException = e;
                    }
                }
            }
            iterators = Collections.emptyList();
            for (CloseableIterable<? extends ContentFile<?>> closeable : readers) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    if (closeException == null) {
                        closeException = e;
                    }
                }
            }
            readers = Collections.emptyList();
            reader = Collections.emptyIterator();
            if (closeException != null) {
                throw closeException;
            }
        }
    }

    private void initReader() {
        Snapshot snapshot = table.snapshot(scanInput.snapshotId);
        Preconditions.checkState(snapshot != null, "Snapshot %s not found for metadata planning",
                scanInput.snapshotId);
        Map<Integer, PartitionSpec> specs = table.specs();
        List<CloseableIterable<? extends ContentFile<?>>> planningReaders = new ArrayList<>();
        for (String serializedManifest : scanInput.serializedManifests) {
            // FE already prunes the candidate manifests for this task. We decode those ManifestFile
            // descriptors directly here so the scanner does not re-scan the entire snapshot manifest list
            // just to recover path -> ManifestFile for each task.
            ManifestFile manifest = decodeManifest(serializedManifest);
            if (manifest.content() == ManifestContent.DATA) {
                planningReaders.add(ManifestFiles.read(manifest, table.io(), specs)
                        .filterRows(predicate)
                        .caseSensitive(false));
            }
        }
        this.readers = planningReaders;
        this.reader = flatten(planningReaders);
    }

    private ManifestFile decodeManifest(String serializedManifest) {
        try {
            return ManifestFiles.decode(Base64.getDecoder().decode(serializedManifest));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decode metadata planning manifest payload", e);
        }
    }

    private Object getFieldValue(String columnName, ContentFile<?> file) {
        switch (columnName) {
            case COL_CONTENT:
                return file.content().id();
            case COL_FILE_PATH:
                return file.path().toString();
            case COL_FILE_FORMAT:
                return file.format().toString();
            case COL_SPEC_ID:
                return file.specId();
            case COL_PARTITION_DATA:
                return getPartitionDataJson(file);
            case COL_RECORD_COUNT:
                return file.recordCount();
            case COL_FILE_SIZE:
                return file.fileSizeInBytes();
            case COL_SPLIT_OFFSETS:
                return file.splitOffsets();
            case COL_FIRST_ROW_ID:
                return file.firstRowId();
            case COL_FILE_SEQUENCE_NUMBER:
                return file.fileSequenceNumber();
            case COL_PREDICATE:
                return scanInput.serializedPredicate;
            default:
                throw new IllegalArgumentException("Unsupported metadata planning column: " + columnName);
        }
    }

    private String getPartitionDataJson(ContentFile<?> file) {
        PartitionSpec spec = table.specs().get(file.specId());
        if (spec == null || spec.fields().isEmpty()) {
            return null;
        }
        org.apache.iceberg.StructLike partition = file.partition();
        List<NestedField> fields = spec.partitionType().asNestedType().fields();
        Class<?>[] classes = spec.javaClasses();
        Preconditions.checkState(fields.size() == spec.fields().size(),
                "Partition fields size does not match partition spec fields size");
        List<String> values = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            values.add(serializePartitionValue(fields.get(i).type(), partition.get(i, classes[i])));
        }
        return serializePartitionData(values);
    }

    private String serializePartitionData(List<String> values) {
        try {
            return OBJECT_MAPPER.writeValueAsString(values);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize partition data", e);
        }
    }

    private String serializePartitionValue(org.apache.iceberg.types.Type type, Object value) {
        switch (type.typeId()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case DECIMAL:
                return value == null ? null : value.toString();
            case DATE:
                return value == null ? null : LocalDate.ofEpochDay((Integer) value)
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIME:
                return value == null ? null : LocalTime.ofNanoOfDay(((Long) value) * 1000)
                        .format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP:
                // Store the raw epoch-microseconds value as a plain numeric string to avoid
                // any timezone-conversion round-trip loss between the BE JNI scanner and the
                // FE DistributedPlanningSplitProducer. FE restores the Long directly without
                // interpreting the string as a human-readable datetime.
                return value == null ? null : value.toString();
            default:
                throw new UnsupportedOperationException("Unsupported partition type: " + type);
        }
    }

    private Iterator<ContentFile<?>> flatten(List<CloseableIterable<? extends ContentFile<?>>> readers) {
        List<CloseableIterator<? extends ContentFile<?>>> openedIterators = new ArrayList<>(readers.size());
        for (CloseableIterable<? extends ContentFile<?>> iterable : readers) {
            openedIterators.add(iterable.iterator());
        }
        this.iterators = openedIterators;
        return new Iterator<ContentFile<?>>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                while (index < openedIterators.size()) {
                    if (openedIterators.get(index).hasNext()) {
                        return true;
                    }
                    index++;
                }
                return false;
            }

            @Override
            public ContentFile<?> next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                return openedIterators.get(index).next();
            }
        };
    }

    private static PlanningScanInput parseScanInput(String serializedSplitParams) {
        try {
            return OBJECT_MAPPER.readValue(serializedSplitParams, PlanningScanInput.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse metadata planning split payload", e);
        }
    }

    private static ColumnType[] parseRequiredTypes(String[] typeStrings, String[] requiredFields) {
        ColumnType[] requiredTypes = new ColumnType[typeStrings.length];
        for (int i = 0; i < typeStrings.length; i++) {
            String type = typeStrings[i];
            ColumnType parsedType = ColumnType.parseType(requiredFields[i], type);
            if (parsedType.isUnsupported()) {
                throw new IllegalArgumentException("Unsupported type " + type + " for field " + requiredFields[i]);
            }
            requiredTypes[i] = parsedType;
        }
        return requiredTypes;
    }

    private static class PlanningScanInput {
        public long snapshotId;
        public String serializedTable;
        public String serializedPredicate;
        public List<String> serializedManifests = Collections.emptyList();
    }
}
