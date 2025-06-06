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

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class IcebergPartitionsJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "partitions";
    private static final Map<String, String> PARTITIONS_SCHEMA = new HashMap<>();

    static {
        PARTITIONS_SCHEMA.put("partition", "string"); // TODO: parse parititon
        PARTITIONS_SCHEMA.put("spec_id", "int");
        PARTITIONS_SCHEMA.put("record_count", "bigint");
        PARTITIONS_SCHEMA.put("file_count", "bigint");
        PARTITIONS_SCHEMA.put("total_data_file_size_in_bytes", "bigint");
        PARTITIONS_SCHEMA.put("position_delete_record_count", "bigint");
        PARTITIONS_SCHEMA.put("position_delete_file_count", "bigint");
        PARTITIONS_SCHEMA.put("equality_delete_record_count", "bigint");
        PARTITIONS_SCHEMA.put("equality_delete_file_count", "bigint");
        PARTITIONS_SCHEMA.put("last_updated_at", "bigint");
    }

    private List<PartitionField> partitionFields;
    private Long lastUpdateTime;
    private Integer specId;
    private GenericRecord reusedRecord;

    // A serializable bean that contains a bare minimum to read a manifest
    private final ManifestFile manifestBean;

    public IcebergPartitionsJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        manifestBean = SerializationUtil.deserializeFromBase64(params.get("serialized_split"));
    }

    @Override
    protected void initReader() throws IOException {
        this.specId = manifestBean.partitionSpecId();
        this.lastUpdateTime = table.snapshot(manifestBean.snapshotId()) != null
                ? table.snapshot(manifestBean.snapshotId()).timestampMillis()
                : null;
        this.partitionFields = getAllPartitionFields(table);
        // TODO: Initialize the reused record with partition fields
        // this.reusedRecord = GenericRecord.create(getResultType());
        if (manifestBean.content() == ManifestContent.DATA) {
            reader = ManifestFiles.read(manifestBean, table.io(), table.specs()).iterator();
        } else {
            reader = ManifestFiles.readDeleteManifest(manifestBean, table.io(), table.specs()).iterator();
        }
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return PARTITIONS_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        ContentFile<?> file = (ContentFile<?>) row;
        FileContent content = file.content();
        switch (columnName) {
            case "partition_value":
                return getPartitionValues((PartitionData) file.partition());
            case "spec_id":
                return specId;
            case "record_count":
                return content == FileContent.DATA ? file.recordCount() : 0;
            case "file_count":
                return content == FileContent.DATA ? 1L : 0L;
            case "total_data_file_size_in_bytes":
                return content == FileContent.DATA ? file.fileSizeInBytes() : 0;
            case "position_delete_record_count":
                return content == FileContent.POSITION_DELETES ? file.recordCount() : 0;
            case "position_delete_file_count":
                return content == FileContent.POSITION_DELETES ? 1L : 0L;
            case "equality_delete_record_count":
                return content == FileContent.EQUALITY_DELETES ? file.recordCount() : 0;
            case "equality_delete_file_count":
                return content == FileContent.EQUALITY_DELETES ? 1L : 0L;
            case "last_updated_at":
                return lastUpdateTime;
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }

    private Object getPartitionValues(PartitionData partitionData) {
        List<Types.NestedField> fileFields = partitionData.getPartitionType().fields();
        Map<Integer, Integer> fieldIdToPos = new HashMap<>();
        for (int i = 0; i < fileFields.size(); i++) {
            fieldIdToPos.put(fileFields.get(i).fieldId(), i);
        }

        for (PartitionField partitionField : partitionFields) {
            Integer fieldId = partitionField.fieldId();
            String name = partitionField.name();
            if (fieldIdToPos.containsKey(fieldId)) {
                int pos = fieldIdToPos.get(fieldId);
                Type fieldType = partitionData.getType(pos);
                Object partitionValue = partitionData.get(pos);
                if (partitionField.transform().isIdentity() && Types.TimestampType.withZone().equals(fieldType)) {
                    partitionValue = ((long) partitionValue) / 1000;
                }
                reusedRecord.setField(name, partitionValue);
            } else {
                reusedRecord.setField(name, null);
            }
        }
        return reusedRecord;
    }

    public static List<PartitionField> getAllPartitionFields(Table icebergTable) {
        Set<Integer> existingColumnsIds = icebergTable.schema()
                .columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toSet());

        List<PartitionField> visiblePartitionFields = icebergTable.specs()
                .values().stream()
                .flatMap(partitionSpec -> partitionSpec.fields().stream())
                .filter(partitionField -> existingColumnsIds.contains(partitionField.sourceId()))
                .collect(Collectors.toList());

        return filterDuplicates(visiblePartitionFields);
    }

    public static List<PartitionField> filterDuplicates(List<PartitionField> visiblePartitionFields) {
        Set<Integer> existingFieldIds = new HashSet<>();
        List<PartitionField> result = new ArrayList<>();
        for (PartitionField partitionField : visiblePartitionFields) {
            if (!existingFieldIds.contains(partitionField.fieldId())) {
                existingFieldIds.add(partitionField.fieldId());
                result.add(partitionField);
            }
        }
        return result;
    }
}
