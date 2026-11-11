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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Query-local partition adaptation; all other content-file metadata is delegated unchanged. */
final class IcebergPartitionValueAdapter {
    private final int[] sourcePositions;
    private final Type[] sourceTypes;
    private final Type[] targetTypes;
    private final PartitionData template;

    IcebergPartitionValueAdapter(Types.StructType source, Types.StructType target) {
        int size = target.fields().size();
        sourcePositions = new int[size];
        sourceTypes = new Type[size];
        targetTypes = new Type[size];
        template = new PartitionData(target);
        for (int i = 0; i < size; i++) {
            Types.NestedField field = target.fields().get(i);
            targetTypes[i] = field.type();
            sourcePositions[i] = -1;
            for (int j = 0; j < source.fields().size(); j++) {
                if (source.fields().get(j).fieldId() == field.fieldId()) {
                    sourcePositions[i] = j;
                    sourceTypes[i] = source.fields().get(j).type();
                    break;
                }
            }
            if (sourcePositions[i] < 0) {
                throw new IllegalArgumentException("Missing cached partition field " + field.fieldId());
            }
        }
    }

    DataFile adapt(DataFile file) {
        return new DataView(file, adapt((PartitionData) file.partition()));
    }

    DeleteFile adapt(DeleteFile file) {
        return new DeleteView(file, adapt((PartitionData) file.partition()));
    }

    PartitionData adapt(PartitionData source) {
        // Copies reuse the Avro schema; do not reconstruct/serialize it for every file.
        PartitionData original = source.copy();
        PartitionData result = template.copy();
        for (int i = 0; i < sourcePositions.length; i++) {
            if (sourcePositions[i] >= 0) {
                result.set(i, convert(original.get(sourcePositions[i], Object.class), sourceTypes[i], targetTypes[i]));
            }
        }
        return result;
    }

    private static Object convert(Object value, Type source, Type target) {
        if (value == null) {
            return null;
        }
        if (target.typeId() == Type.TypeID.UNKNOWN) {
            // A dropped source has no target type. Different cache loaders may have decoded the same
            // physical value before or after promotion, so preserve its value in a common representation.
            // Iceberg's UNKNOWN comparator uses natural ordering: Integer/Long and Float/Double cannot
            // compare across Java classes and would silently fail data/delete partition-key matching.
            if (source.typeId() == Type.TypeID.INTEGER) {
                return ((Integer) value).longValue();
            }
            if (source.typeId() == Type.TypeID.FLOAT) {
                return ((Float) value).doubleValue();
            }
            // Decimal promotion changes precision only; its Java type and scale already agree.
            return value;
        }
        if (source.equals(target)) {
            return value;
        }
        if (source.typeId() == Type.TypeID.INTEGER && target.typeId() == Type.TypeID.LONG) {
            return ((Integer) value).longValue();
        }
        if (source.typeId() == Type.TypeID.LONG && target.typeId() == Type.TypeID.INTEGER) {
            return Math.toIntExact((Long) value);
        }
        if (source.typeId() == Type.TypeID.FLOAT && target.typeId() == Type.TypeID.DOUBLE) {
            return ((Float) value).doubleValue();
        }
        if (source.typeId() == Type.TypeID.DOUBLE && target.typeId() == Type.TypeID.FLOAT) {
            double original = (Double) value;
            float narrowed = (float) original;
            if (!Double.isNaN(original) && Double.compare(original, (double) narrowed) != 0) {
                throw new IllegalArgumentException("Partition value cannot be represented as float: " + value);
            }
            return narrowed;
        }
        if (source.typeId() == Type.TypeID.DECIMAL && target.typeId() == Type.TypeID.DECIMAL) {
            Types.DecimalType decimalType = (Types.DecimalType) target;
            BigDecimal decimal = ((BigDecimal) value).setScale(decimalType.scale());
            if (decimal.precision() > decimalType.precision()) {
                throw new IllegalArgumentException("Partition value exceeds " + target + ": " + value);
            }
            return decimal;
        }
        throw new IllegalArgumentException("Cannot adapt partition value from " + source + " to " + target);
    }

    private abstract static class FileView<F extends ContentFile<F>> implements ContentFile<F> {
        protected final F file;
        private final PartitionData partition;

        FileView(F file, PartitionData partition) {
            this.file = file;
            this.partition = partition;
        }

        protected abstract F wrap(F copy, PartitionData copiedPartition);

        @Override
        public PartitionData partition() {
            return partition;
        }

        @Override
        public F copy() {
            return wrap(file.copy(), partition.copy());
        }

        @Override
        public F copyWithoutStats() {
            return wrap(file.copyWithoutStats(), partition.copy());
        }

        @Override
        public F copyWithStats(Set<Integer> requestedColumnIds) {
            return wrap(file.copyWithStats(requestedColumnIds), partition.copy());
        }

        @Override
        public String manifestLocation() {
            return file.manifestLocation();
        }

        @Override
        public Long pos() {
            return file.pos();
        }

        @Override
        public int specId() {
            return file.specId();
        }

        @Override
        public FileContent content() {
            return file.content();
        }

        @Override
        public CharSequence path() {
            return file.path();
        }

        @Override
        public String location() {
            return file.location();
        }

        @Override
        public FileFormat format() {
            return file.format();
        }

        @Override
        public long recordCount() {
            return file.recordCount();
        }

        @Override
        public long fileSizeInBytes() {
            return file.fileSizeInBytes();
        }

        @Override
        public Map<Integer, Long> columnSizes() {
            return file.columnSizes();
        }

        @Override
        public Map<Integer, Long> valueCounts() {
            return file.valueCounts();
        }

        @Override
        public Map<Integer, Long> nullValueCounts() {
            return file.nullValueCounts();
        }

        @Override
        public Map<Integer, Long> nanValueCounts() {
            return file.nanValueCounts();
        }

        @Override
        public Map<Integer, ByteBuffer> lowerBounds() {
            return file.lowerBounds();
        }

        @Override
        public Map<Integer, ByteBuffer> upperBounds() {
            return file.upperBounds();
        }

        @Override
        public ByteBuffer keyMetadata() {
            return file.keyMetadata();
        }

        @Override
        public List<Long> splitOffsets() {
            return file.splitOffsets();
        }

        @Override
        public List<Integer> equalityFieldIds() {
            return file.equalityFieldIds();
        }

        @Override
        public Integer sortOrderId() {
            return file.sortOrderId();
        }

        @Override
        public Long dataSequenceNumber() {
            return file.dataSequenceNumber();
        }

        @Override
        public Long fileSequenceNumber() {
            return file.fileSequenceNumber();
        }

        @Override
        public Long firstRowId() {
            return file.firstRowId();
        }
    }

    private static final class DataView extends FileView<DataFile> implements DataFile {
        DataView(DataFile file, PartitionData partition) {
            super(file, partition);
        }

        @Override
        protected DataFile wrap(DataFile copy, PartitionData copiedPartition) {
            return new DataView(copy, copiedPartition);
        }
    }

    private static final class DeleteView extends FileView<DeleteFile> implements DeleteFile {
        DeleteView(DeleteFile file, PartitionData partition) {
            super(file, partition);
        }

        @Override
        protected DeleteFile wrap(DeleteFile copy, PartitionData copiedPartition) {
            return new DeleteView(copy, copiedPartition);
        }

        @Override
        public String referencedDataFile() {
            return file.referencedDataFile();
        }

        @Override
        public Long contentOffset() {
            return file.contentOffset();
        }

        @Override
        public Long contentSizeInBytes() {
            return file.contentSizeInBytes();
        }
    }
}
