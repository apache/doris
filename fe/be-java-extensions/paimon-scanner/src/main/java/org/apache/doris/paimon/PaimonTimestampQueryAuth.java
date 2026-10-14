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

package org.apache.doris.paimon;

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Keeps SDK authorization and masking after current-precision materialization. */
final class PaimonTimestampQueryAuth extends TableQueryAuthResult {
    private static final long serialVersionUID = 1L;
    private final RowType currentReadType;
    private final TableQueryAuthResult original;

    PaimonTimestampQueryAuth(TableQueryAuthResult original, RowType currentReadType) {
        super(original.filter(), original.columnMasking());
        this.original = original;
        this.currentReadType = currentReadType;
    }

    @Override
    public Predicate extractPredicate() {
        return original.extractPredicate();
    }

    @Override
    public Map<String, Transform> extractColumnMasking() {
        return original.extractColumnMasking();
    }

    static void addPolicyFields(TableQueryAuthResult auth, RowType tableType,
            List<DataField> readFields, List<DataField> currentFields) {
        List<String> projected = readFields.stream().map(DataField::name).collect(Collectors.toList());
        Set<String> required = requiredFields(auth, new LinkedHashSet<>(projected));
        for (DataField field : tableType.getFields()) {
            if (required.contains(field.name())) {
                int index = projected.indexOf(field.name());
                DataField widened = field.newType(PaimonJniScanner.createSafeTimestampReadType(field.type()));
                if (index < 0) {
                    currentFields.add(field);
                    readFields.add(widened);
                } else if (field.type() instanceof RowType || field.type() instanceof ArrayType
                        || field.type() instanceof MapType) {
                    // A policy can read a sibling that SQL pruned from a projected complex column.
                    currentFields.set(index, field);
                    readFields.set(index, widened);
                }
            }
        }
    }

    static boolean readsTimestamp(TableQueryAuthResult auth, RowType tableType, List<String> projected) {
        Set<String> required = requiredFields(auth, new LinkedHashSet<>(projected));
        return tableType.getFields().stream().anyMatch(field -> required.contains(field.name())
                && PaimonJniScanner.containsTimestampType(field.type()));
    }

    private static Set<String> requiredFields(TableQueryAuthResult auth, Set<String> projected) {
        Set<String> required = new LinkedHashSet<>();
        Predicate filter = auth.extractPredicate();
        if (filter != null) {
            required.addAll(PredicateVisitor.collectFieldNames(filter));
        }
        for (Map.Entry<String, Transform> mask : auth.extractColumnMasking().entrySet()) {
            if (!projected.contains(mask.getKey())) {
                continue;
            }
            required.add(mask.getKey());
            for (Object input : mask.getValue().inputs()) {
                if (input instanceof FieldRef) {
                    required.add(((FieldRef) input).name());
                }
            }
        }
        return required;
    }

    static RecordReader<InternalRow> projectOutput(RecordReader<InternalRow> reader,
            RowType currentReadType, RowType outputType) {
        if (currentReadType.equals(outputType)) {
            return reader;
        }
        return reader.transform(row -> repairRow(row, currentReadType, outputType));
    }

    @Override
    public RecordReader<InternalRow> doAuth(RecordReader<InternalRow> reader, RowType readType) {
        // Delegate the policy itself to Paimon; neither filters nor masks are dropped or rewritten.
        if (readType.equals(currentReadType)) {
            return original.doAuth(reader, readType);
        }
        return original.doAuth(reader.transform(row -> repairRow(row, readType, currentReadType)), currentReadType);
    }

    static InternalRow repairRow(InternalRow row, RowType readType, RowType currentType) {
        GenericRow result = new GenericRow(row.getRowKind(), currentType.getFieldCount());
        for (int i = 0; i < currentType.getFieldCount(); i++) {
            int sourceIndex = readType.getFieldIndex(currentType.getFields().get(i).name());
            DataType source = readType.getTypeAt(sourceIndex);
            result.setField(i, repairValue(InternalRowUtils.get(row, sourceIndex, source),
                    source, currentType.getTypeAt(i)));
        }
        return result;
    }

    private static Object repairValue(Object value, DataType readType, DataType currentType) {
        if (value == null || readType.equals(currentType)) {
            return value;
        }
        if (currentType instanceof TimestampType || currentType instanceof LocalZonedTimestampType) {
            return PaimonColumnValue.truncateTimestampPrecision(
                    (Timestamp) value, DataTypeChecks.getPrecision(currentType));
        }
        if (currentType instanceof RowType) {
            return repairRow((InternalRow) value, (RowType) readType, (RowType) currentType);
        }
        if (currentType instanceof ArrayType) {
            InternalArray array = (InternalArray) value;
            Object[] elements = new Object[array.size()];
            DataType source = ((ArrayType) readType).getElementType();
            DataType target = ((ArrayType) currentType).getElementType();
            for (int i = 0; i < elements.length; i++) {
                elements[i] = repairValue(InternalRowUtils.get(array, i, source), source, target);
            }
            return new GenericArray(elements);
        }
        if (currentType instanceof MapType) {
            InternalMap map = (InternalMap) value;
            MapType source = (MapType) readType;
            DataType target = ((MapType) currentType).getValueType();
            Map<Object, Object> entries = new LinkedHashMap<>();
            for (int i = 0; i < map.size(); i++) {
                entries.put(InternalRowUtils.get(map.keyArray(), i, source.getKeyType()),
                        repairValue(InternalRowUtils.get(map.valueArray(), i, source.getValueType()),
                                source.getValueType(), target));
            }
            return new GenericMap(entries);
        }
        return value;
    }
}
