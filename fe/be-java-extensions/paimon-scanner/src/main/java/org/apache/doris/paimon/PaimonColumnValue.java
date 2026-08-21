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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonColumnValue implements ColumnValue {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonColumnValue.class);
    private static final Map<String, String> DORIS_TIME_ZONE_ALIASES;

    static {
        Map<String, String> aliases = new HashMap<>(ZoneId.SHORT_IDS);
        // The scanner cannot depend on FE's TimeUtils, so keep its accepted aliases and CST
        // interpretation identical at this JNI boundary.
        aliases.put("CST", "Asia/Shanghai");
        aliases.put("PRC", "Asia/Shanghai");
        aliases.put("UTC", "UTC");
        aliases.put("GMT", "UTC");
        DORIS_TIME_ZONE_ALIASES = Collections.unmodifiableMap(aliases);
    }

    private int idx;
    private DataGetters record;
    private ColumnType dorisType;
    private DataType dataType;
    private ZoneId timeZone;
    private boolean directBinary;
    private byte[] directBytes;
    // Keep these caches lazy so scalar columns do not pay for complex-type reuse bookkeeping.
    private List<PaimonColumnValue> arrayValues;
    private List<PaimonColumnValue> mapKeys;
    private List<PaimonColumnValue> mapValues;
    private List<PaimonColumnValue> structValues;

    public PaimonColumnValue() {
    }

    public PaimonColumnValue(DataGetters record, int idx, ColumnType columnType, DataType dataType, String timeZone) {
        this(record, idx, columnType, dataType, resolveTimeZone(timeZone));
    }

    private PaimonColumnValue(
            DataGetters record, int idx, ColumnType columnType, DataType dataType, ZoneId timeZone) {
        this.idx = idx;
        this.record = record;
        this.dorisType = columnType;
        this.dataType = dataType;
        this.timeZone = timeZone;
    }

    public void setIdx(int idx, ColumnType dorisType, DataType dataType) {
        this.idx = idx;
        this.dorisType = dorisType;
        this.dataType = dataType;
        this.directBinary = false;
        this.directBytes = null;
    }

    public void setOffsetRow(InternalRow record) {
        this.record = record;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = resolveTimeZone(timeZone);
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean getBoolean() {
        return record.getBoolean(idx);
    }

    @Override
    public byte getByte() {
        return record.getByte(idx);
    }

    @Override
    public short getShort() {
        return record.getShort(idx);
    }

    @Override
    public int getInt() {
        return record.getInt(idx);
    }

    @Override
    public float getFloat() {
        return record.getFloat(idx);
    }

    @Override
    public long getLong() {
        return record.getLong(idx);
    }

    @Override
    public double getDouble() {
        return record.getDouble(idx);
    }

    @Override
    public BigInteger getBigInteger() {
        return BigInteger.valueOf(record.getInt(idx));
    }

    @Override
    public BigDecimal getDecimal() {
        return record.getDecimal(idx, dorisType.getPrecision(), dorisType.getScale()).toBigDecimal();
    }

    @Override
    public String getString() {
        return record.getString(idx).toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return record.getString(idx).toBytes();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(record.getInt(idx));
    }

    @Override
    public LocalDateTime getDateTime() {
        Timestamp ts = record.getTimestamp(idx, dorisType.getPrecision());
        if (dataType instanceof LocalZonedTimestampType) {
            // Paimon stores TIMESTAMP_LTZ as an epoch instant, so convert it directly in the cached session zone.
            return LocalDateTime.ofInstant(ts.toInstant(), timeZone);
        } else {
            return ts.toLocalDateTime();
        }
    }

    @Override
    public LocalDateTime getTimeStampTz() {
        Timestamp ts = record.getTimestamp(idx, dorisType.getPrecision());
        // Timestamp's local representation is identical to converting its epoch instant in UTC.
        return ts.toLocalDateTime();
    }

    @Override
    public boolean isNull() {
        if (directBinary) {
            return directBytes == null;
        }
        if (dataType == null) {
            // Synthetic-null sentinel (see PaimonJniScanner.initReader): the slot exists only on
            // the Doris side (row locator on a merged read), so there is no record index to probe.
            return true;
        }
        boolean isNull = record.isNullAt(idx);
        if (isNull) {
            // A null complex value has no live descendants; release wrappers retained by its prior row.
            clearChildCaches();
        }
        return isNull;
    }

    @Override
    public byte[] getBytes() {
        if (directBinary) {
            return directBytes;
        }
        if (dataType instanceof BlobType) {
            return record.getBlob(idx).toData();
        }
        return record.getBinary(idx);
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        InternalArray recordArray;
        DataType elementPaimonType;
        if (dataType instanceof VectorType) {
            VectorType vectorType = (VectorType) dataType;
            InternalVector vector = record.getVector(idx);
            if (vector.size() != vectorType.getLength()) {
                throw new IllegalArgumentException("Paimon VECTOR length " + vector.size()
                        + " does not match declared length " + vectorType.getLength());
            }
            recordArray = vector;
            elementPaimonType = vectorType.getElementType();
        } else {
            recordArray = record.getArray(idx);
            elementPaimonType = ((ArrayType) dataType).getElementType();
        }
        if (arrayValues == null) {
            arrayValues = new ArrayList<>();
        }
        ColumnType elementDorisType = dorisType.getChildTypes().get(0);
        for (int i = 0; i < recordArray.size(); i++) {
            values.add(reuseColumnValue(arrayValues, i, (DataGetters) recordArray, i,
                    elementDorisType, elementPaimonType));
        }
        trimCache(arrayValues, recordArray.size());
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        InternalMap map = record.getMap(idx);
        if (mapKeys == null) {
            mapKeys = new ArrayList<>();
            mapValues = new ArrayList<>();
        }
        InternalArray key = map.keyArray();
        ColumnType keyDorisType = dorisType.getChildTypes().get(0);
        DataType keyPaimonType = ((MapType) dataType).getKeyType();
        for (int i = 0; i < key.size(); i++) {
            keys.add(reuseColumnValue(mapKeys, i, (DataGetters) key, i,
                    keyDorisType, keyPaimonType));
        }
        trimCache(mapKeys, key.size());
        InternalArray value = map.valueArray();
        ColumnType valueDorisType = dorisType.getChildTypes().get(1);
        DataType valuePaimonType = ((MapType) dataType).getValueType();
        for (int i = 0; i < value.size(); i++) {
            values.add(reuseColumnValue(mapValues, i, (DataGetters) value, i,
                    valueDorisType, valuePaimonType));
        }
        trimCache(mapValues, value.size());
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        if (dataType instanceof VariantType) {
            unpackVariant(structFieldIndex, values);
            return;
        }
        RowType rowType = (RowType) dataType;
        // Projection entries are original child indexes, so the binary row must keep the full RowType arity.
        InternalRow row = record.getRow(idx, rowType.getFieldCount());
        if (structValues == null) {
            structValues = new ArrayList<>();
        }
        for (int i : structFieldIndex) {
            values.add(reuseColumnValue(structValues, i, row, i, dorisType.getChildTypes().get(i),
                    rowType.getFields().get(i).type()));
        }
    }

    private void unpackVariant(List<Integer> structFieldIndex, List<ColumnValue> values) {
        Variant variant = record.getVariant(idx);
        if (structValues == null) {
            structValues = new ArrayList<>();
        }
        for (int fieldIndex : structFieldIndex) {
            if (fieldIndex < 0 || fieldIndex > 1) {
                throw new IllegalArgumentException(
                        "Paimon VARIANT transport only has value and metadata fields");
            }
            byte[] bytes = fieldIndex == 0 ? variant.value() : variant.metadata();
            values.add(reuseBinaryColumnValue(
                    structValues, fieldIndex, bytes, dorisType.getChildTypes().get(fieldIndex)));
        }
    }

    private PaimonColumnValue reuseColumnValue(
            List<PaimonColumnValue> cache, int cacheIndex, DataGetters childRecord, int childIndex,
            ColumnType childDorisType, DataType childPaimonType) {
        while (cache.size() <= cacheIndex) {
            cache.add(null);
        }
        PaimonColumnValue value = cache.get(cacheIndex);
        if (value == null) {
            value = new PaimonColumnValue(
                    childRecord, childIndex, childDorisType, childPaimonType, timeZone);
            cache.set(cacheIndex, value);
        } else {
            // VectorColumn consumes unpacked values synchronously, before this parent advances to another value.
            value.reset(childRecord, childIndex, childDorisType, childPaimonType, timeZone);
        }
        return value;
    }

    private PaimonColumnValue reuseBinaryColumnValue(
            List<PaimonColumnValue> cache, int cacheIndex, byte[] bytes, ColumnType childDorisType) {
        while (cache.size() <= cacheIndex) {
            cache.add(null);
        }
        PaimonColumnValue value = cache.get(cacheIndex);
        if (value == null) {
            value = new PaimonColumnValue();
            cache.set(cacheIndex, value);
        }
        value.resetBinary(bytes, childDorisType);
        return value;
    }

    private void reset(
            DataGetters record, int idx, ColumnType dorisType, DataType dataType, ZoneId timeZone) {
        this.record = record;
        this.idx = idx;
        this.dorisType = dorisType;
        this.dataType = dataType;
        this.timeZone = timeZone;
        this.directBinary = false;
        this.directBytes = null;
    }

    private void resetBinary(byte[] bytes, ColumnType dorisType) {
        this.record = null;
        this.idx = 0;
        this.dorisType = dorisType;
        this.dataType = null;
        this.directBinary = true;
        this.directBytes = bytes;
        clearChildCaches();
    }

    private static ZoneId resolveTimeZone(String timeZone) {
        return ZoneId.of(timeZone, DORIS_TIME_ZONE_ALIASES);
    }

    private static void trimCache(List<PaimonColumnValue> cache, int liveSize) {
        if (cache.size() > liveSize) {
            // Retain only wrappers addressable by the current container, not its historical maximum.
            cache.subList(liveSize, cache.size()).clear();
        }
    }

    private void clearChildCaches() {
        arrayValues = null;
        mapKeys = null;
        mapValues = null;
        structValues = null;
    }
}
