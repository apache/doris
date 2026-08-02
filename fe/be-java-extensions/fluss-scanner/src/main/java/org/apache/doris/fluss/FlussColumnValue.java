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

package org.apache.doris.fluss;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One column of one fluss row, read the way Doris's vector table wants it.
 *
 * <p>Two fluss-specific things this has to get right, both of which a paimon-shaped port would miss:
 *
 * <ul>
 *   <li>Fixed-width reads carry their width. {@code getChar} and {@code getBinary} take the declared
 *       length, so the fluss type — not just the Doris one — has to be around at read time.</li>
 *   <li>Timestamps come from two different accessors rather than one. A plain TIMESTAMP is
 *       {@code getTimestampNtz} and reads back as the wall clock that was written; a TIMESTAMP WITH
 *       LOCAL TIME ZONE is {@code getTimestampLtz}, an instant, which only becomes a wall clock once
 *       a zone is applied. Reading one through the other's accessor throws rather than silently
 *       shifting the value, which is why the fluss type is kept rather than inferred from the Doris
 *       side.</li>
 * </ul>
 *
 * <p>Instances are reused across rows and, for nested values, across containers: the vector table
 * consumes an unpacked value before the parent moves on, so a cache keyed by position is safe and
 * keeps a wide nested column from allocating per row.
 */
public class FlussColumnValue implements ColumnValue {

    private static final Map<String, String> DORIS_TIME_ZONE_ALIASES;

    static {
        Map<String, String> aliases = new HashMap<>(ZoneId.SHORT_IDS);
        // The scanner cannot depend on FE's TimeUtils, so keep its accepted aliases and CST
        // interpretation identical at this JNI boundary (same list as the paimon scanner's).
        aliases.put("CST", "Asia/Shanghai");
        aliases.put("PRC", "Asia/Shanghai");
        aliases.put("UTC", "UTC");
        aliases.put("GMT", "UTC");
        DORIS_TIME_ZONE_ALIASES = Collections.unmodifiableMap(aliases);
    }

    private DataGetters record;
    private int idx;
    private ColumnType dorisType;
    private DataType flussType;
    private ZoneId timeZone;

    // Lazy: a scalar column never pays for the nested-reuse bookkeeping.
    private List<FlussColumnValue> arrayValues;
    private List<FlussColumnValue> mapKeys;
    private List<FlussColumnValue> mapValues;
    private List<FlussColumnValue> structValues;

    public FlussColumnValue(String timeZone) {
        this.timeZone = resolveTimeZone(timeZone);
    }

    private FlussColumnValue(DataGetters record, int idx, ColumnType dorisType, DataType flussType,
            ZoneId timeZone) {
        this.record = record;
        this.idx = idx;
        this.dorisType = dorisType;
        this.flussType = flussType;
        this.timeZone = timeZone;
    }

    /** Points this value at the row the scanner is currently emitting. */
    public void setRow(InternalRow record) {
        this.record = record;
    }

    /** Points this value at one column of that row. */
    public void setIdx(int idx, ColumnType dorisType, DataType flussType) {
        this.idx = idx;
        this.dorisType = dorisType;
        this.flussType = flussType;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        boolean isNull = record.isNullAt(idx);
        if (isNull) {
            // A null container has no live descendants; drop wrappers its previous row left behind.
            clearChildCaches();
        }
        return isNull;
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
    public long getLong() {
        return record.getLong(idx);
    }

    @Override
    public float getFloat() {
        return record.getFloat(idx);
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
        return new String(readBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getStringAsBytes() {
        return readBytes();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(record.getInt(idx));
    }

    @Override
    public LocalDateTime getDateTime() {
        if (flussType instanceof LocalZonedTimestampType) {
            // Stored as an instant, so it only becomes a wall clock in a zone — the session's.
            return LocalDateTime.ofInstant(
                    record.getTimestampLtz(idx, precisionOf(flussType)).toInstant(), timeZone);
        }
        return record.getTimestampNtz(idx, precisionOf(flussType)).toLocalDateTime();
    }

    @Override
    public LocalDateTime getTimeStampTz() {
        // TIMESTAMPTZ keeps the instant itself, and Doris carries it as the UTC wall clock — NOT the
        // session zone's, which is what getDateTime above applies.
        return LocalDateTime.ofInstant(
                record.getTimestampLtz(idx, precisionOf(flussType)).toInstant(), ZoneOffset.UTC);
    }

    @Override
    public byte[] getBytes() {
        return readBytes();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        InternalArray array = record.getArray(idx);
        if (arrayValues == null) {
            arrayValues = new ArrayList<>();
        }
        ColumnType elementDorisType = dorisType.getChildTypes().get(0);
        DataType elementFlussType = ((ArrayType) flussType).getElementType();
        for (int i = 0; i < array.size(); i++) {
            values.add(reuse(arrayValues, i, array, i, elementDorisType, elementFlussType));
        }
        trim(arrayValues, array.size());
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        InternalMap map = record.getMap(idx);
        if (mapKeys == null) {
            mapKeys = new ArrayList<>();
            mapValues = new ArrayList<>();
        }
        InternalArray keyArray = map.keyArray();
        ColumnType keyDorisType = dorisType.getChildTypes().get(0);
        DataType keyFlussType = ((MapType) flussType).getKeyType();
        for (int i = 0; i < keyArray.size(); i++) {
            keys.add(reuse(mapKeys, i, keyArray, i, keyDorisType, keyFlussType));
        }
        trim(mapKeys, keyArray.size());

        InternalArray valueArray = map.valueArray();
        ColumnType valueDorisType = dorisType.getChildTypes().get(1);
        DataType valueFlussType = ((MapType) flussType).getValueType();
        for (int i = 0; i < valueArray.size(); i++) {
            values.add(reuse(mapValues, i, valueArray, i, valueDorisType, valueFlussType));
        }
        trim(mapValues, valueArray.size());
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        RowType rowType = (RowType) flussType;
        // The indexes are into the FULL child list, so the nested row must keep its whole arity.
        InternalRow row = record.getRow(idx, rowType.getFieldCount());
        if (structValues == null) {
            structValues = new ArrayList<>();
        }
        for (int i : structFieldIndex) {
            values.add(reuse(structValues, i, row, i, dorisType.getChildTypes().get(i),
                    rowType.getTypeAt(i)));
        }
    }

    /**
     * The raw bytes of a character or binary column, chosen by the FLUSS type rather than the Doris
     * one. That distinction matters because the two do not line up one to one: fluss BYTES maps to
     * Doris STRING unless the catalog turns on VARBINARY mapping, so the same fluss column can be
     * asked for as a string here or as bytes there. Which accessor fluss needs is a property of how
     * fluss stored the value, and CHAR and BINARY are stored fixed-width — reading them without their
     * declared length gets nothing back.
     */
    private byte[] readBytes() {
        switch (flussType.getTypeRoot()) {
            case CHAR:
                return record.getChar(idx, ((CharType) flussType).getLength()).toBytes();
            case STRING:
                return record.getString(idx).toBytes();
            case BINARY:
                return record.getBinary(idx, ((BinaryType) flussType).getLength());
            case BYTES:
                return record.getBytes(idx);
            default:
                throw new IllegalStateException(
                        "fluss type " + flussType + " has no byte representation to read");
        }
    }

    private FlussColumnValue reuse(List<FlussColumnValue> cache, int cacheIndex,
            DataGetters childRecord, int childIndex, ColumnType childDorisType, DataType childFlussType) {
        while (cache.size() <= cacheIndex) {
            cache.add(null);
        }
        FlussColumnValue value = cache.get(cacheIndex);
        if (value == null) {
            value = new FlussColumnValue(childRecord, childIndex, childDorisType, childFlussType, timeZone);
            cache.set(cacheIndex, value);
        } else {
            value.record = childRecord;
            value.idx = childIndex;
            value.dorisType = childDorisType;
            value.flussType = childFlussType;
            value.timeZone = timeZone;
        }
        return value;
    }

    private static int precisionOf(DataType type) {
        return org.apache.fluss.types.DataTypeChecks.getPrecision(type);
    }

    private static ZoneId resolveTimeZone(String timeZone) {
        return ZoneId.of(timeZone, DORIS_TIME_ZONE_ALIASES);
    }

    private static void trim(List<FlussColumnValue> cache, int liveSize) {
        if (cache.size() > liveSize) {
            // Keep only what the CURRENT container can address, not its historical maximum.
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
