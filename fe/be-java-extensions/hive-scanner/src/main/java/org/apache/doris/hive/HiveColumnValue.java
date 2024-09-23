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

package org.apache.doris.hive;

import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map.Entry;

public class HiveColumnValue implements ColumnValue {

    private static final Logger LOG = LogManager.getLogger(HiveColumnValue.class);
    private final Object fieldData;
    private final ObjectInspector fieldInspector;

    public HiveColumnValue(ObjectInspector fieldInspector, Object fieldData) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
    }

    private Object inspectObject() {
        if (fieldData == null) {
            return null;
        }
        if (fieldInspector instanceof PrimitiveObjectInspector) {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) fieldInspector;
            return poi.getPrimitiveJavaObject(fieldData);
        }
        return fieldData;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return fieldInspector instanceof BinaryObjectInspector;
    }

    @Override
    public boolean isNull() {
        return fieldData == null || inspectObject() == null;
    }

    @Override
    public boolean getBoolean() {
        Object value = inspectObject();
        return value != null && ((Boolean) value);
    }

    @Override
    public byte getByte() {
        Object value = inspectObject();
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        return Byte.parseByte(value.toString());
    }

    @Override
    public short getShort() {
        Object value = inspectObject();
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return Short.parseShort(value.toString());
    }

    @Override
    public int getInt() {
        Object value = inspectObject();
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    @Override
    public float getFloat() {
        Object value = inspectObject();
        if (value == null) {
            return 0.0f;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public long getLong() {
        Object value = inspectObject();
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public double getDouble() {
        Object value = inspectObject();
        if (value == null) {
            return 0.0d;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public BigInteger getBigInteger() {
        Object value = inspectObject();
        if (value == null) {
            return null;
        }
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        return new BigInteger(value.toString());
    }

    @Override
    public BigDecimal getDecimal() {
        Object value = inspectObject();
        if (value == null) {
            return null;
        }
        if (value instanceof HiveDecimal) {
            return ((HiveDecimal) value).bigDecimalValue();
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        return new BigDecimal(value.toString());
    }

    @Override
    public String getString() {
        Object value = inspectObject();
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        if (fieldData == null) {
            return null;
        }
        if (fieldInspector instanceof BinaryObjectInspector) {
            BytesWritable bw = ((BinaryObjectInspector) fieldInspector).getPrimitiveWritableObject(fieldData);
            return bw.copyBytes();
        } else if (fieldInspector instanceof StringObjectInspector) {
            String str = getString();
            return str != null ? str.getBytes() : null;
        }
        return null;
    }

    @Override
    public LocalDate getDate() {
        if (fieldData == null) {
            return null;
        }
        if (fieldInspector instanceof DateObjectInspector) {
            DateObjectInspector doi = (DateObjectInspector) fieldInspector;
            Date hiveDate = doi.getPrimitiveJavaObject(fieldData);
            if (hiveDate != null) {
                return LocalDate.of(hiveDate.getYear(), hiveDate.getMonth(), hiveDate.getDay());
            }
        } else if (fieldInspector instanceof PrimitiveObjectInspector) {
            Object value = inspectObject();
            if (value instanceof Date) {
                Date hiveDate = (Date) value;
                return LocalDate.of(hiveDate.getYear(), hiveDate.getMonth(), hiveDate.getDay());
            }
        }
        return null;
    }

    @Override
    public LocalDateTime getDateTime() {
        if (fieldData == null) {
            return null;
        }
        if (fieldInspector instanceof TimestampObjectInspector) {
            TimestampObjectInspector toi = (TimestampObjectInspector) fieldInspector;
            Timestamp hiveTimestamp = toi.getPrimitiveJavaObject(fieldData);
            if (hiveTimestamp != null) {
                // Convert Hive Timestamp to LocalDateTime
                return LocalDateTime.of(
                        hiveTimestamp.getYear(),
                        hiveTimestamp.getMonth(),
                        hiveTimestamp.getDay(),
                        hiveTimestamp.getHours(),
                        hiveTimestamp.getMinutes(),
                        hiveTimestamp.getSeconds(),
                        hiveTimestamp.getNanos()
                );
            }
        } else if (fieldInspector instanceof DateObjectInspector) {
            LocalDate date = getDate();
            if (date != null) {
                return date.atStartOfDay();
            }
        }
        return null;
    }

    @Override
    public byte[] getBytes() {
        return getStringAsBytes();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        if (fieldData == null) {
            return;
        }
        ListObjectInspector listInspector = (ListObjectInspector) fieldInspector;
        List<?> items = listInspector.getList(fieldData);
        ObjectInspector itemInspector = listInspector.getListElementObjectInspector();
        for (Object item : items) {
            ColumnValue cv = item != null ? new HiveColumnValue(itemInspector, item) : null;
            values.add(cv);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        if (fieldData == null) {
            return;
        }
        MapObjectInspector mapInspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyInspector = mapInspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = mapInspector.getMapValueObjectInspector();
        for (Entry<?, ?> entry : mapInspector.getMap(fieldData).entrySet()) {
            ColumnValue key = entry.getKey() != null ? new HiveColumnValue(keyInspector, entry.getKey()) : null;
            ColumnValue value = entry.getValue() != null ? new HiveColumnValue(valueInspector, entry.getValue()) : null;
            keys.add(key);
            values.add(value);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        if (fieldData == null) {
            return;
        }
        StructObjectInspector structInspector = (StructObjectInspector) fieldInspector;
        List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
        for (Integer idx : structFieldIndex) {
            if (idx != null && idx >= 0 && idx < fields.size()) {
                StructField sf = fields.get(idx);
                Object fieldObj = structInspector.getStructFieldData(fieldData, sf);
                ColumnValue cv = fieldObj != null ? new HiveColumnValue(sf.getFieldObjectInspector(), fieldObj) : null;
                values.add(cv);
            } else {
                values.add(null);
            }
        }
    }
}
