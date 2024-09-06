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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map.Entry;

public class HiveColumnValue implements ColumnValue {

    private final Object fieldData;
    private final ObjectInspector fieldInspector;
    private final String timeZone;

    public HiveColumnValue(ObjectInspector fieldInspector, Object fieldData, String timeZone) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
        this.timeZone = timeZone;
    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
    }

    @Override
    public boolean canGetStringAsBytes() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
    }

    @Override
    public byte getByte() {
        Object value = inspectObject();
        if (value instanceof Integer) {
            return ((Integer) value).byteValue();
        }
        return (Byte) inspectObject();
    }

    @Override
    public short getShort() {
        return (short) inspectObject();
    }

    @Override
    public int getInt() {
        return (int) inspectObject();
    }

    @Override
    public float getFloat() {
        return (float) inspectObject();
    }

    @Override
    public long getLong() {
        return (long) inspectObject();
    }

    @Override
    public double getDouble() {
        return (double) inspectObject();
    }

    @Override
    public BigInteger getBigInteger() {
        return null;
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimal) inspectObject()).bigDecimalValue();
    }

    @Override
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        return null;
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay(
                ((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData).toEpochDay());
    }

    @Override
    public LocalDateTime getDateTime() {
        if (fieldData instanceof Timestamp) {
            return ((Timestamp) fieldData).toLocalDateTime();
        } else if (fieldData instanceof TimestampWritableV2) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(
                            ((TimestampObjectInspector) fieldInspector)
                                    .getPrimitiveJavaObject(fieldData).toEpochSecond()),
                    ZoneId.of(timeZone));
        }
        org.apache.hadoop.hive.common.type.Timestamp timestamp
                = ((TimestampObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
        return LocalDateTime.of(timestamp.getYear(), timestamp.getMonth(), timestamp.getDay(), timestamp.getHours(),
                timestamp.getMinutes(), timestamp.getSeconds());
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ListObjectInspector inspector = (ListObjectInspector) fieldInspector;
        List<?> items = inspector.getList(fieldData);
        ObjectInspector itemInspector = inspector.getListElementObjectInspector();
        for (Object item : items) {
            HiveColumnValue hiveColumnValue = null;
            if (item != null) {
                hiveColumnValue = new HiveColumnValue(itemInspector, item, timeZone);
            }
            values.add(hiveColumnValue);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapObjectInspector inspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyObjectInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueObjectInspector = inspector.getMapValueObjectInspector();
        for (Entry kv : inspector.getMap(fieldData).entrySet()) {
            HiveColumnValue key = null;
            HiveColumnValue value = null;
            if (kv.getKey() != null) {
                key = new HiveColumnValue(keyObjectInspector, kv.getKey(), timeZone);
            }
            if (kv.getValue() != null) {
                value = new HiveColumnValue(valueObjectInspector, kv.getValue(), timeZone);
            }
            keys.add(key);
            values.add(value);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructObjectInspector inspector = (StructObjectInspector) fieldInspector;
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        for (Integer idx : structFieldIndex) {
            HiveColumnValue hiveColumnValue = null;
            if (idx != null) {
                StructField field = fields.get(idx);
                Object object = inspector.getStructFieldData(fieldData, field);
                if (object != null) {
                    hiveColumnValue = new HiveColumnValue(field.getFieldObjectInspector(), object, timeZone);
                }
            }
            values.add(hiveColumnValue);
        }
    }
}
