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

package org.apache.doris.hudi;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class HadoopHudiColumnValue implements ColumnValue {
    private ColumnType dorisType;
    private ObjectInspector fieldInspector;
    private Object fieldData;
    private final ZoneId zoneId;

    public HadoopHudiColumnValue(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public void setRow(Object record) {
        this.fieldData = record;
    }

    public void setField(ColumnType dorisType, ObjectInspector fieldInspector) {
        this.dorisType = dorisType;
        this.fieldInspector = fieldInspector;
    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
    }

    @Override
    public short getShort() {
        // hudi lsm will read parquet short type as IntWritable
        if (fieldData instanceof IntWritable) {
            return (short) ((IntWritable) fieldData).get();
        }
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
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public byte[] getBytes() {
        byte[] bytes = getStringAsBytes();
        if (bytes != null) {
            return bytes;
        }
        Object value = inspectObject();
        return value instanceof byte[] ? (byte[]) value : null;
    }


    @Override
    public byte getByte() {
        throw new UnsupportedOperationException("Hoodie type does not support tinyint");
    }

    @Override
    public LocalDateTime getTimeStampTz() {
        return ((Timestamp) fieldData).toLocalDateTime();
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimal) inspectObject()).bigDecimalValue();
    }

    @Override
    public LocalDate getDate() {
        if (fieldData instanceof DateWritable) {
            DateWritable dateWritable = (DateWritable) fieldData;
            return LocalDate.ofEpochDay(dateWritable.getDays());
        } else {
            return LocalDate.ofEpochDay((((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData))
                .toEpochDay());
        }
    }

    @Override
    public LocalDateTime getDateTime() {
        if (fieldData instanceof org.apache.hadoop.hive.common.type.Timestamp) {
            return ((org.apache.hadoop.hive.common.type.Timestamp) fieldData).toSqlTimestamp().toLocalDateTime();
        } else if (fieldData instanceof TimestampWritableV2) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond((((TimestampObjectInspector) fieldInspector)
                    .getPrimitiveJavaObject(fieldData)).toEpochSecond()), zoneId);
        } else if (fieldData instanceof TimestampWritable) {
            return (((org.apache.hadoop.hive.serde2.io.TimestampWritable) fieldData).getTimestamp()).toLocalDateTime();
        } else {
            long datetime = ((LongWritable) fieldData).get();
            long seconds;
            long nanoseconds;
            if (dorisType.getPrecision() == 3) {
                seconds = datetime / 1000;
                nanoseconds = (datetime % 1000) * 1000000;
            } else if (dorisType.getPrecision() == 6) {
                seconds = datetime / 1000000;
                nanoseconds = (datetime % 1000000) * 1000;
            } else {
                throw new RuntimeException("Hoodie timestamp only support milliseconds and microseconds, "
                        + "wrong precision = " + dorisType.getPrecision());
            }
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoseconds), zoneId);
        }
    }

    @Override
    public boolean canGetStringAsBytes() {
        return fieldInspector instanceof BinaryObjectInspector;
    }

    @Override
    public boolean isNull() {
        return fieldData == null;
    }

    @Override
    public BigInteger getBigInteger() {
        throw new UnsupportedOperationException("Hoodie type does not support largeint");
    }

    @Override
    public byte[] getStringAsBytes() {
        if (fieldData == null) {
            return null;
        }
        if (fieldInspector instanceof BinaryObjectInspector) {
            BinaryObjectInspector binaryInspector = (BinaryObjectInspector) fieldInspector;
            if (fieldData instanceof BytesWritable) {
                return ((BytesWritable) fieldData).copyBytes();
            }
            Object value = binaryInspector.getPrimitiveJavaObject(fieldData);
            return value instanceof byte[] ? (byte[]) value : null;
        }
        return null;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ListObjectInspector inspector = (ListObjectInspector) fieldInspector;
        List<?> items = inspector.getList(fieldData);
        ObjectInspector itemInspector = inspector.getListElementObjectInspector();
        ColumnType itemType = dorisType.getChildTypes().get(0);
        for (int i = 0; i < items.size(); i++) {
            Object item = items.get(i);
            // Some parquet/hudi reader paths wrap array<string> elements as one-level nested array.
            // Compat: flatten single-level wrappers for Doris string-like array elements.
            if (item != null && itemType.isStringType()) {
                if (item instanceof ArrayWritable) {
                    Writable[] nested = ((ArrayWritable) item).get();
                    item = nested.length > 0 ? nested[0] : null;
                } else if (item instanceof List) {
                    List<?> nested = (List<?>) item;
                    item = nested.isEmpty() ? null : nested.get(0);
                }
            }
            HadoopHudiColumnValue childValue = new HadoopHudiColumnValue(zoneId);
            childValue.setRow(item);
            childValue.setField(itemType, itemInspector);
            values.add(childValue);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapObjectInspector inspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyObjectInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueObjectInspector = inspector.getMapValueObjectInspector();
        for (Map.Entry kv : inspector.getMap(fieldData).entrySet()) {
            HadoopHudiColumnValue key = new HadoopHudiColumnValue(zoneId);
            key.setRow(kv.getKey());
            key.setField(dorisType.getChildTypes().get(0), keyObjectInspector);
            keys.add(key);

            HadoopHudiColumnValue value = new HadoopHudiColumnValue(zoneId);
            value.setRow(kv.getValue());
            value.setField(dorisType.getChildTypes().get(1), valueObjectInspector);
            values.add(value);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructObjectInspector inspector = (StructObjectInspector) fieldInspector;
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            HadoopHudiColumnValue value = new HadoopHudiColumnValue(zoneId);
            Object obj = null;
            if (idx != null) {
                StructField sf = fields.get(idx);
                obj = inspector.getStructFieldData(fieldData, sf);
            }
            value.setRow(obj);
            value.setField(dorisType.getChildTypes().get(i), fields.get(i).getFieldObjectInspector());
            values.add(value);
        }
    }
}
