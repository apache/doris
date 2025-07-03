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
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.LongWritable;

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

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
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
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
    }


    @Override
    public byte getByte() {
        throw new UnsupportedOperationException("Hoodie type does not support tinyint");
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimal) inspectObject()).bigDecimalValue();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay((((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData))
                .toEpochDay());
    }

    @Override
    public LocalDateTime getDateTime() {
        if (fieldData instanceof Timestamp) {
            return ((Timestamp) fieldData).toLocalDateTime();
        } else if (fieldData instanceof TimestampWritableV2) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond((((TimestampObjectInspector) fieldInspector)
                    .getPrimitiveJavaObject(fieldData)).toEpochSecond()), zoneId);
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
        return false;
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
        throw new UnsupportedOperationException("Hoodie type does not support getStringAsBytes");
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ListObjectInspector inspector = (ListObjectInspector) fieldInspector;
        List<?> items = inspector.getList(fieldData);
        ObjectInspector itemInspector = inspector.getListElementObjectInspector();
        for (int i = 0; i < items.size(); i++) {
            Object item = items.get(i);
            HadoopHudiColumnValue childValue = new HadoopHudiColumnValue(zoneId);
            childValue.setRow(item);
            childValue.setField(dorisType.getChildTypes().get(0), itemInspector);
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
