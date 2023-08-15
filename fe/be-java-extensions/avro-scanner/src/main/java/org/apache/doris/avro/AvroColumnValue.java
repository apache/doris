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

package org.apache.doris.avro;

import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map.Entry;

public class AvroColumnValue implements ColumnValue {

    private final Object fieldData;
    private final ObjectInspector fieldInspector;

    public AvroColumnValue(ObjectInspector fieldInspector, Object fieldData) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
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
        return (byte) inspectObject();
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
        return (BigDecimal) inspectObject();
    }

    @Override
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public byte[] getStringAsBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate getDate() {
        // avro has no date type
        return null;
    }

    @Override
    public LocalDateTime getDateTime() {
        // avro has no dateTime type
        return null;
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
            AvroColumnValue avroColumnValue = null;
            if (item != null) {
                avroColumnValue = new AvroColumnValue(itemInspector, item);
            }
            values.add(avroColumnValue);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapObjectInspector inspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyObjectInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueObjectInspector = inspector.getMapValueObjectInspector();
        for (Entry<?, ?> kv : inspector.getMap(fieldData).entrySet()) {
            AvroColumnValue avroKey = null;
            AvroColumnValue avroValue = null;
            if (kv.getKey() != null) {
                avroKey = new AvroColumnValue(keyObjectInspector, kv.getKey());
            }
            if (kv.getValue() != null) {
                avroValue = new AvroColumnValue(valueObjectInspector, kv.getValue());
            }
            keys.add(avroKey);
            values.add(avroValue);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

    }
}
