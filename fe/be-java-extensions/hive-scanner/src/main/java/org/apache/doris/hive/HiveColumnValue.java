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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

public class HiveColumnValue implements ColumnValue {

    private static final Logger LOG = LogManager.getLogger(HiveColumnValue.class);
    private final Object fieldData;
    private final ObjectInspector fieldInspector;

    HiveColumnValue(ObjectInspector fieldInspector, Object fieldData) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
    }

    private Object inspectObject() {
        if (fieldData instanceof ByteBuffer) {
            return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(((ByteBuffer) fieldData).array());
        }
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
        Object value = inspectObject();
        if (value instanceof Integer) {
            return ((Integer) value).shortValue();
        } else {
            return (short) value;
        }
    }

    @Override
    public int getInt() {
        Object value = inspectObject();
        if (value instanceof Integer) {
            return (Integer) value;
        } else {
            return (int) value;
        }
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
        return null;
    }

    @Override
    public LocalDateTime getDateTime() {
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
            HiveColumnValue cv = null;
            if (item != null) {
                cv = new HiveColumnValue(itemInspector, item);
            }
            values.add(cv);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapObjectInspector inspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyObjectInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueObjectInspector = inspector.getMapValueObjectInspector();
        for (Entry<?, ?> kv : inspector.getMap(fieldData).entrySet()) {
            HiveColumnValue cv0 = null;
            HiveColumnValue cv1 = null;
            if (kv.getKey() != null) {
                cv0 = new HiveColumnValue(keyObjectInspector, kv.getKey());
            }
            if (kv.getValue() != null) {
                cv1 = new HiveColumnValue(valueObjectInspector, kv.getValue());
            }
            keys.add(cv0);
            values.add(cv1);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructObjectInspector inspector = (StructObjectInspector) fieldInspector;
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            HiveColumnValue cv = null;
            if (idx != null) {
                StructField sf = fields.get(idx);
                Object o = inspector.getStructFieldData(fieldData, sf);
                if (o != null) {
                    cv = new HiveColumnValue(sf.getFieldObjectInspector(), o);
                }
                if (Objects.nonNull(o)) {
                    cv = new HiveColumnValue(sf.getFieldObjectInspector(), o);
                }
            }
            values.add(cv);
        }
    }
}
