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


import org.apache.doris.common.jni.vec.ColumnValue;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.LongWritable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class HudiColumnValue implements ColumnValue {
    public enum TimeUnit {
        MILLIS, MICROS
    }

    private final Object fieldData;
    private final ObjectInspector fieldInspector;
    private final TimeUnit timeUnit;

    HudiColumnValue(ObjectInspector fieldInspector, Object fieldData) {
        this(fieldInspector, fieldData, TimeUnit.MICROS);
    }

    HudiColumnValue(ObjectInspector fieldInspector, Object fieldData, int timePrecision) {
        this(fieldInspector, fieldData, timePrecision == 3 ? TimeUnit.MILLIS : TimeUnit.MICROS);
    }

    HudiColumnValue(ObjectInspector fieldInspector, Object fieldData, TimeUnit timeUnit) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
        this.timeUnit = timeUnit;
    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
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
        return 0;
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
        throw new UnsupportedOperationException("Hudi type does not support largeint");
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimalObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData).bigDecimalValue();
    }

    @Override
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public LocalDate getDate() {
        return ((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData).toLocalDate();
    }

    @Override
    public LocalDateTime getDateTime() {
        if (fieldData instanceof LongWritable) {
            long datetime = ((LongWritable) fieldData).get();
            long seconds;
            long nanoseconds;
            if (timeUnit == TimeUnit.MILLIS) {
                seconds = datetime / 1000;
                nanoseconds = (datetime % 1000) * 1000000;
            } else {
                seconds = datetime / 1000000;
                nanoseconds = (datetime % 1000000) * 1000;
            }
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoseconds), ZoneId.systemDefault());
        } else {
            return ((TimestampObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData).toLocalDateTime();
        }
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {

    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {

    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

    }

}
