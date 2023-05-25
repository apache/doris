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

import org.apache.doris.jni.vec.ColumnValue;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class HudiColumnValue implements ColumnValue {
    private final Object fieldData;
    private final ObjectInspector fieldInspector;

    HudiColumnValue(ObjectInspector fieldInspector, Object fieldData) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
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
    public BigDecimal getDecimal() {
        return null;
    }

    @Override
    public String getString() {
        return inspectObject().toString();
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

    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {

    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

    }

}
