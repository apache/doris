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

package org.apache.doris.jni.vec;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Column value in vector column
 */
public interface ColumnValue {
    public boolean getBoolean();

    // tinyint
    public byte getByte();

    // smallint
    public short getShort();

    public int getInt();

    public float getFloat();

    // bigint
    public long getLong();

    public double getDouble();

    public BigDecimal getDecimal();

    public String getString();

    public LocalDate getDate();

    public LocalDateTime getDateTime();

    public byte[] getBytes();

    public void unpackArray(List<ColumnValue> values);

    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values);

    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values);
}
