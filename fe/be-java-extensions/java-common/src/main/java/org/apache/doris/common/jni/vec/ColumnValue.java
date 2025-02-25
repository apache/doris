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

package org.apache.doris.common.jni.vec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Column value in vector column
 */
public interface ColumnValue {
    // Get bytes directly when reading string value to avoid decoding&encoding
    boolean canGetStringAsBytes();

    boolean isNull();

    boolean getBoolean();

    // tinyint
    byte getByte();

    // smallint
    short getShort();

    int getInt();

    float getFloat();

    // bigint
    long getLong();

    double getDouble();

    BigInteger getBigInteger();

    BigDecimal getDecimal();

    String getString();

    byte[] getStringAsBytes();

    LocalDate getDate();

    default String getChar() {
        return getString();
    }

    default byte[] getCharAsBytes() {
        return getStringAsBytes();
    }

    default boolean canGetCharAsBytes() {
        return canGetStringAsBytes();
    }

    LocalDateTime getDateTime();

    byte[] getBytes();

    void unpackArray(List<ColumnValue> values);

    void unpackMap(List<ColumnValue> keys, List<ColumnValue> values);

    void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values);
}
