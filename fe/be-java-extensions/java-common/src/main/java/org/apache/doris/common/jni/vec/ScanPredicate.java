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


import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.utils.TypeNativeBytes;
import org.apache.doris.common.jni.vec.ColumnType.Type;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reference to doris::JniConnector::ScanPredicate
 */
public class ScanPredicate {
    public enum FilterOp {
        FILTER_LARGER(">"),
        FILTER_LARGER_OR_EQUAL(">="),
        FILTER_LESS("<"),
        FILTER_LESS_OR_EQUAL("<="),
        FILTER_IN("in"),
        FILTER_NOT_IN("not in");

        public final String op;

        FilterOp(String op) {
            this.op = op;
        }
    }

    private static FilterOp parseFilterOp(int op) {
        switch (op) {
            case 0:
                return FilterOp.FILTER_LARGER;
            case 1:
                return FilterOp.FILTER_LARGER_OR_EQUAL;
            case 2:
                return FilterOp.FILTER_LESS;
            case 3:
                return FilterOp.FILTER_LESS_OR_EQUAL;
            case 4:
                return FilterOp.FILTER_IN;
            default:
                return FilterOp.FILTER_NOT_IN;
        }
    }

    public static class PredicateValue implements ColumnValue {
        private final byte[] valueBytes;
        private final ColumnType.Type type;
        private final int scale;

        public PredicateValue(byte[] valueBytes, ColumnType.Type type, int scale) {
            this.valueBytes = valueBytes;
            this.type = type;
            this.scale = scale;
        }

        private Object inspectObject() {
            ByteBuffer byteBuffer = ByteBuffer.wrap(
                    TypeNativeBytes.convertByteOrder(Arrays.copyOf(valueBytes, valueBytes.length)));
            switch (type) {
                case BOOLEAN:
                    return byteBuffer.get() == 1;
                case TINYINT:
                    return byteBuffer.get();
                case SMALLINT:
                    return byteBuffer.getShort();
                case INT:
                    return byteBuffer.getInt();
                case BIGINT:
                    return byteBuffer.getLong();
                case LARGEINT:
                    return TypeNativeBytes.getBigInteger(Arrays.copyOf(valueBytes, valueBytes.length));
                case FLOAT:
                    return byteBuffer.getFloat();
                case DOUBLE:
                    return byteBuffer.getDouble();
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return TypeNativeBytes.getDecimal(Arrays.copyOf(valueBytes, valueBytes.length), scale);
                case CHAR:
                case VARCHAR:
                case STRING:
                    return new String(valueBytes, StandardCharsets.UTF_8);
                case BINARY:
                    return valueBytes;
                default:
                    return new Object();
            }
        }

        @Override
        public boolean canGetStringAsBytes() {
            return false;
        }

        @Override
        public String toString() {
            return inspectObject().toString();
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
            return (BigInteger) inspectObject();
        }

        @Override
        public BigDecimal getDecimal() {
            return (BigDecimal) inspectObject();
        }

        @Override
        public String getString() {
            return toString();
        }

        @Override
        public byte[] getStringAsBytes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalDate getDate() {
            return LocalDate.now();
        }

        @Override
        public LocalDateTime getDateTime() {
            return LocalDateTime.now();
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

    private final long bytesLength;
    public final String columName;
    public final ColumnType.Type type;
    public final FilterOp op;
    private final byte[][] values;
    public final int scale;

    private ScanPredicate(long predicateAddress, Map<String, ColumnType.Type> nameToType) {
        long address = predicateAddress;
        int length = OffHeap.getInt(null, address);
        address += 4;
        byte[] nameBytes = new byte[length];
        OffHeap.copyMemory(null, address, nameBytes, OffHeap.BYTE_ARRAY_OFFSET, length);
        columName = new String(nameBytes, StandardCharsets.UTF_8);
        type = nameToType.getOrDefault(columName, Type.UNSUPPORTED);
        address += length;
        op = parseFilterOp(OffHeap.getInt(null, address));
        address += 4;
        scale = OffHeap.getInt(null, address);
        address += 4;
        int numValues = OffHeap.getInt(null, address);
        address += 4;
        values = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
            int valueLength = OffHeap.getInt(null, address);
            address += 4;
            byte[] valueBytes = new byte[valueLength];
            OffHeap.copyMemory(null, address, valueBytes, OffHeap.BYTE_ARRAY_OFFSET, valueLength);
            address += valueLength;
            values[i] = valueBytes;
        }
        bytesLength = address - predicateAddress;
    }

    public PredicateValue[] predicateValues() {
        PredicateValue[] result = new PredicateValue[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = new PredicateValue(values[i], type, scale);
        }
        return result;
    }

    public static ScanPredicate[] parseScanPredicates(long predicatesAddress, ColumnType[] types) {
        Map<String, ColumnType.Type> nameToType = new HashMap<>();
        for (ColumnType columnType : types) {
            nameToType.put(columnType.getName(), columnType.getType());
        }
        int numPredicates = OffHeap.getInt(null, predicatesAddress);
        long nextPredicateAddress = predicatesAddress + 4;
        ScanPredicate[] predicates = new ScanPredicate[numPredicates];
        for (int i = 0; i < numPredicates; i++) {
            predicates[i] = new ScanPredicate(nextPredicateAddress, nameToType);
            nextPredicateAddress += predicates[i].bytesLength;
        }
        return predicates;
    }

    public void dump(StringBuilder sb) {
        sb.append(columName).append(' ').append(op.op).append(' ');
        if (op == FilterOp.FILTER_IN || op == FilterOp.FILTER_NOT_IN) {
            sb.append('(').append(StringUtils.join(predicateValues(), ", ")).append(')');
        } else {
            sb.append(predicateValues()[0]);
        }
    }

    public static String dump(ScanPredicate[] scanPredicates) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < scanPredicates.length; i++) {
            if (i != 0) {
                sb.append(" and ");
            }
            scanPredicates[i].dump(sb);
        }
        return sb.toString();
    }
}
