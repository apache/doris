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

package org.apache.doris.common.jni;


import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.jni.vec.ScanPredicate;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * The demo usage of JniScanner. This class will only be retained during the functional testing phase to
 * verify that the communication and data exchange with the BE are correct.
 */
public class MockJniScanner extends JniScanner {
    public static class MockColumnValue implements ColumnValue {
        private int i;
        private int j;

        public MockColumnValue(int i, int j) {
            set(i, j);
        }

        public MockColumnValue() {
        }

        public void set(int i, int j) {
            this.i = i;
            this.j = j;
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
            return (i + j) % 2 == 0;
        }

        @Override
        public byte getByte() {
            return (byte) (i + j);
        }

        @Override
        public short getShort() {
            return (short) (i - j);
        }

        @Override
        public int getInt() {
            return i + j;
        }

        @Override
        public float getFloat() {
            return (float) (j + i - 11) / (i + 1);
        }

        @Override
        public long getLong() {
            return (long) (i - 13) * (j + 1);
        }

        @Override
        public double getDouble() {
            return (double) (j + i - 15) / (i + 1);
        }

        @Override
        public BigInteger getBigInteger() {
            return BigInteger.valueOf(getLong());
        }

        @Override
        public BigDecimal getDecimal() {
            return BigDecimal.valueOf(getDouble());
        }

        @Override
        public String getString() {
            return "row-" + i + "-column-" + j;
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
            return ("row-" + i + "-column-" + j).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void unpackArray(List<ColumnValue> values) {
            for (int m = 1; m < i; ++m) {
                if (m % 3 == 0) {
                    values.add(null);
                } else {
                    values.add(new MockColumnValue(i, j + m));
                }
            }
        }

        @Override
        public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
            for (int m = 0; m < i; ++m) {
                values.add(new MockColumnValue(i + m, j));
            }
            for (int m = 0; m < i; ++m) {
                if (m % 3 == 0) {
                    values.add(null);
                } else {
                    values.add(new MockColumnValue(i, j + m));
                }
            }
        }

        @Override
        public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
            structFieldIndex.clear();
            structFieldIndex.add(0);
            structFieldIndex.add(1);
            if ((i + j) % 4 == 0) {
                values.add(null);
            } else {
                values.add(new MockColumnValue(i, j));
            }
            values.add(new MockColumnValue(i, j + 3));
        }
    }

    private static final Logger LOG = Logger.getLogger(MockJniScanner.class);

    private int mockRows;
    private int readRows = 0;
    private final MockColumnValue columnValue = new MockColumnValue();

    public MockJniScanner(int batchSize, Map<String, String> params) {
        mockRows = Integer.parseInt(params.get("mock_rows"));
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        ScanPredicate[] predicates = new ScanPredicate[0];
        if (params.containsKey("push_down_predicates")) {
            long predicatesAddress = Long.parseLong(params.get("push_down_predicates"));
            if (predicatesAddress != 0) {
                predicates = ScanPredicate.parseScanPredicates(predicatesAddress, columnTypes);
                LOG.info("MockJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
            }
        }
        initTableInfo(columnTypes, requiredFields, predicates, batchSize);
    }

    @Override
    public void open() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    protected int getNext() throws IOException {
        if (readRows == mockRows) {
            return 0;
        }
        int rows = Math.min(batchSize, mockRows - readRows);
        for (int i = 0; i < rows; ++i) {
            for (int j = 0; j < types.length; ++j) {
                if ((i + j) % 16 == 0) {
                    appendData(j, null);
                } else {
                    columnValue.set(i, j);
                    appendData(j, columnValue);
                }
            }
        }
        readRows += rows;
        return rows;
    }
}
