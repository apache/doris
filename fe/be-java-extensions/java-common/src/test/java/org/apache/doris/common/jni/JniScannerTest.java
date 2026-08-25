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


import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.utils.TypeNativeBytes;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorTable;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JniScannerTest {
    @Test
    public void testOnlyJavaTimeLocalDateTimeSupportsTimestampNsUdf() {
        Assert.assertTrue(JavaUdfDataType.getCandidateTypes(LocalDateTime.class)
                .contains(JavaUdfDataType.TIMESTAMP_NS));
        Assert.assertFalse(JavaUdfDataType.getCandidateTypes(org.joda.time.LocalDateTime.class)
                .contains(JavaUdfDataType.TIMESTAMP_NS));
        Assert.assertFalse(JavaUdfDataType.getCandidateTypes(org.joda.time.DateTime.class)
                .contains(JavaUdfDataType.TIMESTAMP_NS));
        Assert.assertTrue(JavaUdfDataType.getCandidateTypes(org.joda.time.LocalDateTime.class)
                .contains(JavaUdfDataType.DATETIMEV2));
    }

    @Test
    public void testTimestampNsVectorTableRoundTripIncludingNestedTypes() {
        OffHeap.setTesting();
        LocalDateTime[] values = {
                LocalDateTime.of(1677, 9, 21, 0, 12, 43, 145224192),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999),
                LocalDateTime.of(1970, 1, 1, 0, 0),
                LocalDateTime.of(2024, 2, 29, 12, 34, 56, 123456789),
                LocalDateTime.of(2024, 2, 29, 12, 34, 56),
                LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854775807),
                null
        };
        Assert.assertEquals(Long.MIN_VALUE, TypeNativeBytes.convertToTimestampNs(values[0]));
        Assert.assertEquals(-1, TypeNativeBytes.convertToTimestampNs(values[1]));
        Assert.assertEquals(0, TypeNativeBytes.convertToTimestampNs(values[2]));
        Assert.assertEquals(Long.MAX_VALUE, TypeNativeBytes.convertToTimestampNs(values[5]));

        ColumnType[] types = {
                ColumnType.parseType("ts", "timestamp_ns"),
                ColumnType.parseType("items", "array<timestamp_ns>"),
                ColumnType.parseType("by_name", "map<string,timestamp_ns>"),
                ColumnType.parseType("record", "struct<ts:timestamp_ns,items:array<timestamp_ns>>")
        };
        Assert.assertEquals(ColumnType.Type.TIMESTAMP_NS, types[0].getType());
        Assert.assertEquals(ColumnType.Type.TIMESTAMP_NS,
                types[1].getChildTypes().get(0).getType());
        Assert.assertEquals(ColumnType.Type.TIMESTAMP_NS,
                types[2].getChildTypes().get(1).getType());
        Assert.assertEquals(ColumnType.Type.TIMESTAMP_NS,
                types[3].getChildTypes().get(0).getType());

        @SuppressWarnings("unchecked")
        List<Object>[] arrays = (List<Object>[]) new List<?>[values.length];
        @SuppressWarnings("unchecked")
        Map<Object, Object>[] maps = (Map<Object, Object>[]) new Map<?, ?>[values.length];
        @SuppressWarnings("unchecked")
        Map<String, Object>[] structs = (Map<String, Object>[]) new Map<?, ?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            arrays[i] = new ArrayList<>();
            arrays[i].add(values[i]);
            maps[i] = new HashMap<>();
            maps[i].put("value", values[i]);
            structs[i] = new HashMap<>();
            structs[i].put("ts", values[i]);
            structs[i].put("items", arrays[i]);
        }

        VectorTable writable = VectorTable.createWritableTable(
                types, new String[] {"ts", "items", "by_name", "record"}, values.length);
        try {
            writable.appendData(0, values, true);
            writable.appendData(1, arrays, true);
            writable.appendData(2, maps, true);
            writable.appendData(3, structs, true);
            long scalarData = writable.getColumn(0).dataAddress();
            Assert.assertEquals(Long.MIN_VALUE, OffHeap.getLong(null, scalarData));
            Assert.assertEquals(-1, OffHeap.getLong(null, scalarData + Long.BYTES));
            Assert.assertEquals(0, OffHeap.getLong(null, scalarData + 2L * Long.BYTES));
            Assert.assertEquals(Long.MAX_VALUE, OffHeap.getLong(null, scalarData + 5L * Long.BYTES));
            Object[][] restored = writable.getMaterializedData();
            Assert.assertArrayEquals(values, restored[0]);
            for (int i = 0; i < values.length; ++i) {
                Assert.assertEquals(arrays[i], restored[1][i]);
                Assert.assertEquals(maps[i], restored[2][i]);
                Assert.assertEquals(structs[i], restored[3][i]);
            }
        } finally {
            writable.close();
        }
    }

    @Test
    public void testUnencodedStructFieldNamesRemainLowerCase() {
        ColumnType structType = ColumnType.parseType(
                "value", "struct<Mixed:int,UPPER:struct<Nested:string>>");

        Assert.assertEquals(Arrays.asList("mixed", "upper"), structType.getChildNames());
        Assert.assertEquals(Arrays.asList("nested"),
                structType.getChildTypes().get(1).getChildNames());
    }

    @Test
    public void testMockJniScanner() throws IOException {
        OffHeap.setTesting();
        MockJniScanner scanner = new MockJniScanner(32, new HashMap<String, String>() {
            {
                put("mock_rows", "128");
                put("required_fields", "boolean,tinyint,smallint,int,bigint,largeint,float,double,"
                        + "date,timestamp,char,varchar,string,decimalv2,decimal64,array,map,struct,"
                        + "decimal18,timestamp4,datev1,datev2,datetimev1,datetimev2");
                put("columns_types", "boolean#tinyint#smallint#int#bigint#largeint#float#double#"
                        + "date#timestamp#char(10)#varchar(10)#string#decimalv2(12,4)#decimal64(10,3)#"
                        + "array<array<string>>#map<string,array<int>>#struct<col1:timestamp(6),col2:array<char(10)>>#"
                        + "decimal(18,5)#timestamp(4)#datev1#datev2#datetimev1#datetimev2(4)");
            }
        });
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);
                Assert.assertEquals(32, rows);

                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
                        scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows).substring(0, 128));
                // Restored table is release by the origin table.
            }
            scanner.resetTable();
        } while (metaAddress != 0);
        scanner.releaseTable();
        scanner.close();
    }

    @Test
    public void testSetBatchSize() throws IOException {
        OffHeap.setTesting();
        MockJniScanner scanner = new MockJniScanner(16, new HashMap<String, String>() {
            {
                put("mock_rows", "64");
                put("required_fields", "int");
                put("columns_types", "int");
            }
        });
        scanner.open();

        // First batch: batchSize = 16
        long metaAddress = scanner.getNextBatchMeta();
        Assert.assertNotEquals(0, metaAddress);
        Assert.assertEquals(16, OffHeap.getLong(null, metaAddress));
        scanner.resetTable();

        // Change batch size to 32
        scanner.setBatchSize(32);
        Assert.assertEquals(32, scanner.getBatchSize());

        // Second batch: should read 32 rows with updated batchSize
        metaAddress = scanner.getNextBatchMeta();
        Assert.assertNotEquals(0, metaAddress);
        Assert.assertEquals(32, OffHeap.getLong(null, metaAddress));
        scanner.resetTable();

        // Third batch: only 16 rows remaining
        metaAddress = scanner.getNextBatchMeta();
        Assert.assertNotEquals(0, metaAddress);
        Assert.assertEquals(16, OffHeap.getLong(null, metaAddress));
        scanner.resetTable();

        // EOF
        metaAddress = scanner.getNextBatchMeta();
        Assert.assertEquals(0, metaAddress);

        scanner.releaseTable();
        scanner.close();
    }
}
