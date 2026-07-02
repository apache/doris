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

package org.apache.doris.paimon;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonJniWriterTest {
    @Test
    public void testFullRowWriteRequiresSameFieldOrder() {
        RowType tableType = RowType.of(new DataType[] {
                DataTypes.INT(), DataTypes.STRING()}, new String[] {"k", "v"});
        RowType sameOrder = RowType.of(new DataType[] {
                DataTypes.INT(), DataTypes.STRING()}, new String[] {"k", "v"});
        RowType differentOrder = RowType.of(new DataType[] {
                DataTypes.STRING(), DataTypes.INT()}, new String[] {"v", "k"});

        Assert.assertTrue(PaimonJniWriter.isFullRowWrite(tableType, sameOrder));
        Assert.assertFalse(PaimonJniWriter.isFullRowWrite(tableType, differentOrder));
    }

    @Test
    public void testFullRowWriteRejectsMissingColumns() {
        RowType tableType = RowType.of(new DataType[] {
                DataTypes.INT(), DataTypes.STRING()}, new String[] {"k", "v"});
        RowType partialWrite = RowType.of(new DataType[] {
                DataTypes.INT()}, new String[] {"k"});

        Assert.assertFalse(PaimonJniWriter.isFullRowWrite(tableType, partialWrite));
    }

    @Test
    public void testBuildWriteTypeUsesRequestedColumnOrder() throws Exception {
        RowType tableType = RowType.of(new DataType[] {
                DataTypes.INT(), DataTypes.STRING()}, new String[] {"k", "v"});
        PaimonJniWriter writer = writerWithSchema(tableType);

        RowType writeType = invokeBuildWriteType(writer, new String[] {"v", "k"});

        Assert.assertEquals("[v, k]", writeType.getFieldNames().toString());
        Assert.assertEquals(tableType.getFields().get(1).type(), writeType.getFields().get(0).type());
        Assert.assertEquals(tableType.getFields().get(0).type(), writeType.getFields().get(1).type());
        closeAllocator(writer);
    }

    @Test
    public void testResolveTargetTypesUsesArrowFieldNamesWithoutExplicitTargetTypes() throws Exception {
        RowType tableType = RowType.of(new DataType[] {
                DataTypes.INT(), DataTypes.STRING()}, new String[] {"k", "v"});
        PaimonJniWriter writer = writerWithSchema(tableType);

        DataType[] targetTypes = invokeResolveTargetTypes(writer, Arrays.asList(
                arrowField("v", new ArrowType.Utf8()),
                arrowField("k", new ArrowType.Int(32, true))));

        Assert.assertEquals(tableType.getFields().get(1).type(), targetTypes[0]);
        Assert.assertEquals(tableType.getFields().get(0).type(), targetTypes[1]);
        closeAllocator(writer);
    }

    @Test
    public void testBuildWriteTypeRequiresExplicitColumns() throws Exception {
        RowType tableType = RowType.of(new DataType[] {
                DataTypes.INT()}, new String[] {"k"});
        PaimonJniWriter writer = writerWithSchema(tableType);

        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> invokeBuildWriteType(writer, new String[0]));

        Assert.assertTrue(exception.getMessage().contains("explicit column names"));
        closeAllocator(writer);
    }

    @Test
    public void testBuildWriteTypeRejectsUnknownColumn() throws Exception {
        RowType tableType = RowType.of(new DataType[] {
                DataTypes.INT()}, new String[] {"k"});
        PaimonJniWriter writer = writerWithSchema(tableType);

        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> invokeBuildWriteType(writer, new String[] {"missing"}));

        Assert.assertTrue(exception.getMessage().contains("Cannot find Paimon column 'missing'"));
        closeAllocator(writer);
    }

    @Test
    public void testInitBucketWriterRejectsUnsupportedBucketModes() throws Exception {
        for (BucketMode bucketMode : Arrays.asList(BucketMode.HASH_DYNAMIC, BucketMode.KEY_DYNAMIC,
                BucketMode.POSTPONE_MODE)) {
            PaimonJniWriter writer = new PaimonJniWriter();
            try {
                UnsupportedOperationException exception = Assert.assertThrows(UnsupportedOperationException.class,
                        () -> invokeInitBucketWriter(writer, fileStoreTableWithBucketMode(bucketMode)));

                Assert.assertTrue(exception.getMessage().contains("Unsupported Paimon bucket mode"));
            } finally {
                closeAllocator(writer);
            }
        }
    }

    @Test
    public void testInitBucketWriterFixedBucketUsesExplicitBucketWrite() throws Exception {
        PaimonJniWriter writer = new PaimonJniWriter();

        invokeInitBucketWriter(writer, fileStoreTableWithBucketMode(BucketMode.HASH_FIXED));

        Assert.assertEquals(BucketMode.HASH_FIXED, getPrivateField(writer, "bucketMode"));
        Assert.assertEquals(Boolean.TRUE, getPrivateField(writer, "useExplicitBucketWrite"));
        Assert.assertNotNull(getPrivateField(writer, "rowKeyExtractor"));
        closeAllocator(writer);
    }

    @Test
    public void testConvertToPaimonPrimitiveTypes() throws Exception {
        PaimonJniWriter writer = new PaimonJniWriter();
        LocalDate date = LocalDate.of(2024, 1, 2);
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 6000);
        BigDecimal decimal = new BigDecimal("12.34");

        Assert.assertNull(invokeConvertToPaimonType(writer, null, null, DataTypes.INT()));
        Assert.assertEquals(Boolean.TRUE, invokeConvertToPaimonType(writer, true, null, DataTypes.BOOLEAN()));
        Assert.assertEquals(12, invokeConvertToPaimonType(writer, 12, null, DataTypes.INT()));
        Assert.assertEquals(123L, invokeConvertToPaimonType(writer, 123L, null, DataTypes.BIGINT()));
        Assert.assertEquals(BinaryString.fromString("abc"),
                invokeConvertToPaimonType(writer, "abc", arrowField("s", new ArrowType.Utf8()), DataTypes.STRING()));
        Assert.assertArrayEquals("abc".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                (byte[]) invokeConvertToPaimonType(writer, "abc", arrowField("b", new ArrowType.Binary()),
                        DataTypes.BYTES()));
        Assert.assertEquals((int) date.toEpochDay(), invokeConvertToPaimonType(writer, date,
                arrowField("d", new ArrowType.Date(DateUnit.DAY)), DataTypes.DATE()));
        Assert.assertEquals(timestamp, ((Timestamp) invokeConvertToPaimonType(writer, timestamp,
                arrowField("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                DataTypes.TIMESTAMP(6))).toLocalDateTime());
        Assert.assertEquals(decimal, ((Decimal) invokeConvertToPaimonType(writer, decimal,
                arrowField("dec", new ArrowType.Decimal(10, 2, 128)), DataTypes.DECIMAL(10, 2))).toBigDecimal());
        closeAllocator(writer);
    }

    @Test
    public void testOversizedCommitPayloadReducesMultiMessageChunk() throws Exception {
        byte[] payload = new byte[PaimonJniWriter.MAX_COMMIT_PAYLOAD_BYTES + 1];

        Assert.assertTrue(PaimonJniWriter.shouldReduceCommitChunk(payload, 2));
    }

    @Test
    public void testOversizedSingleCommitPayloadFails() {
        byte[] payload = new byte[PaimonJniWriter.MAX_COMMIT_PAYLOAD_BYTES + 1];

        IOException exception = Assert.assertThrows(IOException.class,
                () -> PaimonJniWriter.shouldReduceCommitChunk(payload, 1));
        Assert.assertTrue(exception.getMessage().contains("exceeds limit"));
    }

    @Test
    public void testCommitPayloadWithinLimitDoesNotReduceChunk() throws Exception {
        byte[] payload = new byte[PaimonJniWriter.MAX_COMMIT_PAYLOAD_BYTES];

        Assert.assertFalse(PaimonJniWriter.shouldReduceCommitChunk(payload, 512));
        Assert.assertFalse(PaimonJniWriter.shouldReduceCommitChunk(payload, 1));
    }

    private static PaimonJniWriter writerWithSchema(RowType tableType) throws Exception {
        PaimonJniWriter writer = new PaimonJniWriter();
        Map<String, org.apache.paimon.types.DataField> fieldMap = tableType.getFields().stream()
                .collect(Collectors.toMap(org.apache.paimon.types.DataField::name,
                        java.util.function.Function.identity(), (left, right) -> left, HashMap::new));
        Field field = PaimonJniWriter.class.getDeclaredField("paimonFieldMap");
        field.setAccessible(true);
        field.set(writer, fieldMap);
        return writer;
    }

    private static RowType invokeBuildWriteType(PaimonJniWriter writer, String[] columnNames) throws Exception {
        Method method = PaimonJniWriter.class.getDeclaredMethod("buildWriteType", String[].class);
        method.setAccessible(true);
        try {
            return (RowType) method.invoke(writer, new Object[] {columnNames});
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static DataType[] invokeResolveTargetTypes(PaimonJniWriter writer,
            List<org.apache.arrow.vector.types.pojo.Field> fields) throws Exception {
        Method method = PaimonJniWriter.class.getDeclaredMethod("resolveTargetTypes", List.class);
        method.setAccessible(true);
        try {
            return (DataType[]) method.invoke(writer, fields);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static void closeAllocator(PaimonJniWriter writer) throws Exception {
        Field field = PaimonJniWriter.class.getDeclaredField("allocator");
        field.setAccessible(true);
        ((AutoCloseable) field.get(writer)).close();
    }

    private static void invokeInitBucketWriter(PaimonJniWriter writer, FileStoreTable fileStoreTable)
            throws Exception {
        Method method = PaimonJniWriter.class.getDeclaredMethod("initBucketWriter", FileStoreTable.class);
        method.setAccessible(true);
        try {
            method.invoke(writer, fileStoreTable);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static Object invokeConvertToPaimonType(PaimonJniWriter writer, Object value,
            org.apache.arrow.vector.types.pojo.Field arrowField, DataType targetType) throws Exception {
        Method method = PaimonJniWriter.class.getDeclaredMethod("convertToPaimonType",
                Object.class, org.apache.arrow.vector.types.pojo.Field.class, DataType.class);
        method.setAccessible(true);
        try {
            return method.invoke(writer, value, arrowField, targetType);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static Object getPrivateField(PaimonJniWriter writer, String name) throws Exception {
        Field field = PaimonJniWriter.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(writer);
    }

    private static org.apache.arrow.vector.types.pojo.Field arrowField(String name, ArrowType arrowType) {
        return new org.apache.arrow.vector.types.pojo.Field(name, FieldType.nullable(arrowType), null);
    }

    private static FileStoreTable fileStoreTableWithBucketMode(BucketMode bucketMode) {
        InvocationHandler handler = (proxy, method, args) -> {
            String methodName = method.getName();
            if ("bucketMode".equals(methodName)) {
                return bucketMode;
            }
            if ("createRowKeyExtractor".equals(methodName)) {
                return new TestRowKeyExtractor();
            }
            if ("toString".equals(methodName)) {
                return "test-file-store-table";
            }
            if ("hashCode".equals(methodName)) {
                return System.identityHashCode(proxy);
            }
            if ("equals".equals(methodName)) {
                return proxy == args[0];
            }
            Class<?> returnType = method.getReturnType();
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == int.class) {
                return 0;
            }
            return null;
        };
        return (FileStoreTable) Proxy.newProxyInstance(
                FileStoreTable.class.getClassLoader(), new Class[] {FileStoreTable.class}, handler);
    }

    private static class TestRowKeyExtractor extends RowKeyExtractor {
        TestRowKeyExtractor() {
            super(new TableSchema(1, Collections.emptyList(), 0, Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyMap(), null));
        }

        @Override
        public int bucket() {
            return 0;
        }
    }
}
