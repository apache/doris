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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
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

    private static void closeAllocator(PaimonJniWriter writer) throws Exception {
        Field field = PaimonJniWriter.class.getDeclaredField("allocator");
        field.setAccessible(true);
        ((AutoCloseable) field.get(writer)).close();
    }
}
