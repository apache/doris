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

package org.apache.doris.spark.serialization;

import static org.hamcrest.core.StringStartsWith.startsWith;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.doris.spark.rest.RestService;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.thrift.TScanBatchResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.spark.sql.types.Decimal;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class TestRowBatch {
    private final static Logger logger = LoggerFactory.getLogger(TestRowBatch.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRowBatch() throws Exception {
        // schema
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k0", FieldType.nullable(new ArrowType.Bool()), null));
        childrenBuilder.add(new Field("k1", FieldType.nullable(new ArrowType.Int(8, true)), null));
        childrenBuilder.add(new Field("k2", FieldType.nullable(new ArrowType.Int(16, true)), null));
        childrenBuilder.add(new Field("k3", FieldType.nullable(new ArrowType.Int(32, true)), null));
        childrenBuilder.add(new Field("k4", FieldType.nullable(new ArrowType.Int(64, true)), null));
        childrenBuilder.add(new Field("k9", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
        childrenBuilder.add(new Field("k8", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        childrenBuilder.add(new Field("k10", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k11", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k5", FieldType.nullable(new ArrowType.Utf8()), null));
        childrenBuilder.add(new Field("k6", FieldType.nullable(new ArrowType.Utf8()), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k0");
        BitVector bitVector = (BitVector)vector;
        bitVector.setInitialCapacity(3);
        bitVector.allocateNew(3);
        bitVector.setSafe(0, 1);
        bitVector.setSafe(1, 0);
        bitVector.setSafe(2, 1);
        vector.setValueCount(3);

        vector = root.getVector("k1");
        TinyIntVector tinyIntVector = (TinyIntVector)vector;
        tinyIntVector.setInitialCapacity(3);
        tinyIntVector.allocateNew(3);
        tinyIntVector.setSafe(0, 1);
        tinyIntVector.setSafe(1, 2);
        tinyIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k2");
        SmallIntVector smallIntVector = (SmallIntVector)vector;
        smallIntVector.setInitialCapacity(3);
        smallIntVector.allocateNew(3);
        smallIntVector.setSafe(0, 1);
        smallIntVector.setSafe(1, 2);
        smallIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k3");
        IntVector intVector = (IntVector)vector;
        intVector.setInitialCapacity(3);
        intVector.allocateNew(3);
        intVector.setSafe(0, 1);
        intVector.setNull(1);
        intVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k4");
        BigIntVector bigIntVector = (BigIntVector)vector;
        bigIntVector.setInitialCapacity(3);
        bigIntVector.allocateNew(3);
        bigIntVector.setSafe(0, 1);
        bigIntVector.setSafe(1, 2);
        bigIntVector.setSafe(2, 3);
        vector.setValueCount(3);

        vector = root.getVector("k5");
        VarCharVector varCharVector = (VarCharVector)vector;
        varCharVector.setInitialCapacity(3);
        varCharVector.allocateNew();
        varCharVector.setIndexDefined(0);
        varCharVector.setValueLengthSafe(0, 5);
        varCharVector.setSafe(0, "12.34".getBytes());
        varCharVector.setIndexDefined(1);
        varCharVector.setValueLengthSafe(1, 5);
        varCharVector.setSafe(1, "88.88".getBytes());
        varCharVector.setIndexDefined(2);
        varCharVector.setValueLengthSafe(2, 2);
        varCharVector.setSafe(2, "10".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k6");
        VarCharVector charVector = (VarCharVector)vector;
        charVector.setInitialCapacity(3);
        charVector.allocateNew();
        charVector.setIndexDefined(0);
        charVector.setValueLengthSafe(0, 5);
        charVector.setSafe(0, "char1".getBytes());
        charVector.setIndexDefined(1);
        charVector.setValueLengthSafe(1, 5);
        charVector.setSafe(1, "char2".getBytes());
        charVector.setIndexDefined(2);
        charVector.setValueLengthSafe(2, 5);
        charVector.setSafe(2, "char3".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k8");
        Float8Vector float8Vector = (Float8Vector)vector;
        float8Vector.setInitialCapacity(3);
        float8Vector.allocateNew(3);
        float8Vector.setSafe(0, 1.1);
        float8Vector.setSafe(1, 2.2);
        float8Vector.setSafe(2, 3.3);
        vector.setValueCount(3);

        vector = root.getVector("k9");
        Float4Vector float4Vector = (Float4Vector)vector;
        float4Vector.setInitialCapacity(3);
        float4Vector.allocateNew(3);
        float4Vector.setSafe(0, 1.1f);
        float4Vector.setSafe(1, 2.2f);
        float4Vector.setSafe(2, 3.3f);
        vector.setValueCount(3);

        vector = root.getVector("k10");
        VarCharVector datecharVector = (VarCharVector)vector;
        datecharVector.setInitialCapacity(3);
        datecharVector.allocateNew();
        datecharVector.setIndexDefined(0);
        datecharVector.setValueLengthSafe(0, 5);
        datecharVector.setSafe(0, "2008-08-08".getBytes());
        datecharVector.setIndexDefined(1);
        datecharVector.setValueLengthSafe(1, 5);
        datecharVector.setSafe(1, "1900-08-08".getBytes());
        datecharVector.setIndexDefined(2);
        datecharVector.setValueLengthSafe(2, 5);
        datecharVector.setSafe(2, "2100-08-08".getBytes());
        vector.setValueCount(3);

        vector = root.getVector("k11");
        VarCharVector timecharVector = (VarCharVector)vector;
        timecharVector.setInitialCapacity(3);
        timecharVector.allocateNew();
        timecharVector.setIndexDefined(0);
        timecharVector.setValueLengthSafe(0, 5);
        timecharVector.setSafe(0, "2008-08-08 00:00:00".getBytes());
        timecharVector.setIndexDefined(1);
        timecharVector.setValueLengthSafe(1, 5);
        timecharVector.setSafe(1, "1900-08-08 00:00:00".getBytes());
        timecharVector.setIndexDefined(2);
        timecharVector.setValueLengthSafe(2, 5);
        timecharVector.setSafe(2, "2100-08-08 00:00:00".getBytes());
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"BOOLEAN\",\"name\":\"k0\",\"comment\":\"\"},"
                + "{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},{\"type\":\"SMALLINT\",\"name\":\"k2\","
                + "\"comment\":\"\"},{\"type\":\"INT\",\"name\":\"k3\",\"comment\":\"\"},{\"type\":\"BIGINT\","
                + "\"name\":\"k4\",\"comment\":\"\"},{\"type\":\"FLOAT\",\"name\":\"k9\",\"comment\":\"\"},"
                + "{\"type\":\"DOUBLE\",\"name\":\"k8\",\"comment\":\"\"},{\"type\":\"DATE\",\"name\":\"k10\","
                + "\"comment\":\"\"},{\"type\":\"DATETIME\",\"name\":\"k11\",\"comment\":\"\"},"
                + "{\"name\":\"k5\",\"scale\":\"0\",\"comment\":\"\","
                + "\"type\":\"DECIMAL\",\"precision\":\"9\"},{\"type\":\"CHAR\",\"name\":\"k6\",\"comment\":\"\"}],"
                + "\"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema);

        List<Object> expectedRow1 = Arrays.asList(
                Boolean.TRUE,
                (byte) 1,
                (short) 1,
                1,
                1L,
                (float) 1.1,
                (double) 1.1,
                "2008-08-08",
                "2008-08-08 00:00:00",
                Decimal.apply(1234L, 4, 2),
                "char1"
        );

        List<Object> expectedRow2 = Arrays.asList(
                Boolean.FALSE,
                (byte) 2,
                (short) 2,
                null,
                2L,
                (float) 2.2,
                (double) 2.2,
                "1900-08-08",
                "1900-08-08 00:00:00",
                Decimal.apply(8888L, 4, 2),
                "char2"
        );

        List<Object> expectedRow3 = Arrays.asList(
                Boolean.TRUE,
                (byte) 3,
                (short) 3,
                3,
                3L,
                (float) 3.3,
                (double) 3.3,
                "2100-08-08",
                "2100-08-08 00:00:00",
                Decimal.apply(10L, 2, 0),
                "char3"
        );

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals(expectedRow1, actualRow1);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals(expectedRow2, actualRow2);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow3 = rowBatch.next();
        Assert.assertEquals(expectedRow3, actualRow3);

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testBinary() throws Exception {
        byte[] binaryRow0 = {'a', 'b', 'c'};
        byte[] binaryRow1 = {'d', 'e', 'f'};
        byte[] binaryRow2 = {'g', 'h', 'i'};

        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k7", FieldType.nullable(new ArrowType.Binary()), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k7");
        VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
        varBinaryVector.setInitialCapacity(3);
        varBinaryVector.allocateNew();
        varBinaryVector.setIndexDefined(0);
        varBinaryVector.setValueLengthSafe(0, 3);
        varBinaryVector.setSafe(0, binaryRow0);
        varBinaryVector.setIndexDefined(1);
        varBinaryVector.setValueLengthSafe(1, 3);
        varBinaryVector.setSafe(1, binaryRow1);
        varBinaryVector.setIndexDefined(2);
        varBinaryVector.setValueLengthSafe(2, 3);
        varBinaryVector.setSafe(2, binaryRow2);
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"BINARY\",\"name\":\"k7\",\"comment\":\"\"}], \"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow0, (byte[])actualRow0.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow1, (byte[])actualRow1.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertArrayEquals(binaryRow2, (byte[])actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }

    @Test
    public void testDecimalV2() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(new Field("k7", FieldType.nullable(new ArrowType.Decimal(27, 9)), null));

        VectorSchemaRoot root = VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(childrenBuilder.build(), null),
                new RootAllocator(Integer.MAX_VALUE));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(
                root,
                new DictionaryProvider.MapDictionaryProvider(),
                outputStream);

        arrowStreamWriter.start();
        root.setRowCount(3);

        FieldVector vector = root.getVector("k7");
        DecimalVector decimalVector = (DecimalVector) vector;
        decimalVector.setInitialCapacity(3);
        decimalVector.allocateNew(3);
        decimalVector.setSafe(0, new BigDecimal("12.340000000"));
        decimalVector.setSafe(1, new BigDecimal("88.880000000"));
        decimalVector.setSafe(2, new BigDecimal("10.000000000"));
        vector.setValueCount(3);

        arrowStreamWriter.writeBatch();

        arrowStreamWriter.end();
        arrowStreamWriter.close();

        TStatus status = new TStatus();
        status.setStatusCode(TStatusCode.OK);
        TScanBatchResult scanBatchResult = new TScanBatchResult();
        scanBatchResult.setStatus(status);
        scanBatchResult.setEos(false);
        scanBatchResult.setRows(outputStream.toByteArray());

        String schemaStr = "{\"properties\":[{\"type\":\"DECIMALV2\",\"scale\": 0,"
                + "\"precision\": 9, \"name\":\"k7\",\"comment\":\"\"}], "
                + "\"status\":200}";

        Schema schema = RestService.parseSchema(schemaStr, logger);

        RowBatch rowBatch = new RowBatch(scanBatchResult, schema);

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow0 = rowBatch.next();
        Assert.assertEquals(Decimal.apply(12340000000L, 11, 9), (Decimal)actualRow0.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow1 = rowBatch.next();
        Assert.assertEquals(Decimal.apply(88880000000L, 11, 9), (Decimal)actualRow1.get(0));

        Assert.assertTrue(rowBatch.hasNext());
        List<Object> actualRow2 = rowBatch.next();
        Assert.assertEquals(Decimal.apply(10000000000L, 11, 9), (Decimal)actualRow2.get(0));

        Assert.assertFalse(rowBatch.hasNext());
        thrown.expect(NoSuchElementException.class);
        thrown.expectMessage(startsWith("Get row offset:"));
        rowBatch.next();
    }
}
