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

package org.apache.doris.udf;

import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;
import org.apache.doris.thrift.TFunctionName;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarFunction;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class UdfExecutorTest {

    @Test
    public void testDateTimeUdf() throws Exception {
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.symbol = "org.apache.doris.udf.DateTimeUdf";

        TFunction fn = new TFunction();
        fn.setBinaryType(TFunctionBinaryType.JAVA_UDF);
        TTypeNode typeNode = new TTypeNode(TTypeNodeType.SCALAR);
        typeNode.setScalarType(new TScalarType(TPrimitiveType.INT));
        fn.setRetType(new TTypeDesc(Collections.singletonList(typeNode)));

        TTypeNode typeNodeArg = new TTypeNode(TTypeNodeType.SCALAR);
        typeNodeArg.setScalarType(new TScalarType(TPrimitiveType.DATETIME));
        TTypeDesc typeDescArg = new TTypeDesc(Collections.singletonList(typeNodeArg));
        fn.arg_types = Arrays.asList(typeDescArg);

        fn.scalar_fn = scalarFunction;
        fn.name = new TFunctionName("DateTimeUdf");

        long batchSizePtr = UdfUtils.UNSAFE.allocateMemory(4);
        int batchSize = 10;
        UdfUtils.UNSAFE.putInt(batchSizePtr, batchSize);

        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setBatchSizePtr(batchSizePtr);
        params.setFn(fn);

        long outputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputNullPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputBuffer = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);
        long outputNull = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        UdfUtils.UNSAFE.putLong(outputNullPtr, outputNull);

        params.setOutputBufferPtr(outputBufferPtr);
        params.setOutputNullPtr(outputNullPtr);

        int numCols = 1;
        long inputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);
        long inputNullPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);

        long inputBuffer1 = UdfUtils.UNSAFE.allocateMemory(8 * batchSize);
        long inputNull1 = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(inputBufferPtr, inputBuffer1);
        UdfUtils.UNSAFE.putLong(inputNullPtr, inputNull1);

        long[] inputLongDateTime =
                new long[] {562960991655690406L, 563242466632401062L, 563523941609111718L, 563805416585822374L,
                        564086891562533030L, 564368366539243686L, 564649841515954342L, 564931316492664998L,
                        565212791469375654L, 565494266446086310L};

        for (int i = 0; i < batchSize; ++i) {
            UdfUtils.UNSAFE.putLong(null, inputBuffer1 + i * 8, inputLongDateTime[i]);
            UdfUtils.UNSAFE.putByte(null, inputNull1 + i, (byte) 0);
        }

        params.setInputBufferPtrs(inputBufferPtr);
        params.setInputNullsPtrs(inputNullPtr);
        params.setInputOffsetsPtrs(0);

        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor executor = new UdfExecutor(serializer.serialize(params));
        executor.evaluate();

        for (int i = 0; i < batchSize; ++i) {
            assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
            assert (UdfUtils.UNSAFE.getInt(outputBuffer + 4 * i) == (2000 + i));
        }
    }

    @Test
    public void testDecimalUdf() throws Exception {
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.symbol = "org.apache.doris.udf.DecimalUdf";
        TFunction fn = new TFunction();
        fn.binary_type = TFunctionBinaryType.JAVA_UDF;
        TTypeNode typeNode = new TTypeNode(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType(TPrimitiveType.DECIMALV2);
        scalarType.setScale(9);
        scalarType.setPrecision(27);
        typeNode.scalar_type = scalarType;
        TTypeDesc typeDesc = new TTypeDesc(Collections.singletonList(typeNode));
        fn.ret_type = typeDesc;
        fn.arg_types = Arrays.asList(typeDesc, typeDesc);
        fn.scalar_fn = scalarFunction;
        fn.name = new TFunctionName("DecimalUdf");

        long batchSizePtr = UdfUtils.UNSAFE.allocateMemory(8);
        int batchSize = 10;
        UdfUtils.UNSAFE.putInt(batchSizePtr, batchSize);

        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setBatchSizePtr(batchSizePtr);
        params.setFn(fn);

        long outputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputNullPtr = UdfUtils.UNSAFE.allocateMemory(8);

        long outputBuffer = UdfUtils.UNSAFE.allocateMemory(16 * batchSize);
        long outputNull = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        UdfUtils.UNSAFE.putLong(outputNullPtr, outputNull);

        params.setOutputBufferPtr(outputBufferPtr);
        params.setOutputNullPtr(outputNullPtr);

        int numCols = 2;
        long inputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);
        long inputNullPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);

        long inputBuffer1 = UdfUtils.UNSAFE.allocateMemory(16 * batchSize);
        long inputNull1 = UdfUtils.UNSAFE.allocateMemory(batchSize);

        long inputBuffer2 = UdfUtils.UNSAFE.allocateMemory(16 * batchSize);
        long inputNull2 = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(inputBufferPtr, inputBuffer1);
        UdfUtils.UNSAFE.putLong(inputBufferPtr + 8, inputBuffer2);
        UdfUtils.UNSAFE.putLong(inputNullPtr, inputNull1);
        UdfUtils.UNSAFE.putLong(inputNullPtr + 8, inputNull2);

        long[] inputLong =
                new long[] {562960991655690406L, 563242466632401062L, 563523941609111718L, 563805416585822374L,
                        564086891562533030L, 564368366539243686L, 564649841515954342L, 564931316492664998L,
                        565212791469375654L, 565494266446086310L};

        BigDecimal[] decimalArray = new BigDecimal[10];
        for (int i = 0; i < batchSize; ++i) {
            BigInteger temp = BigInteger.valueOf(inputLong[i]);
            decimalArray[i] = new BigDecimal(temp, 9);
        }

        BigDecimal decimal2 = new BigDecimal(BigInteger.valueOf(0L), 9);
        byte[] intput2 = convertByteOrder(decimal2.unscaledValue().toByteArray());
        byte[] value2 = new byte[16];
        if (decimal2.signum() == -1) {
            Arrays.fill(value2, (byte) -1);
        }
        for (int index = 0; index < Math.min(intput2.length, value2.length); ++index) {
            value2[index] = intput2[index];
        }

        for (int i = 0; i < batchSize; ++i) {
            byte[] intput1 = convertByteOrder(decimalArray[i].unscaledValue().toByteArray());
            byte[] value1 = new byte[16];
            if (decimalArray[i].signum() == -1) {
                Arrays.fill(value1, (byte) -1);
            }
            for (int index = 0; index < Math.min(intput1.length, value1.length); ++index) {
                value1[index] = intput1[index];
            }
            UdfUtils.copyMemory(value1, UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer1 + i * 16, value1.length);
            UdfUtils.copyMemory(value2, UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer2 + i * 16, value2.length);
            UdfUtils.UNSAFE.putByte(null, inputNull1 + i, (byte) 0);
            UdfUtils.UNSAFE.putByte(null, inputNull2 + i, (byte) 0);
        }

        params.setInputBufferPtrs(inputBufferPtr);
        params.setInputNullsPtrs(inputNullPtr);
        params.setInputOffsetsPtrs(0);

        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor udfExecutor = new UdfExecutor(serializer.serialize(params));
        udfExecutor.evaluate();

        for (int i = 0; i < batchSize; ++i) {
            byte[] bytes = new byte[16];
            assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
            UdfUtils.copyMemory(null, outputBuffer + 16 * i, bytes, UdfUtils.BYTE_ARRAY_OFFSET, bytes.length);

            BigInteger integer = new BigInteger(convertByteOrder(bytes));
            BigDecimal result = new BigDecimal(integer, 9);
            assert (result.equals(decimalArray[i]));
        }
    }

    @Test
    public void testConstantOneUdf() throws Exception {
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.symbol = "org.apache.doris.udf.ConstantOneUdf";

        TFunction fn = new TFunction();
        fn.binary_type = TFunctionBinaryType.JAVA_UDF;
        TTypeNode typeNode = new TTypeNode(TTypeNodeType.SCALAR);
        typeNode.scalar_type = new TScalarType(TPrimitiveType.INT);
        fn.ret_type = new TTypeDesc(Collections.singletonList(typeNode));
        fn.arg_types = new ArrayList<>();
        fn.scalar_fn = scalarFunction;
        fn.name = new TFunctionName("ConstantOne");


        long batchSizePtr = UdfUtils.UNSAFE.allocateMemory(4);
        int batchSize = 10;
        UdfUtils.UNSAFE.putInt(batchSizePtr, batchSize);
        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setBatchSizePtr(batchSizePtr);
        params.setFn(fn);

        long outputBuffer = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);
        long outputNull = UdfUtils.UNSAFE.allocateMemory(batchSize);
        long outputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8);
        UdfUtils.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        long outputNullPtr = UdfUtils.UNSAFE.allocateMemory(8);
        UdfUtils.UNSAFE.putLong(outputNullPtr, outputNull);
        params.setOutputBufferPtr(outputBufferPtr);
        params.setOutputNullPtr(outputNullPtr);
        params.setInputBufferPtrs(0);
        params.setInputNullsPtrs(0);
        params.setInputOffsetsPtrs(0);

        TBinaryProtocol.Factory factory =
                new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor executor;
        executor = new UdfExecutor(serializer.serialize(params));

        executor.evaluate();
        for (int i = 0; i < 10; i++) {
            assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
            assert (UdfUtils.UNSAFE.getInt(outputBuffer + 4 * i) == 1);
        }
    }

    @Test
    public void testSimpleAddUdf() throws Exception {
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.symbol = "org.apache.doris.udf.SimpleAddUdf";

        TFunction fn = new TFunction();
        fn.binary_type = TFunctionBinaryType.JAVA_UDF;
        TTypeNode typeNode = new TTypeNode(TTypeNodeType.SCALAR);
        typeNode.scalar_type = new TScalarType(TPrimitiveType.INT);
        TTypeDesc typeDesc = new TTypeDesc(Collections.singletonList(typeNode));
        fn.ret_type = typeDesc;
        fn.arg_types = Arrays.asList(typeDesc, typeDesc);
        fn.scalar_fn = scalarFunction;
        fn.name = new TFunctionName("SimpleAdd");


        long batchSizePtr = UdfUtils.UNSAFE.allocateMemory(4);
        int batchSize = 10;
        UdfUtils.UNSAFE.putInt(batchSizePtr, batchSize);

        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setBatchSizePtr(batchSizePtr);
        params.setFn(fn);

        long outputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputNullPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputBuffer = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);
        long outputNull = UdfUtils.UNSAFE.allocateMemory(batchSize);
        UdfUtils.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        UdfUtils.UNSAFE.putLong(outputNullPtr, outputNull);

        params.setOutputBufferPtr(outputBufferPtr);
        params.setOutputNullPtr(outputNullPtr);

        int numCols = 2;
        long inputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);
        long inputNullPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);

        long inputBuffer1 = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);
        long inputNull1 = UdfUtils.UNSAFE.allocateMemory(batchSize);
        long inputBuffer2 = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);
        long inputNull2 = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(inputBufferPtr, inputBuffer1);
        UdfUtils.UNSAFE.putLong(inputBufferPtr + 8, inputBuffer2);
        UdfUtils.UNSAFE.putLong(inputNullPtr, inputNull1);
        UdfUtils.UNSAFE.putLong(inputNullPtr + 8, inputNull2);

        for (int i = 0; i < batchSize; i++) {
            UdfUtils.UNSAFE.putInt(null, inputBuffer1 + i * 4, i);
            UdfUtils.UNSAFE.putInt(null, inputBuffer2 + i * 4, i);

            if (i % 2 == 0) {
                UdfUtils.UNSAFE.putByte(null, inputNull1 + i, (byte) 1);
            } else {
                UdfUtils.UNSAFE.putByte(null, inputNull1 + i, (byte) 0);
            }
            UdfUtils.UNSAFE.putByte(null, inputNull2 + i, (byte) 0);
        }
        params.setInputBufferPtrs(inputBufferPtr);
        params.setInputNullsPtrs(inputNullPtr);
        params.setInputOffsetsPtrs(0);

        TBinaryProtocol.Factory factory =
                new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor executor;
        executor = new UdfExecutor(serializer.serialize(params));

        executor.evaluate();
        for (int i = 0; i < batchSize; i++) {
            if (i % 2 == 0) {
                assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 1);
            } else {
                assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
                assert (UdfUtils.UNSAFE.getInt(outputBuffer + 4 * i) == i * 2);
            }
        }
    }

    @Test
    public void testStringConcatUdf() throws Exception {
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.symbol = "org.apache.doris.udf.StringConcatUdf";

        TFunction fn = new TFunction();
        fn.binary_type = TFunctionBinaryType.JAVA_UDF;
        TTypeNode typeNode = new TTypeNode(TTypeNodeType.SCALAR);
        typeNode.scalar_type = new TScalarType(TPrimitiveType.STRING);
        TTypeDesc typeDesc = new TTypeDesc(Collections.singletonList(typeNode));
        fn.ret_type = typeDesc;
        fn.arg_types = Arrays.asList(typeDesc, typeDesc);
        fn.scalar_fn = scalarFunction;
        fn.name = new TFunctionName("StringConcat");


        long batchSizePtr = UdfUtils.UNSAFE.allocateMemory(32);
        int batchSize = 10;
        UdfUtils.UNSAFE.putInt(batchSizePtr, batchSize);

        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setBatchSizePtr(batchSizePtr);
        params.setFn(fn);

        long outputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputNullPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputOffsetsPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputIntermediateStatePtr = UdfUtils.UNSAFE.allocateMemory(8 * 2);

        String[] input1 = new String[batchSize];
        String[] input2 = new String[batchSize];
        long[] inputOffsets1 = new long[batchSize];
        long[] inputOffsets2 = new long[batchSize];
        long inputBufferSize1 = 0;
        long inputBufferSize2 = 0;
        for (int i = 0; i < batchSize; i++) {
            input1[i] = "Input1_" + i;
            input2[i] = "Input2_" + i;
            inputOffsets1[i] = i == 0 ? input1[i].getBytes(StandardCharsets.UTF_8).length
                    : inputOffsets1[i - 1] + input1[i].getBytes(StandardCharsets.UTF_8).length;
            inputOffsets2[i] = i == 0 ? input2[i].getBytes(StandardCharsets.UTF_8).length
                    : inputOffsets2[i - 1] + input2[i].getBytes(StandardCharsets.UTF_8).length;
            inputBufferSize1 += input1[i].getBytes(StandardCharsets.UTF_8).length;
            inputBufferSize2 += input2[i].getBytes(StandardCharsets.UTF_8).length;
        }
        // In our test case, output buffer is (8 + 1) bytes * batchSize
        long outputBuffer = UdfUtils.UNSAFE.allocateMemory(inputBufferSize1 + inputBufferSize2 + batchSize);
        long outputNull = UdfUtils.UNSAFE.allocateMemory(batchSize);
        long outputOffset = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);

        UdfUtils.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        UdfUtils.UNSAFE.putLong(outputNullPtr, outputNull);
        UdfUtils.UNSAFE.putLong(outputOffsetsPtr, outputOffset);
        // reserved buffer size
        UdfUtils.UNSAFE.putLong(outputIntermediateStatePtr, inputBufferSize1 + inputBufferSize2 + batchSize);
        // current row id
        UdfUtils.UNSAFE.putLong(outputIntermediateStatePtr + 8, 0);

        params.setOutputBufferPtr(outputBufferPtr);
        params.setOutputNullPtr(outputNullPtr);
        params.setOutputOffsetsPtr(outputOffsetsPtr);
        params.setOutputIntermediateStatePtr(outputIntermediateStatePtr);

        int numCols = 2;
        long inputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);
        long inputNullPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);
        long inputOffsetsPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);

        long inputBuffer1 = UdfUtils.UNSAFE.allocateMemory(inputBufferSize1 + batchSize);
        long inputOffset1 = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);
        long inputBuffer2 = UdfUtils.UNSAFE.allocateMemory(inputBufferSize2 + batchSize);
        long inputOffset2 = UdfUtils.UNSAFE.allocateMemory(4 * batchSize);

        UdfUtils.UNSAFE.putLong(inputBufferPtr, inputBuffer1);
        UdfUtils.UNSAFE.putLong(inputBufferPtr + 8, inputBuffer2);
        UdfUtils.UNSAFE.putLong(inputNullPtr, -1);
        UdfUtils.UNSAFE.putLong(inputNullPtr + 8, -1);
        UdfUtils.UNSAFE.putLong(inputOffsetsPtr, inputOffset1);
        UdfUtils.UNSAFE.putLong(inputOffsetsPtr + 8, inputOffset2);

        for (int i = 0; i < batchSize; i++) {
            if (i == 0) {
                UdfUtils.copyMemory(input1[i].getBytes(StandardCharsets.UTF_8),
                        UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer1,
                        input1[i].getBytes(StandardCharsets.UTF_8).length);
                UdfUtils.copyMemory(input2[i].getBytes(StandardCharsets.UTF_8),
                        UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer2,
                        input2[i].getBytes(StandardCharsets.UTF_8).length);
            } else {
                UdfUtils.copyMemory(input1[i].getBytes(StandardCharsets.UTF_8),
                        UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer1 + inputOffsets1[i - 1],
                        input1[i].getBytes(StandardCharsets.UTF_8).length);
                UdfUtils.copyMemory(input2[i].getBytes(StandardCharsets.UTF_8),
                        UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer2 + inputOffsets2[i - 1],
                        input2[i].getBytes(StandardCharsets.UTF_8).length);
            }
            UdfUtils.UNSAFE.putInt(null, inputOffset1 + 4L * i,
                    Integer.parseUnsignedInt(String.valueOf(inputOffsets1[i])));
            UdfUtils.UNSAFE.putInt(null, inputOffset2 + 4L * i,
                    Integer.parseUnsignedInt(String.valueOf(inputOffsets2[i])));
        }
        params.setInputBufferPtrs(inputBufferPtr);
        params.setInputNullsPtrs(inputNullPtr);
        params.setInputOffsetsPtrs(inputOffsetsPtr);

        TBinaryProtocol.Factory factory =
                new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor executor;
        executor = new UdfExecutor(serializer.serialize(params));

        executor.evaluate();
        for (int i = 0; i < batchSize; i++) {
            byte[] bytes = new byte[input1[i].getBytes(StandardCharsets.UTF_8).length
                    + input2[i].getBytes(StandardCharsets.UTF_8).length];
            assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
            if (i == 0) {
                UdfUtils.copyMemory(null, outputBuffer, bytes, UdfUtils.BYTE_ARRAY_OFFSET,
                        bytes.length);
            } else {
                long lastOffset = UdfUtils.UNSAFE.getInt(null, outputOffset + 4 * (i - 1));
                UdfUtils.copyMemory(null, outputBuffer + lastOffset, bytes, UdfUtils.BYTE_ARRAY_OFFSET,
                        bytes.length);
            }
            assert (new String(bytes, StandardCharsets.UTF_8).equals(input1[i] + input2[i]));
            assert (UdfUtils.UNSAFE.getByte(null, outputNull + i) == 0);
        }
    }

    @Test
    public void testLargeIntUdf() throws Exception {
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.symbol = "org.apache.doris.udf.LargeIntUdf";
        TFunction fn = new TFunction();
        fn.binary_type = TFunctionBinaryType.JAVA_UDF;
        TTypeNode typeNode = new TTypeNode(TTypeNodeType.SCALAR);
        typeNode.scalar_type = new TScalarType(TPrimitiveType.LARGEINT);

        TTypeDesc typeDesc = new TTypeDesc(Collections.singletonList(typeNode));

        fn.ret_type = typeDesc;
        fn.arg_types = Arrays.asList(typeDesc, typeDesc);
        fn.scalar_fn = scalarFunction;
        fn.name = new TFunctionName("LargeIntUdf");

        long batchSizePtr = UdfUtils.UNSAFE.allocateMemory(8);
        int batchSize = 10;
        UdfUtils.UNSAFE.putInt(batchSizePtr, batchSize);

        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.setBatchSizePtr(batchSizePtr);
        params.setFn(fn);

        long outputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8);
        long outputNullPtr = UdfUtils.UNSAFE.allocateMemory(8);

        long outputBuffer = UdfUtils.UNSAFE.allocateMemory(16 * batchSize);
        long outputNull = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        UdfUtils.UNSAFE.putLong(outputNullPtr, outputNull);

        params.setOutputBufferPtr(outputBufferPtr);
        params.setOutputNullPtr(outputNullPtr);

        int numCols = 2;
        long inputBufferPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);
        long inputNullPtr = UdfUtils.UNSAFE.allocateMemory(8 * numCols);

        long inputBuffer1 = UdfUtils.UNSAFE.allocateMemory(16 * batchSize);
        long inputNull1 = UdfUtils.UNSAFE.allocateMemory(batchSize);

        long inputBuffer2 = UdfUtils.UNSAFE.allocateMemory(16 * batchSize);
        long inputNull2 = UdfUtils.UNSAFE.allocateMemory(batchSize);

        UdfUtils.UNSAFE.putLong(inputBufferPtr, inputBuffer1);
        UdfUtils.UNSAFE.putLong(inputBufferPtr + 8, inputBuffer2);
        UdfUtils.UNSAFE.putLong(inputNullPtr, inputNull1);
        UdfUtils.UNSAFE.putLong(inputNullPtr + 8, inputNull2);

        long[] inputLong =
                new long[] {562960991655690406L, 563242466632401062L, 563523941609111718L, 563805416585822374L,
                        564086891562533030L, 564368366539243686L, 564649841515954342L, 564931316492664998L,
                        565212791469375654L, 565494266446086310L};

        BigInteger[] integerArray = new BigInteger[10];
        for (int i = 0; i < batchSize; ++i) {
            integerArray[i] = BigInteger.valueOf(inputLong[i]);
        }
        BigInteger integer2 = BigInteger.valueOf(1L);
        byte[] intput2 = convertByteOrder(integer2.toByteArray());
        byte[] value2 = new byte[16];
        if (integer2.signum() == -1) {
            Arrays.fill(value2, (byte) -1);
        }
        for (int index = 0; index < Math.min(intput2.length, value2.length); ++index) {
            value2[index] = intput2[index];
        }

        for (int i = 0; i < batchSize; ++i) {
            byte[] intput1 = convertByteOrder(integerArray[i].toByteArray());
            byte[] value1 = new byte[16];
            if (integerArray[i].signum() == -1) {
                Arrays.fill(value1, (byte) -1);
            }
            for (int index = 0; index < Math.min(intput1.length, value1.length); ++index) {
                value1[index] = intput1[index];
            }
            UdfUtils.copyMemory(value1, UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer1 + i * 16, value1.length);
            UdfUtils.copyMemory(value2, UdfUtils.BYTE_ARRAY_OFFSET, null, inputBuffer2 + i * 16, value2.length);
            UdfUtils.UNSAFE.putByte(null, inputNull1 + i, (byte) 0);
            UdfUtils.UNSAFE.putByte(null, inputNull2 + i, (byte) 0);
        }

        params.setInputBufferPtrs(inputBufferPtr);
        params.setInputNullsPtrs(inputNullPtr);
        params.setInputOffsetsPtrs(0);

        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor udfExecutor = new UdfExecutor(serializer.serialize(params));
        udfExecutor.evaluate();

        for (int i = 0; i < batchSize; ++i) {
            byte[] bytes = new byte[16];
            assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
            UdfUtils.copyMemory(null, outputBuffer + 16 * i, bytes, UdfUtils.BYTE_ARRAY_OFFSET, bytes.length);
            BigInteger result = new BigInteger(convertByteOrder(bytes));
            assert (result.equals(integerArray[i].add(BigInteger.valueOf(1))));
        }
    }

    public byte[] convertByteOrder(byte[] bytes) {
        int length = bytes.length;
        for (int i = 0; i < length / 2; ++i) {
            byte temp = bytes[i];
            bytes[i] = bytes[length - 1 - i];
            bytes[length - 1 - i] = temp;
        }
        return bytes;
    }
}
