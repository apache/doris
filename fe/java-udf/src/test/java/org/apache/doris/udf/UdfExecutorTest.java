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

import org.apache.doris.thrift.*;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class UdfExecutorTest {
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
        for (int i = 0; i < 10; i ++) {
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

        for (int i = 0; i < batchSize; i ++) {
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
        for (int i = 0; i < batchSize; i ++) {
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
        for (int i = 0; i < batchSize; i ++) {
            input1[i] = "Input1_" + i;
            input2[i] = "Input2_" + i;
            inputOffsets1[i] = i == 0? input1[i].getBytes(StandardCharsets.UTF_8).length + 1:
                    inputOffsets1[i - 1] + input1[i].getBytes(StandardCharsets.UTF_8).length + 1;
            inputOffsets2[i] = i == 0? input2[i].getBytes(StandardCharsets.UTF_8).length + 1:
                    inputOffsets2[i - 1] + input2[i].getBytes(StandardCharsets.UTF_8).length + 1;
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

        for (int i = 0; i < batchSize; i ++) {
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
            UdfUtils.UNSAFE.putChar(null, inputBuffer1 + inputOffsets1[i] - 1,
                    UdfUtils.END_OF_STRING);
            UdfUtils.UNSAFE.putChar(null, inputBuffer2 + inputOffsets2[i] - 1,
                    UdfUtils.END_OF_STRING);

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
        for (int i = 0; i < batchSize; i ++) {
            byte[] bytes = new byte[input1[i].getBytes(StandardCharsets.UTF_8).length +
                    input2[i].getBytes(StandardCharsets.UTF_8).length];
            assert (UdfUtils.UNSAFE.getByte(outputNull + i) == 0);
            if (i == 0) {
                UdfUtils.copyMemory(null, outputBuffer, bytes, UdfUtils.BYTE_ARRAY_OFFSET,
                        bytes.length);
            } else {
                long lastOffset = UdfUtils.UNSAFE.getInt(null, outputOffset + 4 * (i - 1));
                UdfUtils.copyMemory(null, outputBuffer + lastOffset, bytes, UdfUtils.BYTE_ARRAY_OFFSET,
                        bytes.length);
            }
            long curOffset = UdfUtils.UNSAFE.getInt(null, outputOffset + 4 * i);
            assert (new String(bytes, StandardCharsets.UTF_8).equals(input1[i] + input2[i]));
            assert (UdfUtils.UNSAFE.getByte(null, outputBuffer + curOffset - 1) == UdfUtils.END_OF_STRING);
            assert (UdfUtils.UNSAFE.getByte(null, outputNull + i) == 0);
        }
    }
}
