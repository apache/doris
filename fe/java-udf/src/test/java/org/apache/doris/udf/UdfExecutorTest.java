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


        long batchSizePtr = UdfExecutor.UNSAFE.allocateMemory(32);
        int batchSize = 10;
        UdfExecutor.UNSAFE.putInt(batchSizePtr, batchSize);
        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.batch_size_ptr = batchSizePtr;
        params.fn = fn;

        long outputBuffer = UdfExecutor.UNSAFE.allocateMemory(32 * batchSize);
        long outputNull = UdfExecutor.UNSAFE.allocateMemory(8 * batchSize);
        long outputBufferPtr = UdfExecutor.UNSAFE.allocateMemory(64);
        UdfExecutor.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        long outputNullPtr = UdfExecutor.UNSAFE.allocateMemory(64);
        UdfExecutor.UNSAFE.putLong(outputNullPtr, outputNull);
        params.output_buffer_ptr = outputBufferPtr;
        params.output_null_ptr = outputNullPtr;
        params.input_buffer_ptrs = 0;
        params.input_nulls_ptrs = 0;
        params.input_byte_offsets = 0;

        TBinaryProtocol.Factory factory =
                new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor executor;
        executor = new UdfExecutor(serializer.serialize(params));

        executor.evaluate();
        for (int i = 0; i < 10; i ++) {
            assert (UdfExecutor.UNSAFE.getByte(outputNull + 8 * i) == 0);
            assert (UdfExecutor.UNSAFE.getInt(outputBuffer + 32 * i) == 1);
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


        long batchSizePtr = UdfExecutor.UNSAFE.allocateMemory(32);
        int batchSize = 10;
        UdfExecutor.UNSAFE.putInt(batchSizePtr, batchSize);

        TJavaUdfExecutorCtorParams params = new TJavaUdfExecutorCtorParams();
        params.batch_size_ptr = batchSizePtr;
        params.fn = fn;

        long outputBufferPtr = UdfExecutor.UNSAFE.allocateMemory(64);
        long outputNullPtr = UdfExecutor.UNSAFE.allocateMemory(64);
        long outputBuffer = UdfExecutor.UNSAFE.allocateMemory(32 * batchSize);
        long outputNull = UdfExecutor.UNSAFE.allocateMemory(8 * batchSize);
        UdfExecutor.UNSAFE.putLong(outputBufferPtr, outputBuffer);
        UdfExecutor.UNSAFE.putLong(outputNullPtr, outputNull);

        params.output_buffer_ptr = outputBufferPtr;
        params.output_null_ptr = outputNullPtr;

        int numCols = 2;
        long inputBufferPtr = UdfExecutor.UNSAFE.allocateMemory(64 * numCols);
        long inputNullPtr = UdfExecutor.UNSAFE.allocateMemory(64 * numCols);

        long inputBuffer1 = UdfExecutor.UNSAFE.allocateMemory(32 * batchSize);
        long inputNull1 = UdfExecutor.UNSAFE.allocateMemory(8 * batchSize);
        long inputBuffer2 = UdfExecutor.UNSAFE.allocateMemory(32 * batchSize);
        long inputNull2 = UdfExecutor.UNSAFE.allocateMemory(8 * batchSize);

        UdfExecutor.UNSAFE.putLong(inputBufferPtr, inputBuffer1);
        UdfExecutor.UNSAFE.putLong(inputBufferPtr + 64, inputBuffer2);
        UdfExecutor.UNSAFE.putLong(inputNullPtr, inputNull1);
        UdfExecutor.UNSAFE.putLong(inputNullPtr + 64, inputNull2);

        for (int i = 0; i < batchSize; i ++) {
            UdfExecutor.UNSAFE.putInt(null, inputBuffer1 + i * 32, i);
            UdfExecutor.UNSAFE.putInt(null, inputBuffer2 + i * 32, i);

            if (i % 2 == 0) {
                UdfExecutor.UNSAFE.putByte(null, inputNull1 + i * 8, (byte) 1);
            } else {
                UdfExecutor.UNSAFE.putByte(null, inputNull1 + i * 8, (byte) 0);
            }
            UdfExecutor.UNSAFE.putByte(null, inputNull2 + i * 8, (byte) 0);
        }
        params.input_buffer_ptrs = inputBufferPtr;
        params.input_nulls_ptrs = inputNullPtr;
        params.input_byte_offsets = 0;

        TBinaryProtocol.Factory factory =
                new TBinaryProtocol.Factory();
        TSerializer serializer = new TSerializer(factory);

        UdfExecutor executor;
        executor = new UdfExecutor(serializer.serialize(params));

        executor.evaluate();
        for (int i = 0; i < batchSize; i ++) {
            if (i % 2 == 0) {
                assert (UdfExecutor.UNSAFE.getByte(outputNull + 8 * i) == 1);
            } else {
                assert (UdfExecutor.UNSAFE.getByte(outputNull + 8 * i) == 0);
                assert (UdfExecutor.UNSAFE.getInt(outputBuffer + 32 * i) == i * 2);
            }
        }
    }
}
