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

package org.apache.doris.load.loadv2.dpp;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HllUnionUDAF extends UserDefinedAggregateFunction {

    private StructType inputSchema;
    private StructType bufferSchema;

    public HllUnionUDAF(DataType dataType) {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("str", dataType,true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("hll", DataTypes.BinaryType,true));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BinaryType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    public static byte[] serializeHll(Hll hll) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(bos);
            hll.serialize(outputStream);
            return bos.toByteArray();
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new RuntimeException(ioException);
        }
    }

    public static Hll deserializeHll(byte[] hllByte) {
        try {
            DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(hllByte));
            Hll hll = new Hll();
            hll.deserialize(inputStream);
            return hll;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        Hll hll = new Hll();
        buffer.update(0, serializeHll(hll));
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (input.isNullAt(0)) {
            throw new RuntimeException("hll doesn't support null");
        }
        Object dstHllBuffer = buffer.get(0);
        byte[] dstHllByte = (byte[]) dstHllBuffer;
        Hll dstHll = deserializeHll(dstHllByte);
        Object srcValue = input.get(0);

        if (srcValue instanceof String) {
            dstHll.updateWithHash(srcValue.toString());
        } else if (srcValue instanceof byte[]) {
            byte[] srcByte = (byte[]) srcValue;
            dstHll.merge(deserializeHll(srcByte));
        } else {
            throw new RuntimeException("unknown column type:" + srcValue.getClass());
        }
        buffer.update(0, serializeHll(dstHll));
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        byte[] hllBytes1 = (byte[])buffer1.get(0);
        byte[] hllBytes2 = (byte[])buffer2.get(0);
        Hll hll1 = deserializeHll(hllBytes1);
        Hll hll2 = deserializeHll(hllBytes2);
        hll1.merge(hll2);
        buffer1.update(0, serializeHll(hll1));
    }

    @Override
    public Object evaluate(Row buffer) {
        byte[] testByte = (byte[])buffer.get(0);
        Hll hll = deserializeHll(testByte);
        System.out.println("print hll evaluate: " + hll.estimateCardinality());
        return testByte;
    }
}
