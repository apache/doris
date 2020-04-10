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
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

public class BitmapUnion extends UserDefinedAggregateFunction {
    private StructType inputSchema;
    private StructType bufferSchema;

    public BitmapUnion() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("str", DataTypes.StringType,true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("bitmap", DataTypes.BinaryType,true));
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
        return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.runOptimize();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            roaringBitmap.serialize(new DataOutputStream(bos));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        buffer.update(0, bos.toByteArray());
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // here maybe there is performance problems
        if (!input.isNullAt(0)) {
            String value = input.getString(0);
            int id = Integer.parseInt(value);
            Object bitmapObj = buffer.get(0);
            byte[] bitmapBytes = (byte[])bitmapObj;
            RoaringBitmap bitmap = new RoaringBitmap();
            try {
                bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bitmapBytes)));
                bitmap.add(id);
                bitmap.runOptimize();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                bitmap.serialize(new DataOutputStream(bos));
                buffer.update(0, bos.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        byte[] bitmap2Bytes = (byte[])buffer2.get(0);
        byte[] bitmap1Bytes = (byte[])buffer1.get(0);
        RoaringBitmap bitmap1 = new RoaringBitmap();
        RoaringBitmap bitmap2 = new RoaringBitmap();
        try {
            bitmap1.deserialize(new DataInputStream(new ByteArrayInputStream(bitmap1Bytes)));
            bitmap2.deserialize(new DataInputStream(new ByteArrayInputStream(bitmap2Bytes)));
            bitmap1.or(bitmap2);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bitmap1.serialize(new DataOutputStream(bos));
            buffer1.update(0, bos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object evaluate(Row buffer) {
        byte[] bitmapBytes = (byte[]) buffer.get(0);
        return bitmapBytes;
    }
}
