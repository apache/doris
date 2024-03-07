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

import org.apache.doris.common.BitmapValueUtil;
import org.apache.doris.common.io.BitmapValue;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;

@Description(name = "bitmap_or", value = "a _FUNC_ b - Compute"
        + " union of two or more input bitmaps, returns the new bitmap")
public class BitmapOrUDF extends GenericUDF {
    private transient BinaryObjectInspector inputOI0;
    private transient BinaryObjectInspector inputOI1;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        ObjectInspector input0 = arguments[0];
        ObjectInspector input1 = arguments[1];
        if (!(input0 instanceof BinaryObjectInspector) || !(input1 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("first and second argument must be a binary");
        }

        this.inputOI0 = (BinaryObjectInspector) input0;
        this.inputOI1 = (BinaryObjectInspector) input1;

        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[]  args) throws HiveException {
        if (args[0] == null || args[1] == null) {
            return null;
        }
        byte[] inputBytes0 = this.inputOI0.getPrimitiveJavaObject(args[0].get());
        byte[] inputBytes1 = this.inputOI1.getPrimitiveJavaObject(args[1].get());

        try {
            BitmapValue bitmapValue0 = BitmapValueUtil.deserializeToBitmap(inputBytes0);
            BitmapValue bitmapValue1 = BitmapValueUtil.deserializeToBitmap(inputBytes1);
            bitmapValue0.or(bitmapValue1);
            return BitmapValueUtil.serializeToBytes(bitmapValue0);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_or(bitmap1,bitmap2)";
    }
}
