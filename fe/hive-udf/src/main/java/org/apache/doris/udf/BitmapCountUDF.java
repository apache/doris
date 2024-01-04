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

@Description(name = "bitmap_count", value = "a _FUNC_ b - Returns the number of distinct integers"
        + " added to the bitmap (e.g., number of bits set)")
public class BitmapCountUDF extends GenericUDF {
    private transient BinaryObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        ObjectInspector input = arguments[0];
        if (!(input instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("first argument must be a binary");
        }

        this.inputOI = (BinaryObjectInspector) input;

        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[]  args) throws HiveException {
        if (args[0] == null) {
            return 0;
        }
        byte[] inputBytes = this.inputOI.getPrimitiveJavaObject(args[0].get());

        try {
            BitmapValue bitmapValue = BitmapValueUtil.deserializeToBitmap(inputBytes);
            return bitmapValue.cardinality();
        } catch (IOException ioException) {
            throw new HiveException(ioException);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_count(bitmap)";
    }
}
