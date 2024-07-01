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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Description(name = "bitmap_from_string", value = "_FUNC_(expr) - Convert a string to a Doris's bitmap, "
        + " return a bitmap")
public class BitmapFromStringUDF extends GenericUDF {
    private transient StringObjectInspector inputOI0;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        ObjectInspector input0 = arguments[0];
        if (!(input0 instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a string");
        }

        this.inputOI0 = (StringObjectInspector) input0;

        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0] == null) {
            return null;
        }
        String inputStr0 = this.inputOI0.getPrimitiveJavaObject(args[0].get());
        BitmapValue bitmapValue0 = new BitmapValue();

        try {
            for (String s : inputStr0.split(",")) {
                long v = Long.parseLong(s.trim());
                bitmapValue0.add(v);
            }
            return BitmapValueUtil.serializeToBytes(bitmapValue0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_from_string(str)";
    }
}
