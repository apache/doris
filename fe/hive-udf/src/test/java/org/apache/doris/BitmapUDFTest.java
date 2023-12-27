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

package org.apache.doris;

import org.apache.doris.common.BitmapValueUtil;
import org.apache.doris.common.io.BitmapValue;
import org.apache.doris.udf.BitmapAndUDF;
import org.apache.doris.udf.BitmapCountUDF;
import org.apache.doris.udf.BitmapOrUDF;
import org.apache.doris.udf.BitmapXorUDF;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBinaryObjectInspector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

// hive bitmap udf test
public class BitmapUDFTest {

    private byte[] bitmapValue0Bytes;
    private byte[] bitmapValue1Bytes;

    private BinaryObjectInspector inputOI0 = new JavaConstantBinaryObjectInspector(new byte[0]);
    private BinaryObjectInspector inputOI1 = new JavaConstantBinaryObjectInspector(new byte[0]);

    @Before
    public void initData() throws IOException {
        BitmapValue bitmapValue0 = new BitmapValue();
        BitmapValue bitmapValue1 = new BitmapValue();

        bitmapValue0.add(1);
        bitmapValue0.add(2);

        bitmapValue1.add(2);
        bitmapValue1.add(3);
        bitmapValue1.add(4);

        bitmapValue0Bytes = BitmapValueUtil.serializeToBytes(bitmapValue0);
        bitmapValue1Bytes = BitmapValueUtil.serializeToBytes(bitmapValue1);
    }

    @Test
    public void bitmapAndTest() throws Exception {
        BitmapAndUDF bitmapAndUDF = new BitmapAndUDF();
        bitmapAndUDF.initialize(new ObjectInspector[]{inputOI0, inputOI1});

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(bitmapValue0Bytes), new GenericUDF.DeferredJavaObject(bitmapValue1Bytes)};
        Object evaluate = bitmapAndUDF.evaluate(args);
        BitmapValue resultBitmap = BitmapValueUtil.deserializeToBitmap((byte[]) evaluate);

        Assert.assertEquals(1, resultBitmap.cardinality());
        Assert.assertEquals("{2}", resultBitmap.toString());
    }

    @Test
    public void bitmapOrTest() throws Exception {

        BitmapOrUDF bitmapOrUDF = new BitmapOrUDF();
        bitmapOrUDF.initialize(new ObjectInspector[]{inputOI0, inputOI1});

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(bitmapValue0Bytes), new GenericUDF.DeferredJavaObject(bitmapValue1Bytes)};
        Object evaluate = bitmapOrUDF.evaluate(args);
        BitmapValue resultBitmap = BitmapValueUtil.deserializeToBitmap((byte[]) evaluate);

        Assert.assertEquals(4, resultBitmap.cardinality());
        Assert.assertEquals("{1,2,3,4}", resultBitmap.toString());
    }

    @Test
    public void bitmapXorTest() throws Exception {
        BitmapXorUDF bitmapXorUDF = new BitmapXorUDF();
        bitmapXorUDF.initialize(new ObjectInspector[]{inputOI0, inputOI1});

        GenericUDF.DeferredObject[] args = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(bitmapValue0Bytes), new GenericUDF.DeferredJavaObject(bitmapValue1Bytes)};
        Object evaluate = bitmapXorUDF.evaluate(args);
        BitmapValue resultBitmap = BitmapValueUtil.deserializeToBitmap((byte[]) evaluate);

        Assert.assertEquals(3, resultBitmap.cardinality());
        Assert.assertEquals("{1,3,4}", resultBitmap.toString());
    }

    @Test
    public void bitmapCountTest() throws Exception {
        BitmapCountUDF bitmapCountUDF = new BitmapCountUDF();
        bitmapCountUDF.initialize(new ObjectInspector[] { inputOI0 });
        Object evaluate = bitmapCountUDF.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(bitmapValue0Bytes) });
        Assert.assertEquals(2L, evaluate);

        bitmapCountUDF.initialize(new ObjectInspector[] { inputOI1 });
        Object evaluate1 = bitmapCountUDF.evaluate(new GenericUDF.DeferredObject[] { new GenericUDF.DeferredJavaObject(bitmapValue1Bytes) });
        Assert.assertEquals(3L, evaluate1);
    }
}
