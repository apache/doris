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

import org.apache.doris.load.loadv2.dpp.BitmapValue;
import org.apache.doris.common.Codec;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BitmapValueTest {

    @Test
    public void testVarint64IntEncode() throws IOException {
        long[] sourceValue = {0, 1000, Integer.MAX_VALUE, Long.MAX_VALUE};
        for (long value : sourceValue) {
            ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
            DataOutput output = new DataOutputStream(byteArrayOutput);
            Codec.encodeVarint64(value, output);
            assertEquals(value, Codec.decodeVarint64(new DataInputStream(new ByteArrayInputStream(byteArrayOutput.toByteArray()))));
        }
    }

    @Test
    public void testBitmapTypeTransfer() {
        BitmapValue bitmapValue = new BitmapValue();
        Assert.assertTrue(bitmapValue.getBitmapType() == BitmapValue.EMPTY);

        bitmapValue.add(1);
        Assert.assertTrue(bitmapValue.getBitmapType() == BitmapValue.SINGLE_VALUE);

        bitmapValue.add(2);
        Assert.assertTrue(bitmapValue.getBitmapType() == BitmapValue.BITMAP_VALUE);

        bitmapValue.clear();
        Assert.assertTrue(bitmapValue.getBitmapType() == BitmapValue.EMPTY);
    }

    @Test
    public void testBitmapValueAdd() {
        // test add int
        BitmapValue bitmapValue1 = new BitmapValue();
        for (int i = 0; i < 10; i++) {
            bitmapValue1.add(i);
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(bitmapValue1.contains(i));
        }
        Assert.assertFalse(bitmapValue1.contains(11));

        // test add long
        BitmapValue bitmapValue2 = new BitmapValue();
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            bitmapValue2.add(i);
        }
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            Assert.assertTrue(bitmapValue2.contains(i));
        }
        Assert.assertFalse(bitmapValue2.contains(0));

        // test add int and long
        for (int i = 0; i < 10; i++) {
            bitmapValue2.add(i);
        }

        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            Assert.assertTrue(bitmapValue2.contains(i));
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(bitmapValue2.contains(i));
        }
        Assert.assertFalse(bitmapValue2.contains(100));

        // test distinct
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(1);
        bitmapValue.add(1);
        Assert.assertTrue(bitmapValue.getBitmapType() == BitmapValue.SINGLE_VALUE);
        Assert.assertTrue(bitmapValue.cardinality() == 1);
    }

    @Test
    public void testBitmapValueAnd() {
        // empty and empty
        BitmapValue bitmapValue1 = new BitmapValue();
        BitmapValue bitmapValue1_1 = new BitmapValue();
        bitmapValue1.and(bitmapValue1_1);
        Assert.assertTrue(bitmapValue1.getBitmapType() == BitmapValue.EMPTY);
        Assert.assertTrue(bitmapValue1.cardinality() == 0);

        // empty and single value
        BitmapValue bitmapValue2 = new BitmapValue();
        BitmapValue bitmapValue2_1 = new BitmapValue();
        bitmapValue2_1.add(1);
        bitmapValue2.and(bitmapValue2_1);
        Assert.assertTrue(bitmapValue2.getBitmapType() == BitmapValue.EMPTY);
        Assert.assertTrue(bitmapValue2.cardinality() == 0);

        // empty and bitmap
        BitmapValue bitmapValue3 = new BitmapValue();
        BitmapValue bitmapValue3_1 =new BitmapValue();
        bitmapValue3_1.add(1);
        bitmapValue3_1.add(2);
        bitmapValue3.and(bitmapValue3_1);
        Assert.assertTrue(bitmapValue2.getBitmapType() == BitmapValue.EMPTY);
        Assert.assertTrue(bitmapValue3.cardinality() == 0);

        // single value and empty
        BitmapValue bitmapValue4 = new BitmapValue();
        bitmapValue4.add(1);
        BitmapValue bitmapValue4_1 = new BitmapValue();
        bitmapValue4.and(bitmapValue4_1);
        Assert.assertTrue(bitmapValue4.getBitmapType() == BitmapValue.EMPTY);
        Assert.assertTrue(bitmapValue4.cardinality() == 0);

        // single value and single value
        BitmapValue bitmapValue5 = new BitmapValue();
        bitmapValue5.add(1);
        BitmapValue bitmapValue5_1 = new BitmapValue();
        bitmapValue5_1.add(1);
        bitmapValue5.and(bitmapValue5_1);
        Assert.assertTrue(bitmapValue5.getBitmapType() == BitmapValue.SINGLE_VALUE);
        Assert.assertTrue(bitmapValue5.contains(1));

        bitmapValue5.clear();
        bitmapValue5_1.clear();
        bitmapValue5.add(1);
        bitmapValue5_1.add(2);
        bitmapValue5.and(bitmapValue5_1);
        Assert.assertTrue(bitmapValue5.getBitmapType() == BitmapValue.EMPTY);

        // single value and bitmap
        BitmapValue bitmapValue6 = new BitmapValue();
        bitmapValue6.add(1);
        BitmapValue bitmapValue6_1 = new BitmapValue();
        bitmapValue6_1.add(1);
        bitmapValue6_1.add(2);
        bitmapValue6.and(bitmapValue6_1);
        Assert.assertTrue(bitmapValue6.getBitmapType() == BitmapValue.SINGLE_VALUE);

        bitmapValue6.clear();
        bitmapValue6.add(3);
        bitmapValue6.and(bitmapValue6_1);
        Assert.assertTrue(bitmapValue6.getBitmapType() == BitmapValue.EMPTY);

        // bitmap and empty
        BitmapValue bitmapValue7 = new BitmapValue();
        bitmapValue7.add(1);
        bitmapValue7.add(2);
        BitmapValue bitmapValue7_1 = new BitmapValue();
        bitmapValue7.and(bitmapValue7_1);
        Assert.assertTrue(bitmapValue7.getBitmapType() == BitmapValue.EMPTY);

        // bitmap and single value
        BitmapValue bitmapValue8 = new BitmapValue();
        bitmapValue8.add(1);
        bitmapValue8.add(2);
        BitmapValue bitmapValue8_1 = new BitmapValue();
        bitmapValue8_1.add(1);
        bitmapValue8.and(bitmapValue8_1);
        Assert.assertTrue(bitmapValue8.getBitmapType() == BitmapValue.SINGLE_VALUE);

        bitmapValue8.clear();
        bitmapValue8.add(2);
        bitmapValue8.add(3);
        bitmapValue8.and(bitmapValue8_1);
        Assert.assertTrue(bitmapValue8.getBitmapType() == BitmapValue.EMPTY);

        // bitmap and bitmap
        BitmapValue bitmapValue9 = new BitmapValue();
        bitmapValue9.add(1);
        bitmapValue9.add(2);
        BitmapValue bitmapValue9_1 = new BitmapValue();
        bitmapValue9_1.add(2);
        bitmapValue9_1.add(3);
        bitmapValue9.and(bitmapValue9_1);
        Assert.assertTrue(bitmapValue9.getBitmapType() == BitmapValue.SINGLE_VALUE);

        bitmapValue9.clear();
        bitmapValue9.add(4);
        bitmapValue9.add(5);
        bitmapValue9.and(bitmapValue9_1);
        Assert.assertTrue(bitmapValue9.getBitmapType() == BitmapValue.EMPTY);

        bitmapValue9.clear();
        bitmapValue9.add(2);
        bitmapValue9.add(3);
        bitmapValue9.add(4);
        bitmapValue9.and(bitmapValue9_1);
        Assert.assertTrue(bitmapValue9.getBitmapType() == BitmapValue.BITMAP_VALUE);
        Assert.assertTrue(bitmapValue9.equals(bitmapValue9_1));

    }

    @Test
    public void testBitmapValueOr() {
        // empty or empty
        BitmapValue bitmapValue1 = new BitmapValue();
        BitmapValue bitmapValue1_1 = new BitmapValue();
        bitmapValue1.or(bitmapValue1_1);
        Assert.assertTrue(bitmapValue1.getBitmapType() == BitmapValue.EMPTY);

        // empty or single value
        BitmapValue bitmapValue2 = new BitmapValue();
        BitmapValue bitmapValue2_1 = new BitmapValue();
        bitmapValue2_1.add(1);
        bitmapValue2.or(bitmapValue2_1);
        Assert.assertTrue(bitmapValue2.getBitmapType() == BitmapValue.SINGLE_VALUE);

        // empty or bitmap
        BitmapValue bitmapValue3 = new BitmapValue();
        BitmapValue bitmapValue3_1 = new BitmapValue();
        bitmapValue3_1.add(1);
        bitmapValue3_1.add(2);
        bitmapValue3.or(bitmapValue3_1);
        Assert.assertTrue(bitmapValue3.getBitmapType() == BitmapValue.BITMAP_VALUE);

        // single or and empty
        BitmapValue bitmapValue4 = new BitmapValue();
        BitmapValue bitmapValue4_1 = new BitmapValue();
        bitmapValue4.add(1);
        bitmapValue4.or(bitmapValue4_1);
        Assert.assertTrue(bitmapValue4.getBitmapType() == BitmapValue.SINGLE_VALUE);

        // single or and single value
        BitmapValue bitmapValue5 = new BitmapValue();
        BitmapValue bitmapValue5_1 = new BitmapValue();
        bitmapValue5.add(1);
        bitmapValue5_1.add(1);
        bitmapValue5.or(bitmapValue5_1);
        Assert.assertTrue(bitmapValue5.getBitmapType() == BitmapValue.SINGLE_VALUE);

        bitmapValue5.clear();
        bitmapValue5.add(2);
        bitmapValue5.or(bitmapValue5_1);
        Assert.assertTrue(bitmapValue5.getBitmapType() == BitmapValue.BITMAP_VALUE);

        // single or and bitmap
        BitmapValue bitmapValue6 = new BitmapValue();
        BitmapValue bitmapValue6_1 = new BitmapValue();
        bitmapValue6.add(1);
        bitmapValue6_1.add(1);
        bitmapValue6_1.add(2);
        bitmapValue6.or(bitmapValue6_1);
        Assert.assertTrue(bitmapValue6.getBitmapType() == BitmapValue.BITMAP_VALUE);

        // bitmap or empty
        BitmapValue bitmapValue7 = new BitmapValue();
        bitmapValue7.add(1);
        bitmapValue7.add(2);
        BitmapValue bitmapValue7_1 =new BitmapValue();
        bitmapValue7.or(bitmapValue7_1);
        Assert.assertTrue(bitmapValue7.getBitmapType() == BitmapValue.BITMAP_VALUE);

        // bitmap or single value
        BitmapValue bitmapValue8 = new BitmapValue();
        bitmapValue8.add(1);
        bitmapValue8.add(2);
        BitmapValue bitmapValue8_1 =new BitmapValue();
        bitmapValue8_1.add(1);
        bitmapValue8.or(bitmapValue8_1);
        Assert.assertTrue(bitmapValue8.getBitmapType() == BitmapValue.BITMAP_VALUE);

        // bitmap or bitmap
        BitmapValue bitmapValue9 = new BitmapValue();
        bitmapValue9.add(1);
        bitmapValue9.add(2);
        BitmapValue bitmapValue9_1 =new BitmapValue();
        bitmapValue9.or(bitmapValue9_1);
        Assert.assertTrue(bitmapValue9.getBitmapType() == BitmapValue.BITMAP_VALUE);
    }

    @Test
    public void testBitmapValueSerializeAndDeserialize() throws IOException {
        // empty
        BitmapValue serializeBitmapValue = new BitmapValue();
        ByteArrayOutputStream emptyOutputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(emptyOutputStream);
        serializeBitmapValue.serialize(output);

        DataInputStream emptyInputStream = new DataInputStream(new ByteArrayInputStream(emptyOutputStream.toByteArray()));
        BitmapValue deserializeBitmapValue = new BitmapValue();
        deserializeBitmapValue.deserialize(emptyInputStream);

        Assert.assertTrue(serializeBitmapValue.equals(deserializeBitmapValue));

        // single value
        BitmapValue serializeSingleValueBitmapValue = new BitmapValue();
        // unsigned 32-bit
        long unsigned32bit = Integer.MAX_VALUE;
        serializeSingleValueBitmapValue.add(unsigned32bit + 1);
        ByteArrayOutputStream singleValueOutputStream = new ByteArrayOutputStream();
        DataOutput singleValueOutput = new DataOutputStream(singleValueOutputStream);
        serializeSingleValueBitmapValue.serialize(singleValueOutput);

        DataInputStream singleValueInputStream = new DataInputStream(new ByteArrayInputStream(singleValueOutputStream.toByteArray()));
        BitmapValue deserializeSingleValueBitmapValue = new BitmapValue();
        deserializeSingleValueBitmapValue.deserialize(singleValueInputStream);

        Assert.assertTrue(serializeSingleValueBitmapValue.equals(deserializeSingleValueBitmapValue));

        // bitmap
        BitmapValue serializeBitmapBitmapValue = new BitmapValue();
        for (int i = 0; i < 10; i++) {
            serializeBitmapBitmapValue.add(i);
        }
        for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 10; i--) {
            serializeBitmapBitmapValue.add(i);
        }
        ByteArrayOutputStream bitmapOutputStream = new ByteArrayOutputStream();
        DataOutput bitmapOutput = new DataOutputStream(bitmapOutputStream);
        serializeBitmapBitmapValue.serialize(bitmapOutput);

        DataInputStream bitmapInputStream = new DataInputStream(new ByteArrayInputStream(bitmapOutputStream.toByteArray()));
        BitmapValue deserializeBitmapBitmapValue = new BitmapValue();
        deserializeBitmapBitmapValue.deserialize(bitmapInputStream);

        Assert.assertTrue(serializeBitmapBitmapValue.equals(deserializeBitmapBitmapValue));
    }

    @Test
    public void testIs32BitsEnough() {
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(0);
        bitmapValue.add(2);
        bitmapValue.add(Integer.MAX_VALUE);
        // unsigned 32-bit
        long unsigned32bit = Integer.MAX_VALUE;
        bitmapValue.add(unsigned32bit + 1);

        Assert.assertTrue(bitmapValue.is32BitsEnough());

        bitmapValue.add(Long.MAX_VALUE);
        Assert.assertFalse(bitmapValue.is32BitsEnough());
    }

    @Test
    public void testCardinality() {
        BitmapValue bitmapValue = new BitmapValue();

        Assert.assertTrue(bitmapValue.cardinality() == 0);

        bitmapValue.add(0);
        bitmapValue.add(0);
        bitmapValue.add(-1);
        bitmapValue.add(-1);
        bitmapValue.add(Integer.MAX_VALUE);
        bitmapValue.add(Integer.MAX_VALUE);
        bitmapValue.add(-Integer.MAX_VALUE);
        bitmapValue.add(-Integer.MAX_VALUE);
        bitmapValue.add(Long.MAX_VALUE);
        bitmapValue.add(Long.MAX_VALUE);
        bitmapValue.add(-Long.MAX_VALUE);
        bitmapValue.add(-Long.MAX_VALUE);

        Assert.assertTrue(bitmapValue.cardinality() == 6);
    }

    @Test
    public void testContains() {
        // empty
        BitmapValue bitmapValue = new BitmapValue();
        Assert.assertFalse(bitmapValue.contains(1));

        // single value
        bitmapValue.add(1);
        Assert.assertTrue(bitmapValue.contains(1));
        Assert.assertFalse(bitmapValue.contains(2));

        // bitmap
        bitmapValue.add(2);
        Assert.assertTrue(bitmapValue.contains(1));
        Assert.assertTrue(bitmapValue.contains(2));
        Assert.assertFalse(bitmapValue.contains(12));
    }

    @Test
    public void testEqual() {
        // empty == empty
        BitmapValue emp1 = new BitmapValue();
        BitmapValue emp2 = new BitmapValue();
        Assert.assertTrue(emp1.equals(emp2));
        // empty == single value
        emp2.add(1);
        Assert.assertFalse(emp1.equals(emp2));
        // empty == bitmap
        emp2.add(2);
        Assert.assertFalse(emp1.equals(emp2));

        // single value = empty
        BitmapValue sgv = new BitmapValue();
        sgv.add(1);
        BitmapValue emp3 = new BitmapValue();
        Assert.assertFalse(sgv.equals(emp3));
        // single value = single value
        BitmapValue sgv1 = new BitmapValue();
        sgv1.add(1);
        BitmapValue sgv2 = new BitmapValue();
        sgv2.add(2);
        Assert.assertTrue(sgv.equals(sgv1));
        Assert.assertFalse(sgv.equals(sgv2));
        // single value = bitmap
        sgv2.add(3);
        Assert.assertFalse(sgv.equals(sgv2));

        // bitmap == empty
        BitmapValue bitmapValue = new BitmapValue();
        bitmapValue.add(1);
        bitmapValue.add(2);
        BitmapValue emp4 = new BitmapValue();
        Assert.assertFalse(bitmapValue.equals(emp4));
        // bitmap == singlevalue
        BitmapValue sgv3 = new BitmapValue();
        sgv3.add(1);
        Assert.assertFalse(bitmapValue.equals(sgv3));
        // bitmap == bitmap
        BitmapValue bitmapValue1 = new BitmapValue();
        bitmapValue1.add(1);
        BitmapValue bitmapValue2 = new BitmapValue();
        bitmapValue2.add(1);
        bitmapValue2.add(2);
        Assert.assertTrue(bitmapValue.equals(bitmapValue2));
        Assert.assertFalse(bitmapValue.equals(bitmapValue1));
    }

    @Test
    public void testToString() {
        BitmapValue empty = new BitmapValue();
        Assert.assertTrue(empty.toString().equals("{}"));

        BitmapValue singleValue = new BitmapValue();
        singleValue.add(1);
        Assert.assertTrue(singleValue.toString().equals("{1}"));


        BitmapValue bitmap = new BitmapValue();
        bitmap.add(1);
        bitmap.add(2);
        Assert.assertTrue(bitmap.toString().equals("{1,2}"));
    }

}
