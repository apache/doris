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

package org.apache.doris.common.property;

import org.apache.doris.thrift.TPropertyVal;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PropertySchemaTest {
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testStringPropNormal() throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertySchema.StringProperty prop = new PropertySchema.StringProperty("key");
        prop.write("val", output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);
        Assert.assertEquals("val", prop.read(input));

        TPropertyVal tProp = new TPropertyVal();
        prop.write("val", tProp);
        Assert.assertEquals("val", prop.read(tProp));

        prop.setMin("b");
        Assert.assertEquals("c", prop.read("c"));
        Assert.assertEquals("b", prop.read("b"));

        prop.setMax("x");
        Assert.assertEquals("w", prop.read("w"));
        Assert.assertEquals("x", prop.read("x"));
    }

    @Test
    public void testStringPropMinExceeded() {
        PropertySchema.StringProperty prop = new PropertySchema.StringProperty("key");
        prop.setMin("b");
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("should not be less than"));
        prop.read("a");
    }

    @Test
    public void testStringPropMinNull() {
        PropertySchema.StringProperty prop = new PropertySchema.StringProperty("key");
        prop.setMin("b");
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("should not be less than"));
        prop.read((String) null);
    }

    @Test
    public void testStringPropMaxExceeded() {
        PropertySchema.StringProperty prop = new PropertySchema.StringProperty("key");
        prop.setMax("b");
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("should not be greater than"));
        prop.read("c");
    }

    @Test
    public void testStringPropMaxNull() {
        PropertySchema.StringProperty prop = new PropertySchema.StringProperty("key");
        prop.setMax("b");
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("should not be greater than"));
        prop.read((String) null);
    }

    @Test
    public void testIntPropNormal() throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertySchema.IntProperty prop = new PropertySchema.IntProperty("key");
        prop.write(5, output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);
        Assert.assertEquals(Integer.valueOf(5), prop.read(input));

        TPropertyVal tProp = new TPropertyVal();
        prop.write(6, tProp);
        Assert.assertEquals(Integer.valueOf(6), prop.read(tProp));

        Assert.assertEquals(Integer.valueOf(7), prop.read("7"));
    }

    @Test
    public void testIntPropInvalidString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid integer"));

        PropertySchema.IntProperty prop = new PropertySchema.IntProperty("key");
        prop.read("23j");
    }

    @Test
    public void testIntPropNullString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid integer"));

        PropertySchema.IntProperty prop = new PropertySchema.IntProperty("key");
        prop.read((String) null);
    }

    @Test
    public void testEnumPropNormal() throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertySchema.EnumProperty<Color> prop = new PropertySchema.EnumProperty<>("key", Color.class);
        prop.write(Color.RED, output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);
        Assert.assertEquals(Color.RED, prop.read(input));

        TPropertyVal tProp = new TPropertyVal();
        prop.write(Color.GREEN, tProp);
        Assert.assertEquals(Color.GREEN, prop.read(tProp));

        Assert.assertEquals(Color.BLUE, prop.read("BLUE"));
        Assert.assertEquals(Color.BLUE, prop.read("blue"));
        Assert.assertEquals(Color.BLUE, prop.read("Blue"));
    }

    @Test
    public void testEnumPropInvalidString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString(
                "Expected values are [RED, GREEN, BLUE], while [invalid] provided"));

        PropertySchema.EnumProperty<Color> prop = new PropertySchema.EnumProperty<>("key", Color.class);
        prop.read("invalid");
    }

    @Test
    public void testEnumPropNullString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString(
                "Expected values are [RED, GREEN, BLUE], while [null] provided"));

        PropertySchema.EnumProperty<Color> prop = new PropertySchema.EnumProperty<>("key", Color.class);
        prop.read((String) null);
    }

    private enum Color {
        RED, GREEN, BLUE
    }

    @Test
    public void testLongPropNormal() throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertySchema.LongProperty prop = new PropertySchema.LongProperty("key");
        prop.write(5L, output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);
        Assert.assertEquals(Long.valueOf(5), prop.read(input));

        TPropertyVal tProp = new TPropertyVal();
        prop.write(6L, tProp);
        Assert.assertEquals(Long.valueOf(6), prop.read(tProp));

        Assert.assertEquals(Long.valueOf(7), prop.read("7"));
    }

    @Test
    public void testLongPropInvalidString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid long"));

        PropertySchema.LongProperty prop = new PropertySchema.LongProperty("key");
        prop.read("23j");
    }

    @Test
    public void testLongPropNullString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid long"));

        PropertySchema.LongProperty prop = new PropertySchema.LongProperty("key");
        prop.read((String) null);
    }

    @Test
    public void testDatePropNormal() throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertySchema.DateProperty prop =
                new PropertySchema.DateProperty("key", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        prop.write(dateFormat.parse("2021-06-30 20:34:51"), output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);
        Assert.assertEquals(1625056491000L, prop.read(input).getTime());

        TPropertyVal tProp = new TPropertyVal();
        prop.write(new Date(1625056491000L), tProp);
        Assert.assertEquals(1625056491000L, prop.read(tProp).getTime());

        Assert.assertEquals(1625056491000L, prop.read("2021-06-30 20:34:51").getTime());
    }

    @Test
    public void testDatePropInvalidString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid time format"));

        PropertySchema.DateProperty prop = new PropertySchema.DateProperty("key", new SimpleDateFormat("yyyy-MM-dd "
                + "HH:mm:ss"));
        prop.read("2021-06-30");
    }

    @Test
    public void testDatePropNullString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid time format"));

        PropertySchema.DateProperty prop = new PropertySchema.DateProperty("key", new SimpleDateFormat("yyyy-MM-dd "
                + "HH:mm:ss"));
        prop.read((String) null);
    }

    @Test
    public void testBooleanPropNormal() throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertySchema.BooleanProperty prop = new PropertySchema.BooleanProperty("key");
        prop.write(true, output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);
        Assert.assertEquals(true, prop.read(input));

        TPropertyVal tProp = new TPropertyVal();
        prop.write(true, tProp);
        Assert.assertEquals(true, prop.read(tProp));

        Assert.assertEquals(true, prop.read("true"));
    }

    @Test
    public void testBooleanPropInvalidString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid boolean"));

        PropertySchema.BooleanProperty prop = new PropertySchema.BooleanProperty("key");
        prop.read("233");
    }

    @Test
    public void testBooleanPropNullString() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage(Matchers.containsString("Invalid boolean"));

        PropertySchema.BooleanProperty prop = new PropertySchema.BooleanProperty("key");
        prop.read((String) null);
    }
}
