/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.tools.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.LazilyParsedNumber;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJsonSchemaFinder {

  @Test
  public void testBinaryPatterns() throws Exception {
    assertEquals("binary",
        JsonSchemaFinder.pickType(new JsonPrimitive("00000000")).toString());
    assertEquals("string",
        JsonSchemaFinder.pickType(new JsonPrimitive("0000000")).toString());
    assertEquals("string",
        JsonSchemaFinder.pickType(new JsonPrimitive("")).toString());
    assertEquals("binary",
        JsonSchemaFinder.pickType(new JsonPrimitive("0123456789abcdefABCDEF")).toString());
    assertEquals("string",
        JsonSchemaFinder.pickType(new JsonPrimitive("00x0")).toString());
  }

  @Test
  public void testTimestampPatterns() throws Exception {
    assertEquals("timestamp",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016-01-05T12:34:56Z")).toString());
    assertEquals("timestamp",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016/01/05 12:34:56")).toString());
    assertEquals("string",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016/01/05")).toString());
    assertEquals("timestamp",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016-01-01 16:00:00 +08")).toString());
    assertEquals("timestamp",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016-01-01 16:00:00+08")).toString());
    assertEquals("string",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016-01-01 16:00:0008")).toString());
    assertEquals("timestamp",
        JsonSchemaFinder.pickType(new JsonPrimitive("2016-01-01 06:00:00 -08:30")).toString());
    assertEquals("timestamp",
        JsonSchemaFinder.pickType(new JsonPrimitive("2017-05-31T12:44:40-04:00")).toString());
  }

  @Test
  public void testBooleans() throws Exception {
    assertEquals("boolean",
        JsonSchemaFinder.pickType(new JsonPrimitive(true)).toString());
    assertEquals("void",
        JsonSchemaFinder.pickType(JsonNull.INSTANCE).toString());
    assertEquals("boolean",
        JsonSchemaFinder.pickType(new JsonPrimitive(false)).toString());
  }

  @Test
  public void testNumbers() throws Exception {
    assertEquals("tinyint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("120"))).toString());
    assertEquals("tinyint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("-128"))).toString());
    assertEquals("smallint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("-24120"))).toString());
    assertEquals("smallint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("128"))).toString());
    assertEquals("int",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("60000"))).toString());
    assertEquals("bigint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("-4294967296"))).toString());
    assertEquals("bigint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("-9223372036854775808"))).toString());
    assertEquals("bigint",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("9223372036854775807"))).toString());
    assertEquals("decimal(19,0)",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("9223372036854775808"))).toString());
    assertEquals("decimal(19,0)",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("-9223372036854775809"))).toString());
    assertEquals("decimal(10,6)",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("1234.567890"))).toString());
    assertEquals("decimal(20,10)",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("-1234567890.1234567890"))).toString());
    assertEquals("float",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("1.2e9"))).toString());
    assertEquals("double",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("1234567890123456789012345678901234567890"))).toString());
    assertEquals("double",
        JsonSchemaFinder.pickType(new JsonPrimitive
            (new LazilyParsedNumber("1.2E40"))).toString());
    // Make the schema
    assertEquals("decimal(3,2)",
        JsonSchemaFinder.pickType(new JsonPrimitive(
            new LazilyParsedNumber("1.23"))).getSchema().toString());
  }

  @Test
  public void testLists() throws Exception {
    assertEquals("list<void>",
        JsonSchemaFinder.pickType(new JsonArray()).toString());
    JsonArray list = new JsonArray();
    list.add(new JsonPrimitive(50000));
    assertEquals("list<int>", JsonSchemaFinder.pickType(list).toString());
    list = new JsonArray();
    list.add(new JsonPrimitive(127));
    list.add(new JsonPrimitive(50000));
    list.add(new JsonPrimitive(50000000000L));
    list.add(new JsonPrimitive(-100));
    assertEquals("list<bigint>", JsonSchemaFinder.pickType(list).toString());
  }

  @Test
  public void testStructs() throws Exception {
    assertEquals("struct<>",
        JsonSchemaFinder.pickType(new JsonObject()).toString());
    JsonObject struct = new JsonObject();
    struct.addProperty("bool", true);
    assertEquals("struct<bool:boolean>",
        JsonSchemaFinder.pickType(struct).toString());
    struct = new JsonObject();
    struct.addProperty("str", "value");
    struct.addProperty("i", new LazilyParsedNumber("124567"));
    assertEquals("struct<i:int,str:string>",
        JsonSchemaFinder.pickType(struct).toString());
  }

  @Test
  public void testNullMerges() throws Exception {
    assertEquals("void", JsonSchemaFinder.mergeType(
        new NullType(),
        new NullType()).toString());
    assertEquals("boolean", JsonSchemaFinder.mergeType(
        new BooleanType(),
        new NullType()).toString());
    assertEquals("int", JsonSchemaFinder.mergeType(
        new NullType(),
        new NumericType(HiveType.Kind.INT, 4, 0)
        ).toString());
    assertEquals("string", JsonSchemaFinder.mergeType(
        new NullType(),
        new StringType(HiveType.Kind.STRING)
        ).toString());
    assertEquals("struct<i:int>", JsonSchemaFinder.mergeType(
        new StructType().addField("i", new NumericType(HiveType.Kind.INT, 5, 0)),
        new NullType()
        ).toString());
    assertEquals("list<int>", JsonSchemaFinder.mergeType(
        new ListType(new NumericType(HiveType.Kind.INT, 5, 0)),
        new NullType()
        ).toString());
    assertEquals("uniontype<int>", JsonSchemaFinder.mergeType(
        new UnionType().addType(new NumericType(HiveType.Kind.INT, 5, 0)),
        new NullType()
        ).toString());
  }

  @Test
  public void testBooleanMerges() throws Exception {
    assertEquals("boolean", JsonSchemaFinder.mergeType(
        new BooleanType(),
        new BooleanType()).toString());
    assertEquals("uniontype<boolean,int>", JsonSchemaFinder.mergeType(
        new BooleanType(),
        new NumericType(HiveType.Kind.INT, 4, 0)
        ).toString());
    assertEquals("uniontype<boolean,string>", JsonSchemaFinder.mergeType(
        new BooleanType(),
        new StringType(HiveType.Kind.STRING)
        ).toString());
    assertEquals("uniontype<struct<i:int>,boolean>", JsonSchemaFinder.mergeType(
        new StructType().addField("i", new NumericType(HiveType.Kind.INT, 5, 0)),
        new BooleanType()
        ).toString());
    assertEquals("uniontype<list<int>,boolean>", JsonSchemaFinder.mergeType(
        new ListType(new NumericType(HiveType.Kind.INT, 5, 0)),
        new BooleanType()
        ).toString());
    assertEquals("uniontype<int,boolean>", JsonSchemaFinder.mergeType(
        new UnionType().addType(new NumericType(HiveType.Kind.INT, 5, 0)),
        new BooleanType()
        ).toString());
  }

  @Test
  public void testNumericMerges() throws Exception {
    assertEquals("smallint", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.BYTE, 2, 0),
        new NumericType(HiveType.Kind.SHORT, 4, 0)
        ).toString());
    assertEquals("int", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.INT, 6, 0),
        new NumericType(HiveType.Kind.SHORT, 4, 0)
        ).toString());
    assertEquals("bigint", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.INT, 6, 0),
        new NumericType(HiveType.Kind.LONG, 10, 0)
        ).toString());
    assertEquals("decimal(20,0)", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.SHORT, 4, 0),
        new NumericType(HiveType.Kind.DECIMAL, 20, 0)
        ).toString());
    assertEquals("float", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.FLOAT, 21, 4),
        new NumericType(HiveType.Kind.DECIMAL, 20, 0)
        ).toString());
    assertEquals("double", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.DOUBLE, 31, 4),
        new NumericType(HiveType.Kind.DECIMAL, 20, 10)
        ).toString());
    assertEquals("uniontype<decimal(30,10),string>", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.DECIMAL, 20, 10),
        new StringType(HiveType.Kind.STRING)
        ).toString());
    assertEquals("uniontype<struct<i:int>,smallint>", JsonSchemaFinder.mergeType(
        new StructType().addField("i", new NumericType(HiveType.Kind.INT, 5, 0)),
        new NumericType(HiveType.Kind.SHORT, 4, 0)
        ).toString());
    assertEquals("uniontype<smallint,list<int>>", JsonSchemaFinder.mergeType(
        new NumericType(HiveType.Kind.SHORT, 4, 0),
        new ListType(new NumericType(HiveType.Kind.INT, 5, 0))
        ).toString());
    assertEquals("uniontype<decimal(20,0),string>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.INT, 5, 0))
            .addType(new StringType(HiveType.Kind.STRING)),
        new NumericType(HiveType.Kind.DECIMAL, 20, 0)
        ).toString());
  }

  @Test
  public void testStringMerges() throws Exception {
    assertEquals("string", JsonSchemaFinder.mergeType(
        new StringType(HiveType.Kind.BINARY),
        new StringType(HiveType.Kind.STRING)
        ).toString());
    assertEquals("string", JsonSchemaFinder.mergeType(
        new StringType(HiveType.Kind.STRING),
        new StringType(HiveType.Kind.TIMESTAMP)
        ).toString());
    assertEquals("uniontype<struct<i:int>,timestamp>", JsonSchemaFinder.mergeType(
        new StructType().addField("i", new NumericType(HiveType.Kind.INT, 5, 0)),
        new StringType(HiveType.Kind.TIMESTAMP)
        ).toString());
    assertEquals("uniontype<binary,list<int>>", JsonSchemaFinder.mergeType(
        new StringType(HiveType.Kind.BINARY),
        new ListType(new NumericType(HiveType.Kind.INT, 5, 0))
        ).toString());
    assertEquals("uniontype<int,string>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.INT, 5, 0))
            .addType(new StringType(HiveType.Kind.STRING)),
        new StringType(HiveType.Kind.TIMESTAMP)
        ).toString());
  }

  @Test
  public void testListMerges() throws Exception {
    assertEquals("list<bigint>", JsonSchemaFinder.mergeType(
        new ListType(new NumericType(HiveType.Kind.INT, 10, 0)),
        new ListType(new NumericType(HiveType.Kind.LONG, 20, 0))
        ).toString());
    assertEquals("list<uniontype<int,string>>", JsonSchemaFinder.mergeType(
        new ListType(new NumericType(HiveType.Kind.INT, 10, 0)),
        new ListType(new StringType(HiveType.Kind.STRING))
        ).toString());
    assertEquals("uniontype<struct<foo:int>,list<int>>", JsonSchemaFinder.mergeType(
        new StructType().addField("foo", new NumericType(HiveType.Kind.INT, 10, 0)),
        new ListType(new NumericType(HiveType.Kind.INT, 5, 0))
        ).toString());
    assertEquals("uniontype<int,string,list<boolean>>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.INT, 5, 0))
            .addType(new StringType(HiveType.Kind.STRING)),
        new ListType(new BooleanType())
        ).toString());
  }

  @Test
  public void testStructMerges() throws Exception {
    assertEquals("struct<bar:timestamp,foo:int>", JsonSchemaFinder.mergeType(
        new StructType().addField("foo", new NumericType(HiveType.Kind.INT, 10, 0)),
        new StructType().addField("bar", new StringType(HiveType.Kind.TIMESTAMP))
        ).toString());
    assertEquals("struct<bar:string,foo:int>", JsonSchemaFinder.mergeType(
        new StructType()
            .addField("foo", new NumericType(HiveType.Kind.INT, 10, 0))
            .addField("bar", new StringType(HiveType.Kind.BINARY)),
        new StructType()
            .addField("bar", new StringType(HiveType.Kind.TIMESTAMP))
        ).toString());
    assertEquals("uniontype<int,string,struct<foo:boolean>>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.INT, 5, 0))
            .addType(new StringType(HiveType.Kind.STRING)),
        new StructType().addField("foo", new BooleanType())
        ).toString());
  }

  @Test
  public void testUnionMerges() throws Exception {
    assertEquals("uniontype<decimal(15,10),boolean,string>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.DECIMAL, 2, 10))
            .addType(new BooleanType())
            .addType(new StringType(HiveType.Kind.BINARY)),
        new UnionType()
            .addType(new StringType(HiveType.Kind.TIMESTAMP))
            .addType(new NumericType(HiveType.Kind.INT, 5, 0))
        ).toString());
    assertEquals("uniontype<int,binary,struct<bar:timestamp>>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.INT, 10, 0))
            .addType(new StringType(HiveType.Kind.BINARY)),
        new StructType()
            .addField("bar", new StringType(HiveType.Kind.TIMESTAMP))
    ).toString());
    assertEquals("uniontype<int,string>", JsonSchemaFinder.mergeType(
        new UnionType()
            .addType(new NumericType(HiveType.Kind.INT, 5, 0))
            .addType(new StringType(HiveType.Kind.BINARY)),
        new StringType(HiveType.Kind.TIMESTAMP)
        ).toString());
  }

  @Test
  public void testMapMerges() throws Exception {
    assertEquals("map<decimal(15,10),string>", JsonSchemaFinder.mergeType(
        new MapType(new NumericType(HiveType.Kind.DECIMAL, 2, 10),
            new StringType(HiveType.Kind.TIMESTAMP)),
        new MapType(new NumericType(HiveType.Kind.INT, 5, 0),
            new StringType(HiveType.Kind.BINARY))
    ).toString());
    assertEquals("map<binary,timestamp>", JsonSchemaFinder.mergeType(
        new MapType(new StringType(HiveType.Kind.BINARY), new StringType(HiveType.Kind.TIMESTAMP)),
        new MapType(new StringType(HiveType.Kind.BINARY), new StringType(HiveType.Kind.TIMESTAMP))
    ).toString());
    assertEquals("map<string,string>", JsonSchemaFinder.mergeType(
        new MapType(new StringType(HiveType.Kind.BINARY), new StringType(HiveType.Kind.TIMESTAMP)),
        new MapType(new StringType(HiveType.Kind.TIMESTAMP), new StringType(HiveType.Kind.BINARY))
    ).toString());
    assertEquals("struct<bar:map<struct<i:decimal(15,10),j:string>,struct<k:boolean>>,foo:int>",
        JsonSchemaFinder.mergeType(
            new StructType()
                .addField("bar", new MapType(
                    new StructType()
                        .addField("i", new NumericType(HiveType.Kind.INT, 5, 0))
                        .addField("j", new StringType(HiveType.Kind.BINARY)),
                    new StructType()
                        .addField("k", new BooleanType())))
                .addField("foo", new NumericType(HiveType.Kind.INT, 5, 0)),
            new StructType()
                .addField("bar", new MapType(
                    new StructType()
                        .addField("i", new NumericType(HiveType.Kind.DECIMAL, 2, 10))
                        .addField("j", new StringType(HiveType.Kind.TIMESTAMP)),
                    new StructType()
                        .addField("k", new BooleanType())))
                .addField("foo", new NumericType(HiveType.Kind.INT, 5, 0))
    ).toString());
  }

}
