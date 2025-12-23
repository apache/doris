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

package org.apache.orc.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestOrcMap {

  @Test
  public void testRead() throws IOException {
    TypeDescription type =
        TypeDescription.createMap(TypeDescription.createInt(),
            TypeDescription.createLong());
    OrcMap<IntWritable, LongWritable> expected = new OrcMap<>(type);
    OrcMap<IntWritable, LongWritable> actual = new OrcMap<>(type);
    expected.put(new IntWritable(999), new LongWritable(1111));
    expected.put(new IntWritable(888), new LongWritable(2222));
    expected.put(new IntWritable(777), new LongWritable(3333));
    assertNotEquals(expected, actual);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.clear();
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.put(new IntWritable(666), null);
    expected.put(new IntWritable(1), new LongWritable(777));
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testCompare() {
    TypeDescription schema = TypeDescription.fromString("map<string,string>");
    OrcMap<Text,Text> left = new OrcMap<>(schema);
    assertEquals(-1 ,left.compareTo(null));
    OrcMap<Text,Text> right = new OrcMap<>(schema);

    // empty maps
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));

    // {} vs {"aa" -> null}
    right.put(new Text("aa"), null);
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));

    // {"aa" -> null} vs {"aa" -> null}
    left.put(new Text("aa"), null);
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));

    // {"aa" -> "bb"} vs {"aa" -> "bb"}
    left.put(new Text("aa"), new Text("bb"));
    right.put(new Text("aa"), new Text("bb"));
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));

    // {"aa" -> "bb"} vs {"aa" -> "cc"}
    right.put(new Text("aa"), new Text("cc"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));

    // {"aa" -> "bb"} vs {"a" -> "zzz", "aa" -> "cc"}
    right.put(new Text("a"), new Text("zzz"));
    assertEquals(1, left.compareTo(right));
    assertEquals(-1, right.compareTo(left));

    // {"aa" -> null} vs {"aa" -> "bb"}
    left.put(new Text("aa"), null);
    right.remove(new Text("a"));
    right.put(new Text("aa"), new Text("cc"));
    assertEquals(1, left.compareTo(right));
    assertEquals(-1, right.compareTo(left));

    // {"aa" -> null, "bb" -> "cc"} vs {"aa" -> null, "bb" -> "dd"}
    left.put(new Text("aa"), null);
    left.put(new Text("bb"), new Text("cc"));
    right.put(new Text("aa"), null);
    right.put(new Text("bb"), new Text("dd"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
  }

  @Test
  public void testStructKeys() {
    TypeDescription schema = TypeDescription.fromString("map<struct<i:int>,string>");
    OrcMap<OrcStruct, Text> map = new OrcMap<>(schema);
    OrcStruct struct = new OrcStruct(schema.getChildren().get(0));
    struct.setFieldValue(0, new IntWritable(12));
    map.put(struct, new Text("a"));
    assertEquals("a", map.get(struct).toString());
    struct = new OrcStruct(schema.getChildren().get(0));
    struct.setFieldValue(0, new IntWritable(14));
    map.put(struct, new Text("b"));
    assertEquals(2, map.size());
  }

  @Test
  public void testListKeys() {
    TypeDescription schema = TypeDescription.fromString("map<array<int>,string>");
    OrcMap<OrcList, Text> map = new OrcMap<>(schema);
    OrcList<IntWritable> list = new OrcList<>(schema.getChildren().get(0));
    list.add(new IntWritable(123));
    map.put(list, new Text("a"));
    assertEquals("a", map.get(list).toString());
    list = new OrcList<>(schema.getChildren().get(0));
    list.add(new IntWritable(333));
    map.put(list, new Text("b"));
    assertEquals(2, map.size());
    assertEquals("b", map.get(list).toString());
  }

  @Test
  public void testUnionKeys() {
    TypeDescription schema = TypeDescription.fromString("map<uniontype<int,string>,string>");
    OrcMap<OrcUnion, Text> map = new OrcMap<>(schema);
    OrcUnion un = new OrcUnion(schema.getChildren().get(0));
    un.set(0, new IntWritable(123));
    map.put(un, new Text("hi"));
    un = new OrcUnion(schema.getChildren().get(0));
    un.set(1, new Text("aaaa"));
    map.put(un, new Text("google"));
    assertEquals(2, map.size());
    assertEquals("google", map.get(un).toString());
  }

  @Test
  public void testMapKeys() {
    TypeDescription schema = TypeDescription.fromString("map<map<string,string>,string>");
    OrcMap<OrcMap<Text,Text>, Text> left = new OrcMap<>(schema);

    assertEquals(-1, left.compareTo(null));

    OrcMap<OrcMap<Text,Text>, Text> right = new OrcMap<>(schema);
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));

    OrcMap<Text,Text> item = new OrcMap<>(schema.getChildren().get(0));
    item.put(new Text("aa"), new Text("bb"));
    left.put(item, new Text("cc"));
    assertEquals(1, left.compareTo(right));
    assertEquals(-1, right.compareTo(left));

    item =  new OrcMap<>(schema.getChildren().get(0));
    item.put(new Text("aa"), new Text("dd"));
    right.put(item, new Text("bb"));
    assertEquals(-2, left.compareTo(right));
    assertEquals(2, right.compareTo(left));
  }

  @Test
  public void testSchemaInCompare() {
    TypeDescription leftType = TypeDescription.fromString("map<string,int>");
    TypeDescription rightType = TypeDescription.fromString("map<string,string>");
    OrcMap left = new OrcMap(leftType);
    OrcMap right = new OrcMap(rightType);
    assertEquals(-4, left.compareTo(right));
    assertEquals(4, right.compareTo(left));
    left.put(new Text("123"), new IntWritable(123));
    right.put(new Text("123"), new Text("123"));
    assertEquals(-4, left.compareTo(right));
    assertEquals(4, right.compareTo(left));
  }
}
