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

public class TestOrcUnion {

  @Test
  public void testRead() throws IOException {
    TypeDescription type =
        TypeDescription.fromString("uniontype<int,bigint,string>");
    OrcUnion expected = new OrcUnion(type);
    OrcUnion actual = new OrcUnion(type);
    expected.set((byte) 2, new Text("foo"));
    assertEquals(131367, expected.hashCode());
    assertNotEquals(expected, actual);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.set((byte) 0, new IntWritable(111));
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.set((byte)1, new LongWritable(4567));
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.set((byte) 1, new LongWritable(12345));
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.set((byte) 1, null);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testCompare() {
    TypeDescription schema =
        TypeDescription.fromString("uniontype<int,string,bigint>");
    OrcUnion left = new OrcUnion(schema);
    OrcUnion right = new OrcUnion(schema);
    assertEquals(-1 ,left.compareTo(null));
    assertEquals(0, left.compareTo(right));

    left.set(1, new IntWritable(10));
    right.set(1, new IntWritable(12));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));

    right.set(2, new Text("a"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
  }

  @Test
  public void testSchemaInCompare() {
    TypeDescription leftType = TypeDescription.fromString("uniontype<string,tinyint>");
    TypeDescription rightType = TypeDescription.fromString("uniontype<string,bigint>");
    OrcUnion left = new OrcUnion(leftType);
    OrcUnion right = new OrcUnion(rightType);
    assertEquals(-3, left.compareTo(right));
    assertEquals(3, right.compareTo(left));
    left.set(0, new Text("123"));
    right.set(0, new Text("1"));
    assertEquals(-3, left.compareTo(right));
    assertEquals(3, right.compareTo(left));
  }
}
