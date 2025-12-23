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

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestOrcStruct {

  @Test
  public void testRead() throws IOException {
    TypeDescription type =
        TypeDescription.createStruct()
          .addField("f1", TypeDescription.createInt())
          .addField("f2", TypeDescription.createLong())
          .addField("f3", TypeDescription.createString());
    OrcStruct expected = new OrcStruct(type);
    OrcStruct actual = new OrcStruct(type);
    assertEquals(3, expected.getNumFields());
    expected.setFieldValue(0, new IntWritable(1));
    expected.setFieldValue(1, new LongWritable(2));
    expected.setFieldValue(2, new Text("wow"));
    assertEquals(178524, expected.hashCode());
    assertNotEquals(expected, actual);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.setFieldValue(0, null);
    expected.setFieldValue(1, null);
    expected.setFieldValue(2, null);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    assertEquals(29791, expected.hashCode());
    expected.setFieldValue(1, new LongWritable(111));
    assertEquals(111, ((LongWritable) expected.getFieldValue(1)).get());
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testMapredRead() throws Exception {
    TypeDescription internalStruct_0 = TypeDescription.createStruct()
        .addField("field0", TypeDescription.createString())
        .addField("field1", TypeDescription.createBoolean());
    TypeDescription internalStruct_1 = TypeDescription.createStruct();
    TypeDescription internalStruct_2 = TypeDescription.createStruct().addField("f0", TypeDescription.createInt());

    TypeDescription unionWithMultipleStruct = TypeDescription.createUnion()
        .addUnionChild(internalStruct_0)
        .addUnionChild(internalStruct_1)
        .addUnionChild(internalStruct_2);

    OrcStruct o1 = new OrcStruct(internalStruct_0);
    o1.setFieldValue("field0", new Text("key"));
    o1.setFieldValue("field1", new BooleanWritable(true));

    OrcStruct o2 = new OrcStruct(internalStruct_0);
    o2.setFieldValue("field0", new Text("key_1"));
    o2.setFieldValue("field1", new BooleanWritable(false));

    OrcStruct o3 = new OrcStruct(TypeDescription.createStruct());

    OrcStruct o4 = new OrcStruct(internalStruct_2);
    o4.setFieldValue("f0", new IntWritable(1));

    OrcUnion u1 = new OrcUnion(unionWithMultipleStruct);
    u1.set(0, o1);
    OrcUnion u2 = new OrcUnion(unionWithMultipleStruct);
    u2.set(0, o2);
    OrcUnion u3 = new OrcUnion(unionWithMultipleStruct);
    u3.set(1, o3);
    OrcUnion u4 = new OrcUnion(unionWithMultipleStruct);
    u4.set(2, o4);

    File testFolder = Files.createTempDir();
    testFolder.deleteOnExit();
    Path testFilePath = new Path(testFolder.getAbsolutePath(), "testFile");
    Configuration conf = new Configuration();

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(unionWithMultipleStruct)
            .stripeSize(100000).bufferSize(10000)
            .version(OrcFile.Version.CURRENT));

    OrcMapredRecordWriter<OrcUnion> recordWriter =
        new OrcMapredRecordWriter<>(writer);
    recordWriter.write(NullWritable.get(), u1);
    recordWriter.write(NullWritable.get(), u2);
    recordWriter.write(NullWritable.get(), u3);
    recordWriter.write(NullWritable.get(), u4);
    recordWriter.close(null);

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(FileSystem.getLocal(conf)));
    Reader.Options options = reader.options().schema(unionWithMultipleStruct);

    OrcMapredRecordReader<OrcUnion> recordReader = new OrcMapredRecordReader<>(reader,options);
    OrcUnion result = recordReader.createValue();
    recordReader.next(recordReader.createKey(), result);
    assertEquals(result, u1);
    recordReader.next(recordReader.createKey(), result);
    assertEquals(result, u2);
    recordReader.next(recordReader.createKey(), result);
    assertEquals(result, u3);
    recordReader.next(recordReader.createKey(), result);
    assertEquals(result, u4);
  }

  @Test
  public void testFieldAccess() {
    OrcStruct struct = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:double,k:string>"));
    struct.setFieldValue("j", new DoubleWritable(1.5));
    struct.setFieldValue("k", new Text("Moria"));
    struct.setFieldValue(0, new IntWritable(42));
    assertEquals(new IntWritable(42), struct.getFieldValue("i"));
    assertEquals(new DoubleWritable(1.5), struct.getFieldValue(1));
    assertEquals(new Text("Moria"), struct.getFieldValue("k"));
    struct.setAllFields(new IntWritable(123), new DoubleWritable(4.5),
        new Text("ok"));
    assertEquals("123", struct.getFieldValue(0).toString());
    assertEquals("4.5", struct.getFieldValue(1).toString());
    assertEquals("ok", struct.getFieldValue(2).toString());
  }

  @Test
  public void testBadFieldRead() {
    OrcStruct struct = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:double,k:string>"));
    assertThrows(IllegalArgumentException.class, () -> {
      struct.getFieldValue("bad");
    });
  }

  @Test
  public void testBadFieldWrite() {
    OrcStruct struct = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:double,k:string>"));
    assertThrows(IllegalArgumentException.class, () -> {
      struct.setFieldValue("bad", new Text("foobar"));
    });
  }

  @Test
  public void testCompare() {
    OrcStruct left = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:string>"));
    assertEquals(-1 ,left.compareTo(null));
    OrcStruct right = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:string,k:int>"));
    left.setFieldValue(0, new IntWritable(10));
    right.setFieldValue(0, new IntWritable(12));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    right.setFieldValue(0, new IntWritable(10));
    left.setFieldValue(1, new Text("a"));
    right.setFieldValue(1, new Text("b"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    right.setFieldValue(1, new Text("a"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    right = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:string>"));
    left.setFieldValue(0, null);
    left.setFieldValue(1, null);
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));
    right.setFieldValue(0, new IntWritable(12));
    assertEquals(1 , left.compareTo(right));
    assertEquals(-1, right.compareTo(left));
  }

  @Test
  public void testSchemaInCompare() {
    TypeDescription leftType = TypeDescription.fromString("struct<s:string,i:int>");
    TypeDescription rightType = TypeDescription.fromString("struct<s:string,j:bigint>");
    OrcStruct left = new OrcStruct(leftType);
    OrcStruct right = new OrcStruct(rightType);
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    left.setAllFields(new Text("123"), new IntWritable(123));
    right.setAllFields(new Text("123"), new LongWritable(456));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
  }
}
