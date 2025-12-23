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
package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.tree.BatchReader;
import org.apache.orc.impl.reader.tree.StructBatchReader;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSchemaEvolution {

  Configuration conf;
  Reader.Options options;
  Path testFilePath;
  FileSystem fs;
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  @BeforeEach
  public void setup(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    options = new Reader.Options(conf);
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testDataTypeConversion1() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null, options);
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertFalse(both1.hasConversion());
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, options);
    assertTrue(both1diff.hasConversion());
    assertTrue(both1diff.isOnlyImplicitConversion());
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1,
        readerStruct1diffPrecision, options);
    assertTrue(both1diffPrecision.hasConversion());
    assertFalse(both1diffPrecision.isOnlyImplicitConversion());
  }

  @Test
  public void testDataTypeConversion2() throws IOException {
    TypeDescription fileStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution same2 = new SchemaEvolution(fileStruct2, null, options);
    assertFalse(same2.hasConversion());
    TypeDescription readerStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2 = new SchemaEvolution(fileStruct2, readerStruct2, options);
    assertFalse(both2.hasConversion());
    TypeDescription readerStruct2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createByte()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2diff = new SchemaEvolution(fileStruct2, readerStruct2diff, options);
    assertTrue(both2diff.hasConversion());
    assertFalse(both2diff.isOnlyImplicitConversion());
    TypeDescription readerStruct2diffChar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(80));
    SchemaEvolution both2diffChar = new SchemaEvolution(fileStruct2, readerStruct2diffChar, options);
    assertTrue(both2diffChar.hasConversion());
    assertFalse(both2diffChar.isOnlyImplicitConversion());
  }

  @Test
  public void testIntegerImplicitConversion() throws IOException {
    TypeDescription fileStructByte = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution sameByte = new SchemaEvolution(fileStructByte, null, options);
    assertFalse(sameByte.hasConversion());
    TypeDescription readerStructByte = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothByte = new SchemaEvolution(fileStructByte, readerStructByte, options);
    assertFalse(bothByte.hasConversion());
    TypeDescription readerStructByte1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothByte1diff = new SchemaEvolution(fileStructByte, readerStructByte1diff, options);
    assertTrue(bothByte1diff.hasConversion());
    assertTrue(bothByte1diff.isOnlyImplicitConversion());
    TypeDescription readerStructByte2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothByte2diff = new SchemaEvolution(fileStructByte, readerStructByte2diff, options);
    assertTrue(bothByte2diff.hasConversion());
    assertTrue(bothByte2diff.isOnlyImplicitConversion());
    TypeDescription readerStruct3diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothByte3diff = new SchemaEvolution(fileStructByte, readerStruct3diff, options);
    assertTrue(bothByte3diff.hasConversion());
    assertTrue(bothByte3diff.isOnlyImplicitConversion());

    TypeDescription fileStructShort = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution sameShort = new SchemaEvolution(fileStructShort, null, options);
    assertFalse(sameShort.hasConversion());
    TypeDescription readerStructShort = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothShort = new SchemaEvolution(fileStructShort, readerStructShort, options);
    assertFalse(bothShort.hasConversion());
    TypeDescription readerStructShort1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothShort1diff = new SchemaEvolution(fileStructShort, readerStructShort1diff, options);
    assertTrue(bothShort1diff.hasConversion());
    assertTrue(bothShort1diff.isOnlyImplicitConversion());
    TypeDescription readerStructShort2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothShort2diff = new SchemaEvolution(fileStructShort, readerStructShort2diff, options);
    assertTrue(bothShort2diff.hasConversion());
    assertTrue(bothShort2diff.isOnlyImplicitConversion());

    TypeDescription fileStructInt = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution sameInt = new SchemaEvolution(fileStructInt, null, options);
    assertFalse(sameInt.hasConversion());
    TypeDescription readerStructInt = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothInt = new SchemaEvolution(fileStructInt, readerStructInt, options);
    assertFalse(bothInt.hasConversion());
    TypeDescription readerStructInt1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothInt1diff = new SchemaEvolution(fileStructInt, readerStructInt1diff, options);
    assertTrue(bothInt1diff.hasConversion());
    assertTrue(bothInt1diff.isOnlyImplicitConversion());
  }

  @Test
  public void testFloatImplicitConversion() throws IOException {
    TypeDescription fileStructFloat = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createFloat())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution sameFloat = new SchemaEvolution(fileStructFloat, null, options);
    assertFalse(sameFloat.hasConversion());
    TypeDescription readerStructFloat = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createFloat())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothFloat = new SchemaEvolution(fileStructFloat, readerStructFloat, options);
    assertFalse(bothFloat.hasConversion());
    TypeDescription readerStructFloat1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDouble())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothFloat1diff = new SchemaEvolution(fileStructFloat, readerStructFloat1diff, options);
    assertTrue(bothFloat1diff.hasConversion());
    assertTrue(bothFloat1diff.isOnlyImplicitConversion());
  }

  @Test
  public void testCharImplicitConversion() throws IOException {
    TypeDescription fileStructChar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar().withMaxLength(15))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution sameChar = new SchemaEvolution(fileStructChar, null, options);
    assertFalse(sameChar.hasConversion());
    TypeDescription readerStructChar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar().withMaxLength(15))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothChar = new SchemaEvolution(fileStructChar, readerStructChar, options);
    assertFalse(bothChar.hasConversion());
    TypeDescription readerStructChar1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothChar1diff = new SchemaEvolution(fileStructChar, readerStructChar1diff, options);
    assertTrue(bothChar1diff.hasConversion());
    assertTrue(bothChar1diff.isOnlyImplicitConversion());
    TypeDescription readerStructChar2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar().withMaxLength(14))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothChar2diff = new SchemaEvolution(fileStructChar, readerStructChar2diff, options);
    assertTrue(bothChar2diff.hasConversion());
    assertFalse(bothChar2diff.isOnlyImplicitConversion());
    TypeDescription readerStructChar3diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar().withMaxLength(15))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothChar3diff = new SchemaEvolution(fileStructChar, readerStructChar3diff, options);
    assertTrue(bothChar3diff.hasConversion());
    assertTrue(bothChar3diff.isOnlyImplicitConversion());
    TypeDescription readerStructChar4diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar().withMaxLength(14))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothChar4diff = new SchemaEvolution(fileStructChar, readerStructChar4diff, options);
    assertTrue(bothChar4diff.hasConversion());
    assertFalse(bothChar4diff.isOnlyImplicitConversion());
  }

  @Test
  public void testVarcharImplicitConversion() throws IOException {
    TypeDescription fileStructVarchar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar().withMaxLength(15))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution sameVarchar = new SchemaEvolution(fileStructVarchar, null, options);
    assertFalse(sameVarchar.hasConversion());
    TypeDescription readerStructVarchar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar().withMaxLength(15))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothVarchar = new SchemaEvolution(fileStructVarchar, readerStructVarchar, options);
    assertFalse(bothVarchar.hasConversion());
    TypeDescription readerStructVarchar1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString())
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothVarchar1diff = new SchemaEvolution(fileStructVarchar, readerStructVarchar1diff, options);
    assertTrue(bothVarchar1diff.hasConversion());
    assertTrue(bothVarchar1diff.isOnlyImplicitConversion());
    TypeDescription readerStructVarchar2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar().withMaxLength(14))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothVarchar2diff = new SchemaEvolution(fileStructVarchar, readerStructVarchar2diff, options);
    assertTrue(bothVarchar2diff.hasConversion());
    assertFalse(bothVarchar2diff.isOnlyImplicitConversion());
    TypeDescription readerStructVarchar3diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar().withMaxLength(15))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothVarchar3diff = new SchemaEvolution(fileStructVarchar, readerStructVarchar3diff, options);
    assertTrue(bothVarchar3diff.hasConversion());
    assertTrue(bothVarchar3diff.isOnlyImplicitConversion());
    TypeDescription readerStructVarchar4diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar().withMaxLength(14))
        .addField("f2", TypeDescription.createString());
    SchemaEvolution bothVarchar4diff = new SchemaEvolution(fileStructVarchar, readerStructVarchar4diff, options);
    assertTrue(bothVarchar4diff.hasConversion());
    assertFalse(bothVarchar4diff.isOnlyImplicitConversion());
  }

  @Test
  public void testFloatToDoubleEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createFloat();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72f;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDouble();
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals(74.72, ((DoubleColumnVector) batch.cols[0]).vector[0], 0.00001);
    rows.close();
  }

  @Test
  public void testFloatToDecimalEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
      testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createFloat();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72f;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(38).withScale(2);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals("74.72", ((DecimalColumnVector) batch.cols[0]).vector[0].toString());
    rows.close();
  }

  @Test
  public void testFloatToDecimal64Evolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createFloat();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72f;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(10).withScale(2);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatchV2();
    rows.nextBatch(batch);
    assertEquals("74.72", ((Decimal64ColumnVector) batch.cols[0]).getScratchWritable().toString());
    rows.close();
  }

  @Test
  public void testDoubleToDecimalEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createDouble();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72d;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(38).withScale(2);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals("74.72", ((DecimalColumnVector) batch.cols[0]).vector[0].toString());
    rows.close();
  }

  @Test
  public void testDoubleToDecimal64Evolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createDouble();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72d;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(10).withScale(2);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatchV2();
    rows.nextBatch(batch);
    assertEquals("74.72", ((Decimal64ColumnVector) batch.cols[0]).getScratchWritable().toString());
    rows.close();
  }

  @Test
  public void testLongToDecimalEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createLong();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    LongColumnVector lcv = new LongColumnVector(1024);
    batch.cols[0] = lcv;
    batch.reset();
    batch.size = 1;
    lcv.vector[0] = 74L;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(38).withScale(2);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals("74", ((DecimalColumnVector) batch.cols[0]).vector[0].toString());
    rows.close();
  }

  @Test
  public void testLongToDecimal64Evolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createLong();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    LongColumnVector lcv = new LongColumnVector(1024);
    batch.cols[0] = lcv;
    batch.reset();
    batch.size = 1;
    lcv.vector[0] = 74L;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(10).withScale(2);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatchV2();
    rows.nextBatch(batch);
    assertEquals("74", ((Decimal64ColumnVector) batch.cols[0]).getScratchWritable().toString());
    rows.close();
  }

  @Test
  public void testDecimalToDecimalEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createDecimal().withPrecision(38).withScale(0);
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DecimalColumnVector dcv = new DecimalColumnVector(1024, 38, 2);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = new HiveDecimalWritable("74.19");
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(38).withScale(1);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals("74.2", ((DecimalColumnVector) batch.cols[0]).vector[0].toString());
    rows.close();
  }

  @Test
  public void testDecimalToDecimal64Evolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createDecimal().withPrecision(38).withScale(2);
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DecimalColumnVector dcv = new DecimalColumnVector(1024, 38, 0);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = new HiveDecimalWritable("74.19");
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(10).withScale(1);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatchV2();
    rows.nextBatch(batch);
    assertEquals(742, ((Decimal64ColumnVector) batch.cols[0]).vector[0]);
    rows.close();
  }

  @Test
  public void testBooleanToStringEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createBoolean();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    LongColumnVector lcv = new LongColumnVector(1024);
    batch.cols[0] = lcv;
    batch.reset();
    batch.size = 3;
    lcv.vector[0] = 1L; // True
    lcv.vector[1] = 0L; // False
    lcv.vector[2] = 1L; // True
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createString();
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals("TRUE", ((BytesColumnVector) batch.cols[0]).toString(0));
    assertEquals("FALSE", ((BytesColumnVector) batch.cols[0]).toString(1));
    assertEquals("TRUE", ((BytesColumnVector) batch.cols[0]).toString(2));
    rows.close();
  }

  @Test
  public void testCharToStringEvolution() throws IOException {
    TypeDescription fileType = TypeDescription.fromString("struct<x:char(10)>");
    TypeDescription readType = TypeDescription.fromString("struct<x:string>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readType, options);

    TreeReaderFactory.Context treeContext =
        new TreeReaderFactory.ReaderContext().setSchemaEvolution(evo);
    TypeReader reader =
        TreeReaderFactory.createTreeReader(readType, treeContext);

    // Make sure the tree reader is built properly
    assertEquals(TreeReaderFactory.StructTreeReader.class, reader.getClass());
    TypeReader[] children =
        ((TreeReaderFactory.StructTreeReader) reader).getChildReaders();
    assertEquals(1, children.length);
    assertEquals(ConvertTreeReaderFactory.StringGroupFromStringGroupTreeReader.class, children[0].getClass());

    // Make sure that varchar behaves the same as char
    fileType = TypeDescription.fromString("struct<x:varchar(10)>");
    evo = new SchemaEvolution(fileType, readType, options);

    treeContext = new TreeReaderFactory.ReaderContext().setSchemaEvolution(evo);
    reader = TreeReaderFactory.createTreeReader(readType, treeContext);

    // Make sure the tree reader is built properly
    assertEquals(TreeReaderFactory.StructTreeReader.class, reader.getClass());
    children = ((TreeReaderFactory.StructTreeReader) reader).getChildReaders();
    assertEquals(1, children.length);
    assertEquals(ConvertTreeReaderFactory.StringGroupFromStringGroupTreeReader.class, children[0].getClass());
  }

  @Test
  public void testStringToDecimalEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createString();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    BytesColumnVector bcv = new BytesColumnVector(1024);
    batch.cols[0] = bcv;
    batch.reset();
    batch.size = 1;
    bcv.vector[0] = "74.19".getBytes(StandardCharsets.UTF_8);
    bcv.length[0] = "74.19".getBytes(StandardCharsets.UTF_8).length;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(38).withScale(1);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals("74.2", ((DecimalColumnVector) batch.cols[0]).vector[0].toString());
    rows.close();
  }

  @Test
  public void testStringToDecimal64Evolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createString();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    BytesColumnVector bcv = new BytesColumnVector(1024);
    batch.cols[0] = bcv;
    batch.reset();
    batch.size = 1;
    bcv.vector[0] = "74.19".getBytes(StandardCharsets.UTF_8);
    bcv.length[0] = "74.19".getBytes(StandardCharsets.UTF_8).length;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(10).withScale(1);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatchV2();
    rows.nextBatch(batch);
    assertEquals(742, ((Decimal64ColumnVector) batch.cols[0]).vector[0]);
    rows.close();
  }

  @Test
  public void testTimestampToDecimalEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createTimestamp();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    TimestampColumnVector tcv = new TimestampColumnVector(1024);
    batch.cols[0] = tcv;
    batch.reset();
    batch.size = 3;
    tcv.time[0] = 74000L;
    tcv.nanos[0] = 123456789;
    tcv.time[1] = 123000L;
    tcv.nanos[1] = 456000000;
    tcv.time[2] = 987000;
    tcv.nanos[2] = 0;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(38).withScale(9);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(3, batch.size);
    DecimalColumnVector dcv = (DecimalColumnVector) batch.cols[0];
    assertEquals("74.123456789", dcv.vector[0].toString());
    assertEquals("123.456", dcv.vector[1].toString());
    assertEquals("987", dcv.vector[2].toString());
    rows.close();
  }

  @Test
  public void testTimestampToDecimal64Evolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.createTimestamp();
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    TimestampColumnVector tcv = new TimestampColumnVector(1024);
    batch.cols[0] = tcv;
    batch.reset();
    batch.size = 1;
    tcv.time[0] = 74000L;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDecimal().withPrecision(10).withScale(1);
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatchV2();
    rows.nextBatch(batch);
    assertEquals(740, ((Decimal64ColumnVector) batch.cols[0]).vector[0]);
    rows.close();
  }

  @Test
  public void testTimestampToStringEvolution(TestInfo testInfo) throws Exception {
    testFilePath = new Path(workDir, "TestSchemaEvolution." +
        testInfo.getTestMethod().get().getName() + ".orc");
    TypeDescription schema = TypeDescription.fromString("timestamp");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000).useUTCTimestamp(true));
    VectorizedRowBatch batch = schema.createRowBatchV2();
    TimestampColumnVector tcv = (TimestampColumnVector) batch.cols[0];
    batch.size = 3;
    tcv.time[0] = 74000L;
    tcv.nanos[0] = 123456789;
    tcv.time[1] = 123000L;
    tcv.nanos[1] = 456000000;
    tcv.time[2] = 987000;
    tcv.nanos[2] = 0;
    writer.addRowBatch(batch);
    writer.close();

    schema = TypeDescription.fromString("string");
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows(reader.options().schema(schema));
    batch = schema.createRowBatchV2();
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    assertTrue(rows.nextBatch(batch));
    assertEquals(3, batch.size);
    assertEquals("1970-01-01 00:01:14.123456789", bcv.toString(0));
    assertEquals("1970-01-01 00:02:03.456", bcv.toString(1));
    assertEquals("1970-01-01 00:16:27", bcv.toString(2));
    rows.close();
  }

  @Test
  public void testSafePpdEvaluation() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null, options);
    assertTrue(same1.isPPDSafeConversion(0));
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertFalse(both1.hasConversion());
    assertTrue(both1.isPPDSafeConversion(0));
    assertTrue(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertTrue(both1.isPPDSafeConversion(3));

    // int -> long
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, options);
    assertTrue(both1diff.hasConversion());
    assertFalse(both1diff.isPPDSafeConversion(0));
    assertTrue(both1diff.isPPDSafeConversion(1));
    assertTrue(both1diff.isPPDSafeConversion(2));
    assertTrue(both1diff.isPPDSafeConversion(3));

    // decimal(38,10) -> decimal(12, 10)
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    options.include(new boolean[] {true, false, false, true});
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1,
        readerStruct1diffPrecision, options);
    assertTrue(both1diffPrecision.hasConversion());
    assertFalse(both1diffPrecision.isPPDSafeConversion(0));
    assertFalse(both1diffPrecision.isPPDSafeConversion(1)); // column not included
    assertFalse(both1diffPrecision.isPPDSafeConversion(2)); // column not included
    assertFalse(both1diffPrecision.isPPDSafeConversion(3));

    // add columns
    readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10))
        .addField("f4", TypeDescription.createBoolean());
    options.include(null);
    both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertTrue(both1.hasConversion());
    assertFalse(both1.isPPDSafeConversion(0));
    assertTrue(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertTrue(both1.isPPDSafeConversion(3));
    assertFalse(both1.isPPDSafeConversion(4));

    // column pruning
    readerStruct1 = TypeDescription.createStruct()
        .addField("f2", TypeDescription.createString());
    both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertTrue(both1.hasConversion());
    assertFalse(both1.isPPDSafeConversion(0));
    assertFalse(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertFalse(both1.isPPDSafeConversion(3));
  }

  @Test
  public void testSafePpdEvaluationForInts() throws IOException {
    // byte -> short -> int -> long
    TypeDescription fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertFalse(schemaEvolution.hasConversion());

    // byte -> short
    TypeDescription readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // byte -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // byte -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // short -> int -> long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion short -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // short -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // short -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // int -> long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion int -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion int -> short
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // int -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion long -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion long -> short
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion long -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createFloat());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createTimestamp());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));
  }

  @Test
  public void testSafePpdEvaluationForStrings() throws IOException {
    TypeDescription fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // string -> char
    TypeDescription readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // string -> varchar
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // char -> string
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // char -> varchar
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // varchar -> string
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // varchar -> char
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDecimal());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDate());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));
  }

  private boolean[] includeAll(TypeDescription readerType) {
    int numColumns = readerType.getMaximumId() + 1;
    boolean[] result = new boolean[numColumns];
    Arrays.fill(result, true);
    return result;
  }

  @Test
  public void testAddFieldToEnd() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,b:string,c:double>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // b -> b
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);

    // c -> null
    reader = readerType.getChildren().get(2);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testAddFieldBeforeEnd() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,c:double,b:string>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // c -> null
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);

    // b -> b
    reader = readerType.getChildren().get(2);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testRemoveLastField() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string,c:double>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,b:string>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // b -> b
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testRemoveFieldBeforeEnd() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string,c:double>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,c:double>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // c -> b
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(2);
    assertSame(original, mapped);

  }

  @Test
  public void testRemoveAndAddField() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,c:double>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // c -> null
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testReorderFields() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<b:string,a:int>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // b -> b
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(1);
    assertSame(original, mapped);

    // a -> a
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(0);
    assertSame(original, mapped);
  }

  @Test
  public void testAddFieldEndOfStruct() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:struct<b:int>,c:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:struct<b:int,d:double>,c:string>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // a.d -> null
    readerChild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerChild);
    originalChild = null;
    assertSame(originalChild, mapped);

    // c -> c
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testAddFieldBeforeEndOfStruct() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:struct<b:int>,c:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:struct<d:double,b:int>,c:string>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // a.d -> null
    readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    originalChild = null;
    assertSame(originalChild, mapped);

    // c -> c
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testCaseMismatchInReaderAndWriterSchema() {
    TypeDescription fileType =
            TypeDescription.fromString("struct<a:struct<b:int>,c:string>");
    TypeDescription readerType =
            TypeDescription.fromString("struct<A:struct<b:int>,c:string>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
            new SchemaEvolution(fileType, readerType, options.include(included).isSchemaEvolutionCaseAware(false));

    // a -> A
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // c -> c
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  /**
   * Two structs can be equal but in different locations. They can converge to this.
   */
  @Test
  public void testAddSimilarField() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:struct<b:int>>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:struct<b:int>,c:struct<b:int>>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // c -> null
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);

    // c.b -> null
    readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    original = null;
    assertSame(original, mapped);
  }

  /**
   * Two structs can be equal but in different locations. They can converge to this.
   */
  @Test
  public void testConvergentEvolution() {
    TypeDescription fileType = TypeDescription
        .fromString("struct<a:struct<a:int,b:string>,c:struct<a:int>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:struct<a:int,b:string>,c:struct<a:int,b:string>>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // c -> c
    TypeDescription reader = readerType.getChildren().get(1);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(1);
    assertSame(original, mapped);

    // c.a -> c.a
    TypeDescription readerchild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // c.b -> null
    readerchild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerchild);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testMapEvolution() {
    TypeDescription fileType =
        TypeDescription
            .fromString("struct<a:map<struct<a:int>,struct<a:int>>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:map<struct<a:int,b:string>,struct<a:int,c:string>>>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.key -> a.key
    TypeDescription readerchild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // a.value -> a.value
    readerchild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerchild);
    original = fileType.getChildren().get(0).getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testListEvolution() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:array<struct<b:int>>>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:array<struct<b:int,c:string>>>");
    boolean[] included = includeAll(readerType);
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.element -> a.element
    TypeDescription readerchild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    readerchild = reader.getChildren().get(0).getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // a.c -> null
    readerchild = reader.getChildren().get(0).getChildren().get(1);
    mapped = transition.getFileType(readerchild);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testIncompatibleTypes() {
    assertThrows(SchemaEvolution.IllegalEvolutionException.class, () -> {
      TypeDescription fileType = TypeDescription.fromString("struct<a:int>");
      TypeDescription readerType = TypeDescription.fromString("struct<a:date>");
      boolean[] included = includeAll(readerType);
      options.tolerateMissingSchema(false);
      new SchemaEvolution(fileType, readerType, options.include(included));
    });
  }

  @Test
  public void testAcidNamedEvolution() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<x:int,z:bigint,y:string>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string,z:bigint>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string,z:bigint>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string,z:bigint>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    for(int c=0; c < 8; ++c) {
      assertEquals(c, evo.getFileType(c).getId(), "column " + c);
    }
    // y and z should swap places
    assertEquals(9, evo.getFileType(8).getId());
    assertEquals(8, evo.getFileType(9).getId());
  }

  @Test
  public void testAcidPositionEvolutionAddField() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:string>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string,z:bigint>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string,z:bigint>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string,z:bigint>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    for(int c=0; c < 9; ++c) {
      assertEquals(c, evo.getFileType(c).getId(), "column " + c);
    }
    // the file doesn't have z
    assertNull(evo.getFileType(9));
  }

  @Test
  public void testAcidPositionEvolutionSkipAcid() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:string,_col2:double>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType,
        options.includeAcidColumns(false));
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();

    //get top level struct col
    assertEquals(0, evo.getFileType(0).getId(), "column " + 0);
    assertTrue(fileInclude[0], "column " + 0);
    for(int c=1; c < 6; ++c) {
      assertNull(evo.getFileType(c), "column " + c);
      //skip all acid metadata columns
      assertFalse(fileInclude[c], "column " + c);
    }
    for(int c=6; c < 9; ++c) {
      assertEquals(c, evo.getFileType(c).getId(), "column " + c);
      assertTrue(fileInclude[c], "column " + c);
    }
    // don't read the last column
    assertFalse(fileInclude[9]);
  }

  @Test
  public void testAcidPositionEvolutionRemoveField() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:string,_col2:double>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();
    for(int c=0; c < 9; ++c) {
      assertEquals(c, evo.getFileType(c).getId(), "column " + c);
      assertTrue(fileInclude[c], "column " + c);
    }
    // don't read the last column
    assertFalse(fileInclude[9]);
  }

  @Test
  public void testAcidPositionSubstructure() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:struct<z:int,x:double,y:string>," +
            "_col2:double>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:int,b:struct<x:double,y:string,z:int>,c:double>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();
    for(int c=0; c < 9; ++c) {
      assertEquals(c, evo.getFileType(c).getId(), "column " + c);
    }
    assertEquals(10, evo.getFileType(9).getId());
    assertEquals(11, evo.getFileType(10).getId());
    assertEquals(9, evo.getFileType(11).getId());
    assertEquals(12, evo.getFileType(12).getId());
    assertEquals(13, fileInclude.length);
    for(int c=0; c < fileInclude.length; ++c) {
      assertTrue(fileInclude[c], "column " + c);
    }
  }

  @Test
  public void testNonAcidPositionSubstructure() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<_col0:int,_col1:struct<x:double,z:int>," +
            "_col2:double>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:int,b:struct<x:double,y:string,z:int>,c:double>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertFalse(evo.isAcid());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();
    assertEquals(0, evo.getFileType(0).getId());
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertNull(evo.getFileType(4));
    assertEquals(4, evo.getFileType(5).getId());
    assertEquals(5, evo.getFileType(6).getId());
    assertEquals(6, fileInclude.length);
    for(int c=0; c < fileInclude.length; ++c) {
      assertTrue(fileInclude[c], "column " + c);
    }
  }

  @Test
  public void testFileIncludeWithNoEvolution() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<a:int,b:double,c:string>");
    SchemaEvolution evo = new SchemaEvolution(fileType, null,
        options.include(new boolean[]{true, false, true, false}));
    assertFalse(evo.isAcid());
    assertEquals("struct<a:int,b:double,c:string>",
        evo.getReaderBaseSchema().toString());
    boolean[] fileInclude = evo.getFileIncluded();
    assertTrue(fileInclude[0]);
    assertFalse(fileInclude[1]);
    assertTrue(fileInclude[2]);
    assertFalse(fileInclude[3]);
  }

  static ByteBuffer createBuffer(int... values) {
    ByteBuffer result = ByteBuffer.allocate(values.length);
    for(int v: values) {
      result.put((byte) v);
    }
    result.flip();
    return result;
  }

  @Test
  public void testTypeConversion() throws IOException {
    TypeDescription fileType = TypeDescription.fromString("struct<x:int,y:string>");
    TypeDescription readType = TypeDescription.fromString("struct<z:int,y:string,x:bigint>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readType, options);

    // check to make sure the fields are mapped correctly
    assertNull(evo.getFileType(1));
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(1, evo.getFileType(3).getId());

    TreeReaderFactory.Context treeContext =
        new TreeReaderFactory.ReaderContext().setSchemaEvolution(evo);
    BatchReader reader =
        TreeReaderFactory.createRootReader(readType, treeContext);

    // check to make sure the tree reader is built right
    assertEquals(StructBatchReader.class, reader.getClass());
    assertEquals(TreeReaderFactory.StructTreeReader.class, reader.rootType.getClass());
    TypeReader[] children =
        ((TreeReaderFactory.StructTreeReader) reader.rootType).getChildReaders();
    assertEquals(3, children.length);
    assertEquals(TreeReaderFactory.NullTreeReader.class, children[0].getClass());
    assertEquals(TreeReaderFactory.StringTreeReader.class, children[1].getClass());
    assertEquals(ConvertTreeReaderFactory.AnyIntegerFromAnyIntegerTreeReader.class,
        children[2].getClass());

    // check to make sure the data is read correctly
    MockDataReader dataReader = new MockDataReader(fileType)
        .addStream(1, OrcProto.Stream.Kind.DATA, createBuffer(7, 1, 0))
        .addStream(2, OrcProto.Stream.Kind.DATA, createBuffer(65, 66, 67, 68,
            69, 70, 71, 72, 73, 74))
        .addStream(2, OrcProto.Stream.Kind.LENGTH, createBuffer(7, 0, 1))
        .addStripeFooter(100, null);
    StripePlanner planner = new StripePlanner(fileType, new ReaderEncryption(),
        dataReader, OrcFile.WriterVersion.ORC_14, true, Integer.MAX_VALUE);
    boolean[] columns = new boolean[]{true, true, true};
    planner.parseStripe(dataReader.getStripe(0), columns)
        .readData(null, null, false, TypeReader.ReadPhase.ALL);
    reader.startStripe(planner, TypeReader.ReadPhase.ALL);
    VectorizedRowBatch batch = readType.createRowBatch();
    reader.nextBatch(batch, 10, TypeReader.ReadPhase.ALL);
    final String EXPECTED = "ABCDEFGHIJ";
    assertTrue(batch.cols[0].isRepeating);
    assertTrue(batch.cols[0].isNull[0]);
    for(int r=0; r < 10; ++r) {
      assertEquals(EXPECTED.substring(r, r+1),
        ((BytesColumnVector) batch.cols[1]).toString(r), "col1." + r);
      assertEquals(r, ((LongColumnVector) batch.cols[2]).vector[r], "col2." + r);
    }
  }

  @Test
  public void testTypeConversionShouldIgnoreAttributes() throws IOException {
    TypeDescription fileType = TypeDescription.fromString("struct<x:int,y:smallint>");
    TypeDescription readType = TypeDescription.fromString("struct<x:int,y:int>");
    readType.findSubtype("x").setAttribute("iceberg.id", "12");
    readType.findSubtype("y").setAttribute("iceberg.id", "13");
    SchemaEvolution evo = new SchemaEvolution(fileType, readType, options);

    // check to make sure the fields are mapped correctly
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());

    TreeReaderFactory.Context treeContext =
        new TreeReaderFactory.ReaderContext().setSchemaEvolution(evo);
    TypeReader reader =
        TreeReaderFactory.createTreeReader(readType, treeContext);

    // check to make sure the tree reader is built right
    assertEquals(TreeReaderFactory.StructTreeReader.class, reader.getClass());
    TypeReader[] children =
        ((TreeReaderFactory.StructTreeReader) reader).getChildReaders();
    assertEquals(2, children.length);
    assertEquals(TreeReaderFactory.IntTreeReader.class, children[0].getClass());
    assertEquals(ConvertTreeReaderFactory.AnyIntegerFromAnyIntegerTreeReader.class,
        children[1].getClass());
  }

  @Test
  public void testPositionalEvolution() throws IOException {
    options.forcePositionalEvolution(true);
    TypeDescription file = TypeDescription.fromString("struct<x:int,y:int,z:int>");
    TypeDescription read = TypeDescription.fromString("struct<z:int,x:int,a:int,b:int>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertNull(evo.getFileType(4));
  }

  @Test
  public void testPositionalEvolutionLevel() throws IOException {
    options.forcePositionalEvolution(true);
    options.positionalEvolutionLevel(2);
    TypeDescription file = TypeDescription.fromString("struct<a:int,b:struct<y:int,y:int>>");
    TypeDescription read = TypeDescription.fromString("struct<a:int,b:struct<y:int,y:int>>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertEquals(4, evo.getFileType(4).getId());
  }

  @Test
  public void testStructInArrayWithoutPositionalEvolution() throws IOException {
    options.forcePositionalEvolution(false);
    options.positionalEvolutionLevel(Integer.MAX_VALUE);
    TypeDescription file = TypeDescription.fromString("array<struct<x:string,y:int,z:double>>");
    TypeDescription read = TypeDescription.fromString("array<struct<z:double,x:string,a:int,b:int>>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(4, evo.getFileType(2).getId());
    assertEquals(2, evo.getFileType(3).getId());
    assertNull(evo.getFileType(4));
    assertNull(evo.getFileType(5));
  }

  @Test
  public void testPositionalEvolutionForStructInArray() throws IOException {
    options.forcePositionalEvolution(true);
    options.positionalEvolutionLevel(Integer.MAX_VALUE);
    TypeDescription file = TypeDescription.fromString("array<struct<x:int,y:int,z:int>>");
    TypeDescription read = TypeDescription.fromString("array<struct<z:int,x:int,a:int,b:int>>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertEquals(4, evo.getFileType(4).getId());
    assertNull(evo.getFileType(5));
  }

  @Test
  public void testPositionalEvolutionForTwoLayerNestedStruct() throws IOException {
    options.forcePositionalEvolution(true);
    options.positionalEvolutionLevel(Integer.MAX_VALUE);
    TypeDescription file = TypeDescription.fromString("struct<s:struct<x:int,y:int,z:int>>");
    TypeDescription read = TypeDescription.fromString("struct<s:struct<z:int,x:int,a:int,b:int>>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertEquals(4, evo.getFileType(4).getId());
    assertNull(evo.getFileType(5));
  }

  @Test
  public void testPositionalEvolutionForThreeLayerNestedStruct() throws IOException {
    options.forcePositionalEvolution(true);
    options.positionalEvolutionLevel(Integer.MAX_VALUE);
    TypeDescription file = TypeDescription.fromString("struct<s1:struct<s2:struct<x:int,y:int,z:int>>>");
    TypeDescription read = TypeDescription.fromString("struct<s1:struct<s:struct<z:int,x:int,a:int,b:int>>>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertEquals(4, evo.getFileType(4).getId());
    assertEquals(5, evo.getFileType(5).getId());
    assertNull(evo.getFileType(6));
  }

  // These are helper methods that pull some of the common code into one
  // place.

  static String decimalTimestampToString(long centiseconds, ZoneId zone) {
    int nano = (int) (Math.floorMod(centiseconds, 100) * 10_000_000);
    return timestampToString(centiseconds * 10, nano, zone);
  }

  static String doubleTimestampToString(double seconds, ZoneId zone) {
    long sec = (long) Math.floor(seconds);
    int nano = 1_000_000 * (int) Math.round((seconds - sec) * 1000);
    return timestampToString(sec * 1000, nano, zone);
  }

  static String timestampToString(long millis, int nanos, ZoneId zone) {
    return timestampToString(Instant.ofEpochSecond(Math.floorDiv(millis, 1000),
        nanos), zone);
  }

  static String longTimestampToString(long seconds, ZoneId zone) {
    return timestampToString(Instant.ofEpochSecond(seconds), zone);
  }

  static String timestampToString(Instant time, ZoneId zone) {
    return time.atZone(zone)
               .format(ConvertTreeReaderFactory.INSTANT_TIMESTAMP_FORMAT);
  }

  static void writeTimestampDataFile(Path path,
                                     Configuration conf,
                                     ZoneId writerZone,
                                     DateTimeFormatter formatter,
                                     String[] values) throws IOException {
    TimeZone oldDefault = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(writerZone));
      TypeDescription fileSchema =
          TypeDescription.fromString("struct<t1:timestamp," +
                                         "t2:timestamp with local time zone>");
      Writer writer = OrcFile.createWriter(path,
          OrcFile.writerOptions(conf).setSchema(fileSchema).stripeSize(10000));
      VectorizedRowBatch batch = fileSchema.createRowBatch(1024);
      TimestampColumnVector t1 = (TimestampColumnVector) batch.cols[0];
      TimestampColumnVector t2 = (TimestampColumnVector) batch.cols[1];
      for (int r = 0; r < values.length; ++r) {
        int row = batch.size++;
        Instant t = Instant.from(formatter.parse(values[r]));
        t1.time[row] = t.getEpochSecond() * 1000;
        t1.nanos[row] = t.getNano();
        t2.time[row] = t1.time[row];
        t2.nanos[row] = t1.nanos[row];
        if (batch.size == 1024) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      }
      if (batch.size != 0) {
        writer.addRowBatch(batch);
      }
      writer.close();
    } finally {
      TimeZone.setDefault(oldDefault);
    }
  }

  /**
   * Tests the various conversions from timestamp and timestamp with local
   * timezone.
   *
   * It writes an ORC file with timestamp and timestamp with local time zone
   * and then reads it back in with each of the relevant types.
   *
   * This test test both with and without the useUtc flag.
   *
   * It uses Australia/Sydney and America/New_York because they both have
   * DST and they move in opposite directions on different days. Thus, we
   * end up with four sets of offsets.
   *
   * Picking the 27th of the month puts it around when DST changes.
   */
  @Test
  public void testEvolutionFromTimestamp() throws Exception {
    // The number of rows in the file that we test with.
    final int VALUES = 1024;
    // The different timezones that we'll use for this test.
    final ZoneId UTC = ZoneId.of("UTC");
    final ZoneId WRITER_ZONE = ZoneId.of("America/New_York");
    final ZoneId READER_ZONE = ZoneId.of("Australia/Sydney");

    final TimeZone oldDefault = TimeZone.getDefault();

    // generate the timestamps to use
    String[] timeStrings = new String[VALUES];
    for(int r=0; r < timeStrings.length; ++r) {
      timeStrings[r] = String.format("%04d-%02d-27 23:45:56.7",
          2000 + (r / 12), (r % 12) + 1);
    }

    final DateTimeFormatter WRITER_FORMAT =
        ConvertTreeReaderFactory.TIMESTAMP_FORMAT.withZone(WRITER_ZONE);

    writeTimestampDataFile(testFilePath, conf, WRITER_ZONE, WRITER_FORMAT, timeStrings);

    try {
      TimeZone.setDefault(TimeZone.getTimeZone(READER_ZONE));
      OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
      Reader.Options rowOptions = new Reader.Options();

      try (Reader reader = OrcFile.createReader(testFilePath, options)) {

        // test conversion to long
        TypeDescription readerSchema = TypeDescription.fromString("struct<t1:bigint,t2:bigint>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatch(VALUES);
          LongColumnVector t1 = (LongColumnVector) batch.cols[0];
          LongColumnVector t2 = (LongColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(
                (timeStrings[r] + " " + READER_ZONE.getId()).replace(".7 ", " "),
                longTimestampToString(t1.vector[current], READER_ZONE),
                "row " + r);
            assertEquals(
                (timeStrings[r] + " " + WRITER_ZONE.getId()).replace(".7 ", " "),
                longTimestampToString(t2.vector[current], WRITER_ZONE),
                "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to decimal
        readerSchema = TypeDescription.fromString("struct<t1:decimal(14,2),t2:decimal(14,2)>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatchV2();
          Decimal64ColumnVector t1 = (Decimal64ColumnVector) batch.cols[0];
          Decimal64ColumnVector t2 = (Decimal64ColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals( timeStrings[r] + " " + READER_ZONE.getId(),
                decimalTimestampToString(t1.vector[current], READER_ZONE), "row " + r);
            assertEquals(timeStrings[r] + " " + WRITER_ZONE.getId(),
                decimalTimestampToString(t2.vector[current], WRITER_ZONE), "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to double
        readerSchema = TypeDescription.fromString("struct<t1:double,t2:double>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatchV2();
          DoubleColumnVector t1 = (DoubleColumnVector) batch.cols[0];
          DoubleColumnVector t2 = (DoubleColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals( timeStrings[r] + " " + READER_ZONE.getId(),
                doubleTimestampToString(t1.vector[current], READER_ZONE), "row " + r);
            assertEquals( timeStrings[r] + " " + WRITER_ZONE.getId(),
                doubleTimestampToString(t2.vector[current], WRITER_ZONE), "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to date
        readerSchema = TypeDescription.fromString("struct<t1:date,t2:date>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatchV2();
          LongColumnVector t1 = (LongColumnVector) batch.cols[0];
          LongColumnVector t2 = (LongColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            String date = timeStrings[r].substring(0, 10);
            assertEquals(date,
                ConvertTreeReaderFactory.DATE_FORMAT.format(
                    LocalDate.ofEpochDay(t1.vector[current])), "row " + r);
            // NYC -> Sydney moves forward a day for instant
            assertEquals(date.replace("-27", "-28"),
                ConvertTreeReaderFactory.DATE_FORMAT.format(
                    LocalDate.ofEpochDay(t2.vector[current])), "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to string
        readerSchema = TypeDescription.fromString("struct<t1:string,t2:string>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatch(VALUES);
          BytesColumnVector bytesT1 = (BytesColumnVector) batch.cols[0];
          BytesColumnVector bytesT2 = (BytesColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(timeStrings[r], bytesT1.toString(current), "row " + r);
            Instant t = Instant.from(WRITER_FORMAT.parse(timeStrings[r]));
            assertEquals(
                timestampToString(Instant.from(WRITER_FORMAT.parse(timeStrings[r])),
                    READER_ZONE),
                bytesT2.toString(current), "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion between timestamps
        readerSchema = TypeDescription.fromString("struct<t1:timestamp with local time zone,t2:timestamp>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatch(VALUES);
          TimestampColumnVector timeT1 = (TimestampColumnVector) batch.cols[0];
          TimestampColumnVector timeT2 = (TimestampColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(timeStrings[r] + " " + READER_ZONE.getId(),
                timestampToString(timeT1.time[current], timeT1.nanos[current], READER_ZONE),
                "row " + r);
            assertEquals(
                timestampToString(Instant.from(WRITER_FORMAT.parse(timeStrings[r])), READER_ZONE),
                timestampToString(timeT2.time[current], timeT2.nanos[current], READER_ZONE),
                "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }
      }

      // Now test using UTC as local
      options.useUTCTimestamp(true);
      try (Reader reader = OrcFile.createReader(testFilePath, options)) {
        DateTimeFormatter UTC_FORMAT =
            ConvertTreeReaderFactory.TIMESTAMP_FORMAT.withZone(UTC);

        // test conversion to int in UTC
        TypeDescription readerSchema =
            TypeDescription.fromString("struct<t1:bigint,t2:bigint>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatch(VALUES);
          LongColumnVector t1 = (LongColumnVector) batch.cols[0];
          LongColumnVector t2 = (LongColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(
                (timeStrings[r] + " " + UTC.getId()).replace(".7 ", " "),
                longTimestampToString(t1.vector[current], UTC), "row " + r);
            assertEquals(
                (timeStrings[r] + " " + WRITER_ZONE.getId()).replace(".7 ", " "),
                longTimestampToString(t2.vector[current], WRITER_ZONE), "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to decimal
        readerSchema = TypeDescription.fromString("struct<t1:decimal(14,2),t2:decimal(14,2)>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatchV2();
          Decimal64ColumnVector t1 = (Decimal64ColumnVector) batch.cols[0];
          Decimal64ColumnVector t2 = (Decimal64ColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(timeStrings[r] + " " + UTC.getId(),
                decimalTimestampToString(t1.vector[current], UTC), "row " + r);
            assertEquals(timeStrings[r] + " " + WRITER_ZONE.getId(),
                decimalTimestampToString(t2.vector[current], WRITER_ZONE), "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to double
        readerSchema = TypeDescription.fromString("struct<t1:double,t2:double>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatchV2();
          DoubleColumnVector t1 = (DoubleColumnVector) batch.cols[0];
          DoubleColumnVector t2 = (DoubleColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(timeStrings[r] + " " + UTC.getId(),
                doubleTimestampToString(t1.vector[current], UTC),
                "row " + r);
            assertEquals(timeStrings[r] + " " + WRITER_ZONE.getId(),
                doubleTimestampToString(t2.vector[current], WRITER_ZONE),
                "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to date
        readerSchema = TypeDescription.fromString("struct<t1:date,t2:date>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatchV2();
          LongColumnVector t1 = (LongColumnVector) batch.cols[0];
          LongColumnVector t2 = (LongColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            String date = timeStrings[r].substring(0, 10);
            assertEquals(date,
                ConvertTreeReaderFactory.DATE_FORMAT.format(
                    LocalDate.ofEpochDay(t1.vector[current])),
                "row " + r);
            // NYC -> UTC still moves forward a day
            assertEquals(date.replace("-27", "-28"),
                ConvertTreeReaderFactory.DATE_FORMAT.format(
                    LocalDate.ofEpochDay(t2.vector[current])),
                "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion to string in UTC
        readerSchema = TypeDescription.fromString("struct<t1:string,t2:string>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatch(VALUES);
          BytesColumnVector bytesT1 = (BytesColumnVector) batch.cols[0];
          BytesColumnVector bytesT2 = (BytesColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(timeStrings[r], bytesT1.toString(current), "row " + r);
            assertEquals(
                timestampToString(Instant.from(WRITER_FORMAT.parse(timeStrings[r])),
                    UTC),
                bytesT2.toString(current),
                "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }

        // test conversion between timestamps in UTC
        readerSchema = TypeDescription.fromString("struct<t1:timestamp with local time zone,t2:timestamp>");
        try (RecordReader rows = reader.rows(rowOptions.schema(readerSchema))) {
          VectorizedRowBatch batch = readerSchema.createRowBatch(VALUES);
          TimestampColumnVector timeT1 = (TimestampColumnVector) batch.cols[0];
          TimestampColumnVector timeT2 = (TimestampColumnVector) batch.cols[1];
          int current = 0;
          for (int r = 0; r < VALUES; ++r) {
            if (current == batch.size) {
              assertTrue(rows.nextBatch(batch), "row " + r);
              current = 0;
            }
            assertEquals(timeStrings[r] + " UTC",
                timestampToString(timeT1.time[current], timeT1.nanos[current], UTC),
                "row " + r);
            assertEquals(
                timestampToString(Instant.from(WRITER_FORMAT.parse(timeStrings[r])), UTC),
                timestampToString(timeT2.time[current], timeT2.nanos[current], UTC),
                "row " + r);
            current += 1;
          }
          assertFalse(rows.nextBatch(batch));
        }
      }
    } finally {
      TimeZone.setDefault(oldDefault);
    }
  }

  static void writeEvolutionToTimestamp(Path path,
                                        Configuration conf,
                                        ZoneId writerZone,
                                        String[] values) throws IOException {
    TypeDescription fileSchema =
        TypeDescription.fromString("struct<l1:bigint,l2:bigint," +
                                       "t1:tinyint,t2:tinyint," +
                                       "d1:decimal(14,2),d2:decimal(14,2)," +
                                       "dbl1:double,dbl2:double," +
                                       "dt1:date,dt2:date," +
                                       "s1:string,s2:string>");
    ZoneId UTC = ZoneId.of("UTC");
    DateTimeFormatter WRITER_FORMAT = ConvertTreeReaderFactory.TIMESTAMP_FORMAT
                                          .withZone(writerZone);
    DateTimeFormatter UTC_FORMAT = ConvertTreeReaderFactory.TIMESTAMP_FORMAT
                                          .withZone(UTC);
    DateTimeFormatter UTC_DATE = ConvertTreeReaderFactory.DATE_FORMAT
                                     .withZone(UTC);
    Writer writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf).setSchema(fileSchema).stripeSize(10000));
    VectorizedRowBatch batch = fileSchema.createRowBatchV2();
    int batchSize = batch.getMaxSize();
    LongColumnVector l1 = (LongColumnVector) batch.cols[0];
    LongColumnVector l2 = (LongColumnVector) batch.cols[1];
    LongColumnVector t1 = (LongColumnVector) batch.cols[2];
    LongColumnVector t2 = (LongColumnVector) batch.cols[3];
    Decimal64ColumnVector d1 = (Decimal64ColumnVector) batch.cols[4];
    Decimal64ColumnVector d2 = (Decimal64ColumnVector) batch.cols[5];
    DoubleColumnVector dbl1 = (DoubleColumnVector) batch.cols[6];
    DoubleColumnVector dbl2 = (DoubleColumnVector) batch.cols[7];
    LongColumnVector dt1 = (LongColumnVector) batch.cols[8];
    LongColumnVector dt2 = (LongColumnVector) batch.cols[9];
    BytesColumnVector s1 = (BytesColumnVector) batch.cols[10];
    BytesColumnVector s2 = (BytesColumnVector) batch.cols[11];
    for (int r = 0; r < values.length; ++r) {
      int row = batch.size++;
      Instant utcTime = Instant.from(UTC_FORMAT.parse(values[r]));
      Instant writerTime = Instant.from(WRITER_FORMAT.parse(values[r]));
      l1.vector[row] =  utcTime.getEpochSecond();
      l2.vector[row] =  writerTime.getEpochSecond();
      t1.vector[row] = r % 128;
      t2.vector[row] = r % 128;
      // balance out the 2 digits of scale
      d1.vector[row] = utcTime.toEpochMilli() / 10;
      d2.vector[row] = writerTime.toEpochMilli() / 10;
      // convert to double
      dbl1.vector[row] = utcTime.toEpochMilli() / 1000.0;
      dbl2.vector[row] = writerTime.toEpochMilli() / 1000.0;
      // convert to date
      dt1.vector[row] = UTC_DATE.parse(values[r].substring(0, 10))
                            .getLong(ChronoField.EPOCH_DAY);
      dt2.vector[row] = dt1.vector[row];
      // set the strings
      s1.setVal(row, values[r].getBytes(StandardCharsets.UTF_8));
      String withZone = values[r] + " " + writerZone.getId();
      s2.setVal(row, withZone.getBytes(StandardCharsets.UTF_8));

      if (batch.size == batchSize) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
  }

  /**
   * Tests the various conversions to timestamp.
   *
   * It writes an ORC file with two longs, two decimals, and two strings and
   * then reads it back with the types converted to timestamp and timestamp
   * with local time zone.
   *
   * This test is run both with and without setting the useUtc flag.
   *
   * It uses Australia/Sydney and America/New_York because they both have
   * DST and they move in opposite directions on different days. Thus, we
   * end up with four sets of offsets.
   */
  @Test
  public void testEvolutionToTimestamp() throws Exception {
    // The number of rows in the file that we test with.
    final int VALUES = 1024;
    // The different timezones that we'll use for this test.
    final ZoneId WRITER_ZONE = ZoneId.of("America/New_York");
    final ZoneId READER_ZONE = ZoneId.of("Australia/Sydney");

    final TimeZone oldDefault = TimeZone.getDefault();
    final ZoneId UTC = ZoneId.of("UTC");

    // generate the timestamps to use
    String[] timeStrings = new String[VALUES];
    for(int r=0; r < timeStrings.length; ++r) {
      timeStrings[r] = String.format("%04d-%02d-27 12:34:56.1",
          1960 + (r / 12), (r % 12) + 1);
    }

    writeEvolutionToTimestamp(testFilePath, conf, WRITER_ZONE, timeStrings);

    try {
      TimeZone.setDefault(TimeZone.getTimeZone(READER_ZONE));

      // test timestamp, timestamp with local time zone to long
      TypeDescription readerSchema = TypeDescription.fromString(
          "struct<l1:timestamp," +
              "l2:timestamp with local time zone," +
              "t1:timestamp," +
              "t2:timestamp with local time zone," +
              "d1:timestamp," +
              "d2:timestamp with local time zone," +
              "dbl1:timestamp," +
              "dbl2:timestamp with local time zone," +
              "dt1:timestamp," +
              "dt2:timestamp with local time zone," +
              "s1:timestamp," +
              "s2:timestamp with local time zone>");
      VectorizedRowBatch batch = readerSchema.createRowBatchV2();
      TimestampColumnVector l1 = (TimestampColumnVector) batch.cols[0];
      TimestampColumnVector l2 = (TimestampColumnVector) batch.cols[1];
      TimestampColumnVector t1 = (TimestampColumnVector) batch.cols[2];
      TimestampColumnVector t2 = (TimestampColumnVector) batch.cols[3];
      TimestampColumnVector d1 = (TimestampColumnVector) batch.cols[4];
      TimestampColumnVector d2 = (TimestampColumnVector) batch.cols[5];
      TimestampColumnVector dbl1 = (TimestampColumnVector) batch.cols[6];
      TimestampColumnVector dbl2 = (TimestampColumnVector) batch.cols[7];
      TimestampColumnVector dt1 = (TimestampColumnVector) batch.cols[8];
      TimestampColumnVector dt2 = (TimestampColumnVector) batch.cols[9];
      TimestampColumnVector s1 = (TimestampColumnVector) batch.cols[10];
      TimestampColumnVector s2 = (TimestampColumnVector) batch.cols[11];
      OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
      Reader.Options rowOptions = new Reader.Options().schema(readerSchema);
      int offset = READER_ZONE.getRules().getOffset(Instant.ofEpochSecond(0, 0)).getTotalSeconds();
      try (Reader reader = OrcFile.createReader(testFilePath, options);
           RecordReader rows = reader.rows(rowOptions)) {
        int current = 0;
        for (int r = 0; r < VALUES; ++r) {
          if (current == batch.size) {
            assertTrue(rows.nextBatch(batch), "row " + r);
            current = 0;
          }

          String expected1 = timeStrings[r] + " " + READER_ZONE.getId();
          String expected2 = timeStrings[r] + " " + WRITER_ZONE.getId();
          String midnight = timeStrings[r].substring(0, 10) + " 00:00:00";
          String expectedDate1 = midnight + " " + READER_ZONE.getId();
          String expectedDate2 = midnight + " " + UTC.getId();

          String msg = "row " + r;
          assertEquals(expected1.replace(".1 ", " "),
              timestampToString(l1.time[current], l1.nanos[current], READER_ZONE),
              msg);

          assertEquals(expected2.replace(".1 ", " "),
              timestampToString(l2.time[current], l2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(longTimestampToString(((r % 128) - offset), READER_ZONE),
              timestampToString(t1.time[current], t1.nanos[current], READER_ZONE),
              msg);

          assertEquals(longTimestampToString((r % 128), WRITER_ZONE),
              timestampToString(t2.time[current], t2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(expected1,
              timestampToString(d1.time[current], d1.nanos[current], READER_ZONE),
              msg);

          assertEquals(expected2,
              timestampToString(d2.time[current], d2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(expected1,
              timestampToString(dbl1.time[current], dbl1.nanos[current], READER_ZONE),
              msg);

          assertEquals(expected2,
              timestampToString(dbl2.time[current], dbl2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(expectedDate1,
              timestampToString(dt1.time[current], dt1.nanos[current], READER_ZONE),
              msg);

          assertEquals(expectedDate2,
              timestampToString(dt2.time[current], dt2.nanos[current], UTC),
              msg);

          assertEquals(expected1,
              timestampToString(s1.time[current], s1.nanos[current], READER_ZONE),
              msg);

          assertEquals(expected2,
              timestampToString(s2.time[current], s2.nanos[current], WRITER_ZONE),
              msg);
          current += 1;
        }
        assertFalse(rows.nextBatch(batch));
      }

      // try the tests with useUtc set on
      options.useUTCTimestamp(true);
      try (Reader reader = OrcFile.createReader(testFilePath, options);
           RecordReader rows = reader.rows(rowOptions)) {
        int current = 0;
        for (int r = 0; r < VALUES; ++r) {
          if (current == batch.size) {
            assertTrue(rows.nextBatch(batch), "row " + r);
            current = 0;
          }

          String expected1 = timeStrings[r] + " " + UTC.getId();
          String expected2 = timeStrings[r] + " " + WRITER_ZONE.getId();
          String midnight = timeStrings[r].substring(0, 10) + " 00:00:00";
          String expectedDate = midnight + " " + UTC.getId();
          String msg = "row " + r;
          assertEquals(expected1.replace(".1 ", " "),
              timestampToString(l1.time[current], l1.nanos[current], UTC),
              msg);

          assertEquals(expected2.replace(".1 ", " "),
              timestampToString(l2.time[current], l2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(expected1,
              timestampToString(d1.time[current], d1.nanos[current], UTC),
              msg);

          assertEquals(expected2,
              timestampToString(d2.time[current], d2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(expected1,
              timestampToString(dbl1.time[current], dbl1.nanos[current], UTC),
              msg);

          assertEquals(expected2,
              timestampToString(dbl2.time[current], dbl2.nanos[current], WRITER_ZONE),
              msg);

          assertEquals(expectedDate,
              timestampToString(dt1.time[current], dt1.nanos[current], UTC),
              msg);

          assertEquals(expectedDate,
              timestampToString(dt2.time[current], dt2.nanos[current], UTC),
              msg);

          assertEquals(expected1,
              timestampToString(s1.time[current], s1.nanos[current], UTC),
              msg);

          assertEquals(expected2,
              timestampToString(s2.time[current], s2.nanos[current], WRITER_ZONE),
              msg);
          current += 1;
        }
        assertFalse(rows.nextBatch(batch));
      }
    } finally {
      TimeZone.setDefault(oldDefault);
    }
  }

  @Test
  public void doubleToTimeStampOverflow() throws Exception {
    floatAndDoubleToTimeStampOverflow("double",
        340282347000000000000000000000000000000000.0,
        1e16,
        9223372036854778.0,
        9000000000000000.1,
        10000000000.0,
        10000000.123,
        -1000000.123,
        -10000000000.0,
        -9000000000000000.1,
        -9223372036854778.0,
        -1e16,
        -340282347000000000000000000000000000000000.0);
  }

  @Test
  public void floatToTimeStampPositiveOverflow() throws Exception {
    floatAndDoubleToTimeStampOverflow("float",
        340282347000000000000000000000000000000000.0,
        1e16,
        9223372036854778.0,
        9000000000000000.1,
        10000000000.0,
        10000000.123,
        -1000000.123,
        -10000000000.0,
        -9000000000000000.1,
        -9223372036854778.0,
        -1e16,
        -340282347000000000000000000000000000000000.0);
  }

  private void floatAndDoubleToTimeStampOverflow(String typeInFileSchema,
                                                 double... values) throws Exception {
    boolean isFloat = typeInFileSchema.equals("float");
    TypeDescription fileSchema =
        TypeDescription.fromString(String.format("struct<c1:%s>", typeInFileSchema));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(fileSchema)
            .stripeSize(10000)
            .useUTCTimestamp(true));

    VectorizedRowBatch batch = fileSchema.createRowBatchV2();
    DoubleColumnVector fl1 = (DoubleColumnVector) batch.cols[0];

    for (double v : values) {
      int row = batch.size++;
      fl1.vector[row] = v;

      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();

    TypeDescription readerSchema = TypeDescription.fromString("struct<c1:timestamp>");
    VectorizedRowBatch batchTimeStamp = readerSchema.createRowBatchV2();
    TimestampColumnVector t1 = (TimestampColumnVector) batchTimeStamp.cols[0];

    OrcFile.ReaderOptions options = OrcFile
                                        .readerOptions(conf)
                                        .useUTCTimestamp(true);

    try (Reader reader = OrcFile.createReader(testFilePath, options);
         RecordReader rows = reader.rows(reader.options().schema(readerSchema))) {
      int value = 0;
      while (value < values.length) {
        assertTrue(rows.nextBatch(batchTimeStamp), "value " + value);
        for(int row=0; row < batchTimeStamp.size; ++row) {
          double expected = values[value + row];
          String rowName = String.format("value %d", value + row);
          boolean isPositive = ((long)Math.floor(expected) * 1000) >= 0;
          if (expected * 1000 < Long.MIN_VALUE ||
                  expected * 1000 > Long.MAX_VALUE ||
                  ((expected >= 0) != isPositive)) {
            assertFalse(t1.noNulls, rowName);
            assertTrue(t1.isNull[row], rowName);
          } else {
            double actual = Math.floorDiv(t1.time[row], 1000) +
                                t1.nanos[row] / 1_000_000_000.0;
            assertEquals(expected, actual,
                Math.abs(expected * (isFloat ? 0.000001 : 0.0000000000000001)), rowName);
            assertFalse(t1.isNull[row], rowName);
            assertTrue(t1.nanos[row] >= 0 && t1.nanos[row] < 1_000_000_000,
                String.format(
                    "%s nanos should be 0 to 1,000,000,000 instead it's: %d",
                    rowName, t1.nanos[row]));
          }
        }
        value += batchTimeStamp.size;
      }
      assertFalse(rows.nextBatch(batchTimeStamp));
    }
  }

  @Test
  public void testCheckAcidSchema() {
    String ccSchema = "struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<a:int,b:int>>";
    String lcSchema = "struct<operation:int,originaltransaction:bigint,bucket:int," +
        "rowid:bigint,currenttransaction:bigint," +
        "row:struct<a:int,b:int>>";

    TypeDescription typeCamelCaseColumns = TypeDescription.fromString(ccSchema);
    TypeDescription typeLowerCaseColumns = TypeDescription.fromString(lcSchema);
    SchemaEvolution evoCc = new SchemaEvolution(typeCamelCaseColumns, null, options);
    SchemaEvolution evoLc = new SchemaEvolution(typeLowerCaseColumns, null, options);

    assertTrue(evoCc.isAcid(), "Schema (" + ccSchema +") was found to be non-acid ");
    assertTrue(evoLc.isAcid(), "Schema (" + lcSchema +") was found to be non-acid ");
  }

  @Test
  public void testAcidReaderSchema() {
    String acidSchema = "struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<a:int,b:int>>";

    TypeDescription fileSchema = TypeDescription.fromString(acidSchema);
    TypeDescription readerSchema = TypeDescription.fromString(acidSchema);
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);

    assertEquals(acidSchema, schemaEvolution.getReaderSchema().toString(),
        String.format("Reader schema %s is not acid", schemaEvolution.getReaderSchema().toString()));

    String notAcidSchema ="struct<a:int,b:int>";
    readerSchema = TypeDescription.fromString(notAcidSchema);
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertEquals(acidSchema, schemaEvolution.getReaderSchema().toString(),
        String.format("Reader schema %s is not acid", schemaEvolution.getReaderSchema().toString()));
  }
}
