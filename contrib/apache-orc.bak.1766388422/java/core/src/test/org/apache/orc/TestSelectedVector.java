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

package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.KeyProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSelectedVector {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  Random random = new Random();

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), VectorizedRowBatch.DEFAULT_SIZE);
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path(workDir, TestSelectedVector.class.getSimpleName() + "." +
        testInfo.getTestMethod().get().getName() + ".orc");
  }

  @AfterEach
  public void deleteTestFile() throws Exception {
    fs.delete(testFilePath, false);
  }

  @Test
  public void testWriteBaseTypeUseSelectedVector() throws IOException {
    TypeDescription schema =
        TypeDescription.fromString("struct<a:boolean,b:tinyint,c:smallint,d:int,e:bigint," +
            "f:float,g:double,h:string,i:date,j:timestamp,k:binary,l:decimal(20,5),m:varchar(5)," +
            "n:char(5)>");

    Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(schema).overwrite(true));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector a = (LongColumnVector) batch.cols[0];
    LongColumnVector b = (LongColumnVector) batch.cols[1];
    LongColumnVector c = (LongColumnVector) batch.cols[2];
    LongColumnVector d = (LongColumnVector) batch.cols[3];
    LongColumnVector e = (LongColumnVector) batch.cols[4];
    DoubleColumnVector f = (DoubleColumnVector) batch.cols[5];
    DoubleColumnVector g = (DoubleColumnVector) batch.cols[6];
    BytesColumnVector h = (BytesColumnVector) batch.cols[7];
    DateColumnVector i = (DateColumnVector) batch.cols[8];
    TimestampColumnVector j = (TimestampColumnVector) batch.cols[9];
    BytesColumnVector k = (BytesColumnVector) batch.cols[10];
    DecimalColumnVector l = (DecimalColumnVector) batch.cols[11];
    BytesColumnVector m = (BytesColumnVector) batch.cols[12];
    BytesColumnVector n = (BytesColumnVector) batch.cols[13];

    List<Integer> selectedRows = new ArrayList<>();
    int[] selected = new int[VectorizedRowBatch.DEFAULT_SIZE];
    int selectedSize = 0;
    int writeRowNum = 0;
    for (int o = 0; o < VectorizedRowBatch.DEFAULT_SIZE * 2; o++) {
      int row = batch.size++;
      if (row % 5 == 0) {
        a.noNulls = false;
        a.isNull[row] = true;
        b.noNulls = false;
        b.isNull[row] = true;
        c.noNulls = false;
        c.isNull[row] = true;
        d.noNulls = false;
        d.isNull[row] = true;
        e.noNulls = false;
        e.isNull[row] = true;
        f.noNulls = false;
        f.isNull[row] = true;
        g.noNulls = false;
        g.isNull[row] = true;
        h.noNulls = false;
        h.isNull[row] = true;
        i.noNulls = false;
        i.isNull[row] = true;
        j.noNulls = false;
        j.isNull[row] = true;
        k.noNulls = false;
        k.isNull[row] = true;
        l.noNulls = false;
        l.isNull[row] = true;
        m.noNulls = false;
        m.isNull[row] = true;
        n.noNulls = false;
        n.isNull[row] = true;
      } else {
        a.vector[row] = row % 2;
        b.vector[row] = row % 128;
        c.vector[row] = row;
        d.vector[row] = row;
        e.vector[row] = row * 10000000L;
        f.vector[row] = row * 1.0f;
        g.vector[row] = row * 1.0d;
        byte[] bytes = String.valueOf(row).getBytes(StandardCharsets.UTF_8);
        h.setRef(row, bytes, 0, bytes.length);
        i.vector[row] = row;
        j.time[row] = row * 1000L;
        j.nanos[row] = row;
        k.setRef(row, bytes, 0, bytes.length);
        l.vector[row] = new HiveDecimalWritable(row);
        m.setRef(row, bytes, 0, bytes.length);
        bytes = String.valueOf(10000 - row).getBytes(StandardCharsets.UTF_8);
        n.setRef(row, bytes, 0, bytes.length);
      }
      if (random.nextInt() % 2 == 0) {
        selectedRows.add(row);
        selected[selectedSize ++] = row;
        writeRowNum ++;
      }
      if (batch.size == batch.getMaxSize()) {
        batch.setFilterContext(true, selected, selectedSize);
        writer.addRowBatch(batch);
        selected = new int[VectorizedRowBatch.DEFAULT_SIZE];
        selectedSize = 0;
        batch.reset();
      }
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    batch = schema.createRowBatch();
    Reader.Options options = reader.options().schema(schema);
    RecordReader rowIterator = reader.rows(options);
    int readRowNum = 0;

    a = (LongColumnVector) batch.cols[0];
    b = (LongColumnVector) batch.cols[1];
    c = (LongColumnVector) batch.cols[2];
    d = (LongColumnVector) batch.cols[3];
    e = (LongColumnVector) batch.cols[4];
    f = (DoubleColumnVector) batch.cols[5];
    g = (DoubleColumnVector) batch.cols[6];
    h = (BytesColumnVector) batch.cols[7];
    i = (DateColumnVector) batch.cols[8];
    j = (TimestampColumnVector) batch.cols[9];
    k = (BytesColumnVector) batch.cols[10];
    l = (DecimalColumnVector) batch.cols[11];
    m = (BytesColumnVector) batch.cols[12];
    n = (BytesColumnVector) batch.cols[13];
    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        int selectedRow = selectedRows.get(readRowNum);
        readRowNum ++;
        if (c.isNull[row]) {
          assertTrue(a.isNull[row]);
          assertTrue(b.isNull[row]);
          assertTrue(d.isNull[row]);
          assertTrue(e.isNull[row]);
          assertTrue(f.isNull[row]);
          assertTrue(g.isNull[row]);
          assertTrue(h.isNull[row]);
          assertTrue(i.isNull[row]);
          assertTrue(j.isNull[row]);
          assertTrue(k.isNull[row]);
          assertTrue(l.isNull[row]);
          assertTrue(m.isNull[row]);
          assertTrue(n.isNull[row]);
        }
        else {
          int rowNum = (int)c.vector[row];
          assertEquals(selectedRow, rowNum);
          assertTrue(rowNum % 5 != 0);
          assertEquals(rowNum % 2, a.vector[row]);
          assertEquals(rowNum % 128, b.vector[row]);
          assertEquals(rowNum, d.vector[row]);
          assertEquals(rowNum * 10000000L, e.vector[row]);
          assertEquals(rowNum * 1.0f, f.vector[row]);
          assertEquals(rowNum * 1.0d, g.vector[row]);
          assertEquals(String.valueOf(rowNum), h.toString(row));
          assertEquals(rowNum, i.vector[row]);
          assertEquals(rowNum * 1000L, j.time[row]);
          assertEquals(rowNum, j.nanos[row]);
          assertEquals(String.valueOf(rowNum), k.toString(row));
          assertEquals(new HiveDecimalWritable(rowNum), l.vector[row]);
          assertEquals(String.valueOf(rowNum), m.toString(row));
          assertEquals(String.valueOf(10000 - rowNum), n.toString(row));
        }
      }
    }
    rowIterator.close();
    assertEquals(writeRowNum, readRowNum);
  }

  @Test
  public void testWriteComplexTypeUseSelectedVector() throws IOException {
    TypeDescription schema =
        TypeDescription.fromString("struct<a:map<int,uniontype<int,string>>," +
            "b:array<struct<c:int>>>");

    Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(schema).overwrite(true));
    VectorizedRowBatch batch = schema.createRowBatch();
    MapColumnVector a = (MapColumnVector) batch.cols[0];
    LongColumnVector keys = (LongColumnVector) a.keys;
    UnionColumnVector values = (UnionColumnVector) a.values;
    LongColumnVector value1 = (LongColumnVector) values.fields[0];
    BytesColumnVector value2 = (BytesColumnVector) values.fields[1];
    ListColumnVector b = (ListColumnVector) batch.cols[1];
    StructColumnVector child = (StructColumnVector) b.child;
    LongColumnVector c = (LongColumnVector) child.fields[0];
    int mapOffset = 0;
    int arrayOffset = 0;
    List<Integer> selectedRows = new ArrayList<>();
    int[] selected = new int[VectorizedRowBatch.DEFAULT_SIZE];
    int selectedSize = 0;
    int writeRowNum = 0;

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE * 2; i++) {
      int row = batch.size++;
      a.offsets[row] = mapOffset;
      b.offsets[row] = arrayOffset;
      int tag = row % 2;
      if (row % 5 == 0) {
        a.lengths[row] = 1;
        values.tags[mapOffset] = tag;
        keys.noNulls = false;
        keys.isNull[mapOffset] = true;
        if (tag == 0) {
          value1.noNulls = false;
          value1.isNull[mapOffset] = true;
        } else {
          value2.noNulls = false;
          value2.isNull[mapOffset] = true;
        }
        b.lengths[row] = 1;
        c.noNulls = false;
        c.isNull[arrayOffset] = true;
      } else {
        a.lengths[row] = 2;
        values.tags[mapOffset] = tag;
        values.tags[mapOffset + 1] = tag;
        keys.vector[mapOffset] = row;
        keys.vector[mapOffset + 1] = row + 1;
        if (tag == 0) {
          value1.vector[mapOffset] = row * 3L;
          value1.vector[mapOffset + 1] = (row + 1) * 3L;
        } else {
          byte[] bytes = String.valueOf(row).getBytes(StandardCharsets.UTF_8);
          value2.setRef(mapOffset, bytes, 0, bytes.length);
          bytes = String.valueOf(row + 1).getBytes(StandardCharsets.UTF_8);
          value2.setRef(mapOffset + 1, bytes, 0, bytes.length);
        }
        b.lengths[row] = 3;
        c.vector[arrayOffset] = row;
        c.vector[arrayOffset + 1] = row + 1;
        c.vector[arrayOffset + 2] = row + 2;
      }
      mapOffset += a.lengths[row];
      arrayOffset += b.lengths[row];

      if (random.nextInt() % 2 == 0) {
        selectedRows.add(row);
        selected[selectedSize ++] = row;
        writeRowNum ++;
      }
      if (arrayOffset + 3 >= batch.getMaxSize()) {
        batch.setFilterContext(true, selected, selectedSize);
        writer.addRowBatch(batch);
        selected = new int[VectorizedRowBatch.DEFAULT_SIZE];
        selectedSize = 0;
        mapOffset = 0;
        arrayOffset = 0;
        batch.reset();
      }
    }
    if (batch.size != 0) {
      batch.setFilterContext(true, selected, selectedSize);
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    batch = schema.createRowBatch();
    Reader.Options options = reader.options().schema(schema);
    RecordReader rowIterator = reader.rows(options);
    int readRowNum = 0;

    a = (MapColumnVector) batch.cols[0];
    keys = (LongColumnVector) a.keys;
    values = (UnionColumnVector) a.values;
    value1 = (LongColumnVector) values.fields[0];
    value2 = (BytesColumnVector) values.fields[1];
    b = (ListColumnVector) batch.cols[1];
    child = (StructColumnVector) b.child;
    c = (LongColumnVector) child.fields[0];
    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        int selectedRow = selectedRows.get(readRowNum);
        readRowNum ++;
        mapOffset = (int)a.offsets[row];
        int mapLen = (int)a.lengths[row];
        arrayOffset = (int)b.offsets[row];
        int arrayLen = (int)b.lengths[row];
        if (mapLen == 1) {
          assertEquals(1, arrayLen);
          assertTrue(keys.isNull[mapOffset]);
          if (values.tags[mapOffset] == 0) {
            assertTrue(value1.isNull[mapOffset]);
          } else {
            assertTrue(value2.isNull[mapOffset]);
          }
          assertTrue(c.isNull[arrayOffset]);
        }
        else {
          assertEquals(2, mapLen);
          assertEquals(3, arrayLen);
          long rowNum = keys.vector[mapOffset];
          assertEquals(selectedRow, rowNum);
          assertEquals(rowNum + 1, keys.vector[mapOffset + 1]);
          if (values.tags[mapOffset] == 0) {
            assertEquals(rowNum * 3, value1.vector[mapOffset]);
          } else {
            assertEquals(String.valueOf(rowNum), value2.toString(mapOffset));
          }
          if (values.tags[mapOffset + 1] == 0) {
            assertEquals((rowNum + 1) * 3, value1.vector[mapOffset + 1]);
          } else {
            assertEquals(String.valueOf(rowNum + 1), value2.toString(mapOffset + 1));
          }
        }

      }
    }
    rowIterator.close();
    assertEquals(writeRowNum, readRowNum);
  }

  @Test
  public void testWriteRepeatedUseSelectedVector() throws IOException {
    TypeDescription schema =
        TypeDescription.fromString("struct<a:int,b:string,c:decimal(20,5)>");

    Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(schema).overwrite(true));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector a = (LongColumnVector) batch.cols[0];
    BytesColumnVector b = (BytesColumnVector) batch.cols[1];
    DecimalColumnVector c = (DecimalColumnVector) batch.cols[2];
    b.fillWithNulls();
    c.fill(new HiveDecimalWritable(42).getHiveDecimal());
    Random random = new Random();
    List<Integer> selectedRows = new ArrayList<>();
    int[] selected = new int[VectorizedRowBatch.DEFAULT_SIZE];

    int selectedSize = 0;
    int writeRowNum = 0;

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      a.vector[i] = i;
      if (random.nextInt() % 2 == 0) {
        selectedRows.add(i);
        selected[selectedSize ++] = i;
        writeRowNum ++;
      }
    }
    batch.setFilterContext(true, selected, selectedSize);
    writer.addRowBatch(batch);
    batch.reset();
    selectedSize = 0;

    b.fill(String.valueOf(42).getBytes(StandardCharsets.UTF_8));
    c.noNulls = false;
    c.isRepeating = true;
    c.vector[0].setFromLong(0);
    c.isNull[0] = true;
    selected = new int[VectorizedRowBatch.DEFAULT_SIZE];

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      a.vector[i] = i + 1024;
      if (random.nextInt() % 2 == 0) {
        selectedRows.add(i + 1024);
        selected[selectedSize ++] = i;
        writeRowNum ++;
      }
    }
    batch.setFilterContext(true, selected, selectedSize);
    writer.addRowBatch(batch);
    batch.reset();
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    batch = schema.createRowBatch();
    Reader.Options options = reader.options().schema(schema);
    RecordReader rowIterator = reader.rows(options);
    int readRowNum = 0;

    a = (LongColumnVector) batch.cols[0];
    b = (BytesColumnVector) batch.cols[1];
    c = (DecimalColumnVector) batch.cols[2];

    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        int selectedRow = selectedRows.get(readRowNum);
        readRowNum ++;
        long rowNum = a.vector[row];
        assertEquals(selectedRow, rowNum);
        if (rowNum < 1024) {
          assertNull(b.toString(row));
          assertEquals(new HiveDecimalWritable(42), c.vector[row]);
        } else {
          assertEquals("42", b.toString(row));
          assertTrue(c.isNull[row]);
        }
      }
    }
    rowIterator.close();
    assertEquals(writeRowNum, readRowNum);
  }

  @Test
  public void testWriteEncryptionUseSelectedVector() throws IOException {
    TypeDescription schema =
        TypeDescription.fromString("struct<id:int,name:string>");

    byte[] kmsKey = "secret123".getBytes(StandardCharsets.UTF_8);
    KeyProvider keyProvider = new InMemoryKeystore()
        .addKey("pii", EncryptionAlgorithm.AES_CTR_128, kmsKey);
    String encryption = "pii:id,name";
    String mask = "sha256:id,name";

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .overwrite(true)
            .setKeyProvider(keyProvider)
            .encrypt(encryption)
            .masks(mask));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector id = (LongColumnVector) batch.cols[0];
    BytesColumnVector name = (BytesColumnVector) batch.cols[1];

    Random random = new Random();
    List<Integer> selectedRows = new ArrayList<>();
    int[] selected = new int[VectorizedRowBatch.DEFAULT_SIZE];

    int selectedSize = 0;
    int writeRowNum = 0;

    for (int r = 0; r < VectorizedRowBatch.DEFAULT_SIZE * 2; ++r) {
      int row = batch.size++;
      id.vector[row] = r;
      byte[] buffer = ("name-" + r).getBytes(StandardCharsets.UTF_8);
      name.setRef(row, buffer, 0, buffer.length);
      if (random.nextInt() % 2 == 0) {
        selectedRows.add(r);
        selected[selectedSize ++] = r % VectorizedRowBatch.DEFAULT_SIZE;
        writeRowNum ++;
      }
      if (batch.size == batch.getMaxSize()) {
        batch.setFilterContext(true, selected, selectedSize);
        writer.addRowBatch(batch);
        selected = new int[VectorizedRowBatch.DEFAULT_SIZE];
        selectedSize = 0;
        batch.reset();
      }
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).setKeyProvider(keyProvider));

    batch = schema.createRowBatch();
    Reader.Options options = reader.options().schema(schema);
    RecordReader rowIterator = reader.rows(options);
    int readRowNum = 0;
    id = (LongColumnVector) batch.cols[0];
    name = (BytesColumnVector) batch.cols[1];
    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        int selectedRow = selectedRows.get(readRowNum);
        readRowNum ++;
        long value = id.vector[row];
        assertEquals(selectedRow, value);
        assertEquals("name-" + (value), name.toString(row));
      }
    }
    rowIterator.close();
    assertEquals(writeRowNum, readRowNum);
  }

}
