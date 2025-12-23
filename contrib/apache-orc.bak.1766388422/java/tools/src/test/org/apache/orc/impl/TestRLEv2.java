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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.orc.tools.FileDump;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRLEv2 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Path testFilePath;
  Configuration conf;
  FileSystem fs;

  @BeforeEach
  public void openFileSystem (TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestRLEv2." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  private void appendInt(VectorizedRowBatch batch, long i) {
    ((LongColumnVector) batch.cols[0]).vector[batch.size++] = i;
  }

  @Test
  public void testFixedDeltaZero() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, 123);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // 10 runs of 512 elements. Each run has 2 bytes header, 2 bytes base (base = 123,
    // zigzag encoded varint) and 1 byte delta (delta = 0). In total, 5 bytes per run.
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 50"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaOne() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, i % 512);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // 10 runs of 512 elements. Each run has 2 bytes header, 1 byte base (base = 0)
    // and 1 byte delta (delta = 1). In total, 4 bytes per run.
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 40"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaOneDescending() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, 512 - (i % 512));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // 10 runs of 512 elements. Each run has 2 bytes header, 2 byte base (base = 512, zigzag + varint)
    // and 1 byte delta (delta = 1). In total, 5 bytes per run.
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 50"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaLarge() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, i % 512 + ((i % 512) * 100));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // 10 runs of 512 elements. Each run has 2 bytes header, 1 byte base (base = 0)
    // and 2 bytes delta (delta = 100, zigzag encoded varint). In total, 5 bytes per run.
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 50"));
    System.setOut(origOut);
  }

  @Test
  public void testFixedDeltaLargeDescending() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5120; ++i) {
      appendInt(batch, (512 - i % 512) + ((i % 512) * 100));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // 10 runs of 512 elements. Each run has 2 bytes header, 2 byte base (base = 512, zigzag + varint)
    // and 2 bytes delta (delta = 100, zigzag encoded varint). In total, 6 bytes per run.
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 60"));
    System.setOut(origOut);
  }

  @Test
  public void testShortRepeat() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (int i = 0; i < 5; ++i) {
      appendInt(batch, 10);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // 1 byte header + 1 byte value
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 2"));
    System.setOut(origOut);
  }

  @Test
  public void testDeltaUnknownSign() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    appendInt(batch, 0);
    for (int i = 0; i < 511; ++i) {
      appendInt(batch, i);
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // monotonicity will be undetermined for this sequence 0,0,1,2,3,...510. Hence DIRECT encoding
    // will be used. 2 bytes for header and 640 bytes for data (512 values with fixed bit of 10 bits
    // each, 5120/8 = 640). Total bytes 642
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 642"));
    System.setOut(origOut);
  }

  @Test
  public void testPatchedBase() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .rowIndexStride(0)
            .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
            .version(OrcFile.Version.V_0_12)
    );

    Random rand = new Random(123);
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    appendInt(batch, 10000000);
    for (int i = 0; i < 511; ++i) {
      appendInt(batch, rand.nextInt(i+1));
    }
    w.addRowBatch(batch);
    w.close();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8.toString()));
    FileDump.main(new String[]{testFilePath.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray(), StandardCharsets.UTF_8);
    // use PATCHED_BASE encoding
    assertTrue(outDump.contains("Stream: column 0 section DATA start: 3 length 583"));
    System.setOut(origOut);
  }

  @Test
  public void testBaseValueLimit() throws Exception {
    TypeDescription schema = TypeDescription.createInt();
    Writer w = OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf)
                    .compress(CompressionKind.NONE)
                    .setSchema(schema)
                    .rowIndexStride(0)
                    .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                    .version(OrcFile.Version.V_0_12)
    );

    VectorizedRowBatch batch = schema.createRowBatch();
    //the minimum value is beyond RunLengthIntegerWriterV2.BASE_VALUE_LIMIT
    long[] input = {-9007199254740992l,-8725724278030337l,-1125762467889153l, -1l,-9007199254740992l,
        -9007199254740992l, -497l,127l,-1l,-72057594037927936l,-4194304l,-9007199254740992l,-4503599593816065l,
        -4194304l,-8936830510563329l,-9007199254740992l, -1l, -70334384439312l,-4063233l, -6755399441973249l};
    for(long data: input) {
      appendInt(batch, data);
    }
    w.addRowBatch(batch);
    w.close();

    try(Reader reader = OrcFile.createReader(testFilePath,
            OrcFile.readerOptions(conf).filesystem(fs))) {
      RecordReader rows = reader.rows();
      batch = reader.getSchema().createRowBatch();
      long[] output = null;
      while (rows.nextBatch(batch)) {
        output = new long[batch.size];
        System.arraycopy(((LongColumnVector) batch.cols[0]).vector, 0, output, 0, batch.size);
      }
      assertArrayEquals(input, output);
    }
  }

  static class TestOutputCatcher implements PhysicalWriter.OutputReceiver {
    int currentBuffer = 0;
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      buffers.add(buffer);
    }

    @Override
    public void suppress() {
    }

    ByteBuffer getCurrentBuffer() {
      while (currentBuffer < buffers.size() &&
          buffers.get(currentBuffer).remaining() == 0) {
        currentBuffer += 1;
      }
      return currentBuffer < buffers.size() ? buffers.get(currentBuffer) : null;
    }

    // assert that the list of ints (as bytes) are equal to the output
    public void compareBytes(int... expected) {
      for(int i=0; i < expected.length; ++i) {
        ByteBuffer current = getCurrentBuffer();
        assertEquals((byte) expected[i], current.get(), "position " + i);
      }
      assertNull(getCurrentBuffer());
    }
  }

  static TestOutputCatcher encodeV2(long[] input,
                                    boolean signed) throws IOException {
    TestOutputCatcher catcher = new TestOutputCatcher();
    RunLengthIntegerWriterV2 writer =
        new RunLengthIntegerWriterV2(new OutStream("test",
            new StreamOptions(10000), catcher), signed);
    for(long x: input) {
      writer.write(x);
    }
    writer.flush();
    return catcher;
  }

  @Test
  public void testShortRepeatExample() throws Exception {
    long[] input = {10000, 10000, 10000, 10000, 10000};
    TestOutputCatcher output = encodeV2(input, false);
    output.compareBytes(0x0a, 0x27, 0x10);
  }

  @Test
  public void testDirectExample() throws Exception {
    long[] input = {23713, 43806, 57005, 48879};
    TestOutputCatcher output = encodeV2(input, false);
    output.compareBytes(0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe,
        0xef);
  }

  @Test
  public void testPatchedBaseExample() throws Exception {
    long[] input = {2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080,
        2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190};
    TestOutputCatcher output = encodeV2(input, false);
    output.compareBytes(0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14,
        0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e, 0x78, 0x82, 0x8c,
        0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8);
  }

  @Test
  public void testDeltaExample() throws Exception {
    long[] input = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29};
    TestOutputCatcher output = encodeV2(input, false);
    output.compareBytes(0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46);
  }

  @Test
  public void testDelta2Example() throws Exception {
    long[] input = {0, 10000, 10001, 10001, 10002, 10003, 10003};
    TestOutputCatcher output = encodeV2(input, false);
    output.compareBytes(0xc2, 0x06, 0x0, 0xa0, 0x9c, 0x01, 0x45, 0x0);
  }
}
