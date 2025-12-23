/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestOrcLargeStripe {

  private Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
    + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  private Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Mock
  private FSDataInputStream mockDataInput;

  static class RangeBuilder {
    BufferChunkList result = new BufferChunkList();

    RangeBuilder range(long offset, int length) {
      result.add(new BufferChunk(offset, length));
      return this;
    }

    BufferChunkList build() {
      return result;
    }
  }

  @Test
  public void testZeroCopy() throws Exception {
    BufferChunkList ranges = new RangeBuilder().range(1000, 3000).build();
    HadoopShims.ZeroCopyReaderShim mockZcr = mock(HadoopShims.ZeroCopyReaderShim.class);
    when(mockZcr.readBuffer(anyInt(), anyBoolean()))
        .thenAnswer(invocation -> ByteBuffer.allocate(1000));
    RecordReaderUtils.readDiskRanges(mockDataInput, mockZcr, ranges, true);
    verify(mockDataInput).seek(1000);
    verify(mockZcr).readBuffer(3000, false);
    verify(mockZcr).readBuffer(2000, false);
    verify(mockZcr).readBuffer(1000, false);
  }

  @Test
  public void testRangeMerge() throws Exception {
    BufferChunkList rangeList = new RangeBuilder()
                                    .range(100, 1000)
                                    .range(1000, 10000)
                                    .range(3000, 30000).build();
    RecordReaderUtils.readDiskRanges(mockDataInput, null, rangeList, false);
    verify(mockDataInput).readFully(eq(100L), any(), eq(0), eq(32900));
  }

  @Test
  public void testRangeSkip() throws Exception {
    BufferChunkList rangeList = new RangeBuilder()
                                    .range(1000, 1000)
                                    .range(2000, 1000)
                                    .range(4000, 1000)
                                    .range(4100, 100)
                                    .range(8000, 1000).build();
    RecordReaderUtils.readDiskRanges(mockDataInput, null, rangeList, false);
    verify(mockDataInput).readFully(eq(1000L), any(), eq(0), eq(2000));
    verify(mockDataInput).readFully(eq(4000L), any(), eq(0), eq(1000));
    verify(mockDataInput).readFully(eq(8000L), any(), eq(0), eq(1000));
  }

  @Test
  public void testEmpty() throws Exception {
    BufferChunkList rangeList = new RangeBuilder().build();
    RecordReaderUtils.readDiskRanges(mockDataInput, null, rangeList, false);
    verify(mockDataInput, never()).readFully(anyLong(), any(), anyInt(), anyInt());
  }

  @Test
  public void testConfigMaxChunkLimit() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    TypeDescription schema = TypeDescription.createTimestamp();
    fs.delete(testFilePath, false);
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000).bufferSize(10000)
        .version(OrcFile.Version.V_0_11).fileSystem(fs));
    writer.close();

    OrcFile.ReaderOptions opts = OrcFile.readerOptions(conf);
    Reader reader = OrcFile.createReader(testFilePath, opts);
    RecordReader recordReader = reader.rows(new Reader.Options().range(0L, Long.MAX_VALUE));
    assertTrue(recordReader instanceof RecordReaderImpl);
    assertEquals(Integer.MAX_VALUE - 1024, ((RecordReaderImpl) recordReader).getMaxDiskRangeChunkLimit());

    conf = new Configuration();
    conf.setInt(OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getHiveConfName(), 1000);
    opts = OrcFile.readerOptions(conf);
    reader = OrcFile.createReader(testFilePath, opts);
    recordReader = reader.rows(new Reader.Options().range(0L, Long.MAX_VALUE));
    assertTrue(recordReader instanceof RecordReaderImpl);
    assertEquals(1000, ((RecordReaderImpl) recordReader).getMaxDiskRangeChunkLimit());
  }

  @Test
  public void testStringDirectGreaterThan2GB() throws IOException {
    final Runtime rt = Runtime.getRuntime();
    assumeTrue(rt.maxMemory() > 4_000_000_000L);
    TypeDescription schema = TypeDescription.createString();

    conf.setDouble("hive.exec.orc.dictionary.key.size.threshold", 0.0);
    Writer writer = OrcFile.createWriter(
      testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema)
        .compress(CompressionKind.NONE));
    // 5000 is the lower bound for a stripe
    int size = 5000;
    int width = 500_000;

    // generate a random string that is width characters long
    Random random = new Random(123);
    char[] randomChars= new char[width];
    int posn = 0;
    for(int length = 0; length < width && posn < randomChars.length; ++posn) {
      char cp = (char) random.nextInt(Character.MIN_SUPPLEMENTARY_CODE_POINT);
      // make sure we get a valid, non-surrogate
      while (Character.isSurrogate(cp)) {
        cp = (char) random.nextInt(Character.MIN_SUPPLEMENTARY_CODE_POINT);
      }
      // compute the length of the utf8
      length += cp < 0x80 ? 1 : (cp < 0x800 ? 2 : 3);
      randomChars[posn] = cp;
    }

    // put the random characters in as a repeating value.
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector string = (BytesColumnVector) batch.cols[0];
    string.setVal(0, new String(randomChars, 0, posn).getBytes(StandardCharsets.UTF_8));
    string.isRepeating = true;
    for(int rows=size; rows > 0; rows -= batch.size) {
      batch.size = Math.min(rows, batch.getMaxSize());
      writer.addRowBatch(batch);
    }
    writer.close();

    try {
      Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
      RecordReader rows = reader.rows();
      batch = reader.getSchema().createRowBatch();
      int rowsRead = 0;
      while (rows.nextBatch(batch)) {
        rowsRead += batch.size;
      }
      assertEquals(size, rowsRead);
    } finally {
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testAdjustRowBatchSizeWhenReadLargeString() throws IOException {
    final Runtime rt = Runtime.getRuntime();
    assumeTrue(rt.maxMemory() > 4_000_000_000L);
    TypeDescription schema = TypeDescription.createString();

    conf.setDouble("hive.exec.orc.dictionary.key.size.threshold", 0.0);
    Writer writer = OrcFile.createWriter(
            testFilePath,
            OrcFile.writerOptions(conf).setSchema(schema)
                    .compress(CompressionKind.NONE));
    // default batch size
    int size = 1024;
    int width = Integer.MAX_VALUE / 1000;

    // generate a random string that is width characters long
    Random random = new Random(123);
    char[] randomChars= new char[width];
    int posn = 0;
    for(int length = 0; length < width && posn < randomChars.length; ++posn) {
      char cp = (char) random.nextInt(Character.MIN_SUPPLEMENTARY_CODE_POINT);
      // make sure we get a valid, non-surrogate
      while (Character.isSurrogate(cp)) {
        cp = (char) random.nextInt(Character.MIN_SUPPLEMENTARY_CODE_POINT);
      }
      // compute the length of the utf8
      length += cp < 0x80 ? 1 : (cp < 0x800 ? 2 : 3);
      randomChars[posn] = cp;
    }

    // put the random characters in as a repeating value.
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector string = (BytesColumnVector) batch.cols[0];
    string.setVal(0, new String(randomChars, 0, posn).getBytes(StandardCharsets.UTF_8));
    string.isRepeating = true;
    for(int rows=size; rows > 0; rows -= batch.size) {
      batch.size = Math.min(rows, batch.getMaxSize());
      writer.addRowBatch(batch);
    }
    writer.close();

    // default batch size
    IOException exception = assertThrows(
            IOException.class,
            () -> {
              try (Reader reader = OrcFile.createReader(testFilePath,
                      OrcFile.readerOptions(conf).filesystem(fs))) {
                RecordReader rows = reader.rows();
                rows.nextBatch(reader.getSchema().createRowBatch());
              }
            }
    );
    assertEquals("totalLength:-2095944704 is a negative number. " +
                    "The current batch size is 1024, " +
                    "you can reduce the value by 'orc.row.batch.size'.",
            exception.getCause().getMessage());

    try {
      Reader reader = OrcFile.createReader(testFilePath,
              OrcFile.readerOptions(conf).filesystem(fs));
      RecordReader rows = reader.rows();
      // Modify RowBatchMaxSize to reduce from 1024 to 2
      batch = reader.getSchema().createRowBatch(2);
      int rowsRead = 0;
      while (rows.nextBatch(batch)) {
        rowsRead += batch.size;
      }
      assertEquals(size, rowsRead);
    } finally {
      fs.delete(testFilePath, false);
    }
  }
}
