/*
 * Copyright 2016 The Apache Software Foundation.
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TestVectorOrcFile;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestReaderImpl {
  private Path workDir = new Path(System.getProperty("example.dir",
      "../../examples/"));

  private final Path path = new Path("test-file.orc");
  private FSDataInputStream in;
  private int psLen;
  private ByteBuffer buffer;

  @BeforeEach
  public void setup() {
    in = null;
  }

  @Test
  public void testEnsureOrcFooterSmallTextFile() throws IOException {
    prepareTestCase("1".getBytes(StandardCharsets.UTF_8));
    assertThrows(FileFormatException.class, () -> {
      ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
    });
  }

  @Test
  public void testEnsureOrcFooterLargeTextFile() throws IOException {
    prepareTestCase("This is Some Text File".getBytes(StandardCharsets.UTF_8));
    assertThrows(FileFormatException.class, () -> {
      ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
    });
  }

  @Test
  public void testEnsureOrcFooter011ORCFile() throws IOException {
    prepareTestCase(composeContent(OrcFile.MAGIC, "FOOTER"));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterCorrectORCFooter() throws IOException {
    prepareTestCase(composeContent("", OrcFile.MAGIC));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testOptionSafety() throws IOException {
    Reader.Options options = new Reader.Options();
    String expected = options.toString();
    Configuration conf = new Configuration();
    Path path = new Path(TestVectorOrcFile.getFileFromClasspath
        ("orc-file-11-format.orc"));
    try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
         RecordReader rows = reader.rows(options)) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      while (rows.nextBatch(batch)) {
        assertTrue(batch.size > 0);
      }
    }
    assertEquals(expected, options.toString());
  }

  private void prepareTestCase(byte[] bytes) throws IOException {
    buffer = ByteBuffer.wrap(bytes);
    psLen = buffer.get(bytes.length - 1) & 0xff;
    in = new FSDataInputStream(new SeekableByteArrayInputStream(bytes));
  }

  private byte[] composeContent(String headerStr, String footerStr) throws CharacterCodingException {
    ByteBuffer header = Text.encode(headerStr);
    ByteBuffer footer = Text.encode(footerStr);
    int headerLen = header.remaining();
    int footerLen = footer.remaining() + 1;

    ByteBuffer buf = ByteBuffer.allocate(headerLen + footerLen);

    buf.put(header);
    buf.put(footer);
    buf.put((byte) footerLen);
    return buf.array();
  }

  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream
          implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      this.reset();
      this.skip(pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
      int nread = 0;
      while (nread < length) {
        int nbytes = read(position + nread, buffer, offset + nread, length - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }

  static byte[] byteArray(int... input) {
    byte[] result = new byte[input.length];
    for(int i=0; i < result.length; ++i) {
      result[i] = (byte) input[i];
    }
    return result;
  }

  static class MockInputStream extends FSDataInputStream {
    MockFileSystem fs;
    // A single row ORC file
    static final byte[] SIMPLE_ORC = byteArray(
        0x4f, 0x52, 0x43, 0x42, 0x00, 0x80, 0x0a, 0x06, 0x08, 0x01, 0x10, 0x01, 0x18, 0x03, 0x12, 0x02,
        0x08, 0x00, 0x12, 0x02, 0x08, 0x02, 0x0a, 0x12, 0x0a, 0x04, 0x08, 0x00, 0x50, 0x00, 0x0a, 0x0a,
        0x08, 0x00, 0x12, 0x02, 0x18, 0x00, 0x50, 0x00, 0x58, 0x03, 0x08, 0x03, 0x10, 0x16, 0x1a, 0x0a,
        0x08, 0x03, 0x10, 0x00, 0x18, 0x03, 0x20, 0x10, 0x28, 0x01, 0x22, 0x08, 0x08, 0x0c, 0x12, 0x01,
        0x01, 0x1a, 0x01, 0x78, 0x22, 0x02, 0x08, 0x03, 0x30, 0x01, 0x3a, 0x04, 0x08, 0x00, 0x50, 0x00,
        0x3a, 0x0a, 0x08, 0x00, 0x12, 0x02, 0x18, 0x00, 0x50, 0x00, 0x58, 0x03, 0x40, 0x00, 0x48, 0x00,
        0x08, 0x36, 0x10, 0x00, 0x22, 0x02, 0x00, 0x0c, 0x28, 0x14, 0x30, 0x07, 0x82, 0xf4, 0x03, 0x03,
        0x4f, 0x52, 0x43, 0x13);

    public MockInputStream(MockFileSystem fs) throws IOException {
      super(new SeekableByteArrayInputStream(SIMPLE_ORC));
      this.fs = fs;
    }

    public void close() {
      fs.removeStream(this);
    }
  }

  static class MockFileSystem extends FileSystem {
    final List<MockInputStream> streams = new ArrayList<>();

    public MockFileSystem(Configuration conf) {
      setConf(conf);
    }

    @Override
    public URI getUri() {
      try {
        return new URI("mock:///");
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("bad uri", e);
      }
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
      MockInputStream result = new MockInputStream(this);
      streams.add(result);
      return result;
    }

    void removeStream(MockInputStream stream) {
      streams.remove(stream);
    }

    int streamCount() {
      return streams.size();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                     boolean b, int i, short i1, long l,
                                     Progressable progressable) throws IOException {
      throw new IOException("Can't create");
    }

    @Override
    public FSDataOutputStream append(Path path, int i,
                                     Progressable progressable) throws IOException {
      throw new IOException("Can't append");
    }

    @Override
    public boolean rename(Path path, Path path1) {
      return false;
    }

    @Override
    public boolean delete(Path path, boolean b) {
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) {
      return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {
      // ignore
    }

    @Override
    public Path getWorkingDirectory() {
      return new Path("/");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) {
      return new FileStatus(MockInputStream.SIMPLE_ORC.length, false, 1, 4096,
          0, path);
    }
  }

  @Test
  public void testClosingRowsFirst() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf);
    Reader reader = OrcFile.createReader(new Path("/foo"),
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(1, fs.streamCount());
    RecordReader rows = reader.rows();
    assertEquals(1, fs.streamCount());
    RecordReader rows2 = reader.rows();
    assertEquals(2, fs.streamCount());
    rows.close();
    assertEquals(1, fs.streamCount());
    rows2.close();
    assertEquals(0, fs.streamCount());
    reader.close();
    assertEquals(0, fs.streamCount());
  }

  @Test
  public void testClosingReaderFirst() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf);
    Reader reader = OrcFile.createReader(new Path("/foo"),
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(1, fs.streamCount());
    RecordReader rows = reader.rows();
    assertEquals(1, fs.streamCount());
    reader.close();
    assertEquals(1, fs.streamCount());
    rows.close();
    assertEquals(0, fs.streamCount());
  }

  @Test
  public void testClosingMultiple() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf);
    Reader reader = OrcFile.createReader(new Path("/foo"),
        OrcFile.readerOptions(conf).filesystem(fs));
    Reader reader2 = OrcFile.createReader(new Path("/bar"),
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(2, fs.streamCount());
    reader.close();
    assertEquals(1, fs.streamCount());
    reader2.close();
    assertEquals(0, fs.streamCount());
  }

  @Test
  public void testOrcTailStripeStats() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "orc_split_elim_new.orc");
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      OrcTail tail = reader.extractFileTail(fs, path, Long.MAX_VALUE);
      List<StripeStatistics> stats = tail.getStripeStatistics();
      assertEquals(1, stats.size());
      OrcProto.TimestampStatistics tsStats =
          stats.get(0).getColumn(5).getTimestampStatistics();
      assertEquals(-28800000, tsStats.getMinimumUtc());
      assertEquals(-28550000, tsStats.getMaximumUtc());

      // Test Tail and Stats extraction from ByteBuffer
      ByteBuffer tailBuffer = tail.getSerializedTail();
      OrcTail extractedTail = ReaderImpl.extractFileTail(tailBuffer);

      assertEquals(tail.getTailBuffer(), extractedTail.getTailBuffer());
      assertEquals(tail.getTailBuffer().getData(), extractedTail.getTailBuffer().getData());
      assertEquals(tail.getTailBuffer().getOffset(), extractedTail.getTailBuffer().getOffset());
      assertEquals(tail.getTailBuffer().getEnd(), extractedTail.getTailBuffer().getEnd());

      assertEquals(tail.getMetadataOffset(), extractedTail.getMetadataOffset());
      assertEquals(tail.getMetadataSize(), extractedTail.getMetadataSize());

      Reader dummyReader = new ReaderImpl(null,
          OrcFile.readerOptions(OrcFile.readerOptions(conf).getConfiguration())
          .orcTail(extractedTail));
      List<StripeStatistics> tailBufferStats = dummyReader.getVariantStripeStatistics(null);

      assertEquals(stats.size(), tailBufferStats.size());
      OrcProto.TimestampStatistics bufferTsStats = tailBufferStats.get(0).getColumn(5).getTimestampStatistics();
      assertEquals(tsStats.getMinimumUtc(), bufferTsStats.getMinimumUtc());
      assertEquals(tsStats.getMaximumUtc(), bufferTsStats.getMaximumUtc());
    }
  }

  @Test
  public void testGetRawDataSizeFromColIndices() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "orc_split_elim_new.orc");
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      TypeDescription schema = reader.getSchema();
      List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
      boolean[] include = new boolean[schema.getMaximumId() + 1];
      List<Integer> list = new ArrayList<Integer>();
      for (int i = 0; i < include.length; i++) {
        include[i] = true;
        list.add(i);
      }
      List<OrcProto.ColumnStatistics> stats = reader.getFileTail().getFooter().getStatisticsList();
      assertEquals(
        ReaderImpl.getRawDataSizeFromColIndices(include, schema, stats),
        ReaderImpl.getRawDataSizeFromColIndices(list, types, stats));
    }
  }

  private void CheckFileWithSargs(String fileName, String softwareVersion)
      throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, fileName);
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      assertEquals(softwareVersion, reader.getSoftwareVersion());

      Reader.Options opt = new Reader.Options();
      SearchArgument.Builder builder = SearchArgumentFactory.newBuilder(conf);
      builder.equals("id", PredicateLeaf.Type.LONG, 18000000000L);
      opt.searchArgument(builder.build(), new String[]{"id"});

      TypeDescription schema = reader.getSchema();
      VectorizedRowBatch batch = schema.createRowBatch();
      try (RecordReader rows = reader.rows(opt)) {
        assertTrue(rows.nextBatch(batch), "No rows read out!");
        assertEquals(5, batch.size);
        assertFalse(rows.nextBatch(batch));
      }
    }
  }

  @Test
  public void testSkipBadBloomFilters() throws IOException {
    CheckFileWithSargs("bad_bloom_filter_1.6.11.orc", "ORC C++ 1.6.11");
    CheckFileWithSargs("bad_bloom_filter_1.6.0.orc", "ORC C++ ");
  }

  @Test
  public void testReadDecimalV2File() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "decimal64_v2_cplusplus.orc");
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      assertEquals("ORC C++ 1.8.0-SNAPSHOT", reader.getSoftwareVersion());
      OrcTail tail = reader.extractFileTail(fs, path, Long.MAX_VALUE);
      List<StripeStatistics> stats = tail.getStripeStatistics();
      assertEquals(1, stats.size());

      try (RecordReader rows = reader.rows()) {
        TypeDescription schema = reader.getSchema();
        assertEquals("struct<a:bigint,b:decimal(10,2),c:decimal(2,2),d:decimal(2,2),e:decimal(2,2)>",
            schema.toString());
        VectorizedRowBatch batch = schema.createRowBatchV2();
        assertTrue(rows.nextBatch(batch), "No rows read out!");
        assertEquals(10, batch.size);
        LongColumnVector col1 = (LongColumnVector) batch.cols[0];
        Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];
        Decimal64ColumnVector col3 = (Decimal64ColumnVector) batch.cols[2];
        Decimal64ColumnVector col4 = (Decimal64ColumnVector) batch.cols[3];
        Decimal64ColumnVector col5 = (Decimal64ColumnVector) batch.cols[4];
        for (int i = 0; i < batch.size; ++i) {
          assertEquals(17292380420L + i, col1.vector[i]);
          if (i == 0) {
            long scaleNum = (long) Math.pow(10, col2.scale);
            assertEquals(164.16 * scaleNum, col2.vector[i]);
          } else {
            assertEquals(col2.vector[i - 1] * 2, col2.vector[i]);
          }
          assertEquals(col3.vector[i] + col4.vector[i], col5.vector[i]);
        }
        assertFalse(rows.nextBatch(batch));
      }
    }
  }

  @Test
  public void testExtractFileTailIndexOutOfBoundsException() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "demo-11-none.orc");
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
            OrcFile.readerOptions(conf).filesystem(fs))) {
      OrcTail tail = reader.extractFileTail(fs, path, Long.MAX_VALUE);
      ByteBuffer tailBuffer = tail.getSerializedTail();

      OrcTail extractedTail = ReaderImpl.extractFileTail(tailBuffer, fileStatus.getLen(), fileStatus.getModificationTime());

      assertEquals(tail.getFileLength(), extractedTail.getFileLength());
      assertEquals(tail.getFooter().getMetadataList(), extractedTail.getFooter().getMetadataList());
      assertEquals(tail.getFooter().getStripesList(), extractedTail.getFooter().getStripesList());
    }
  }

  @Test
  public void testWithoutCompressionBlockSize() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "TestOrcFile.testWithoutCompressionBlockSize.orc");
    FileSystem fs = path.getFileSystem(conf);
    try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
            OrcFile.readerOptions(conf).filesystem(fs))) {
      try (RecordReader rows = reader.rows()) {
        TypeDescription schema = reader.getSchema();
        assertEquals("bigint", schema.toString());
        VectorizedRowBatch batch = schema.createRowBatchV2();
        assertTrue(rows.nextBatch(batch), "No rows read out!");
        assertEquals(100, batch.size);
        LongColumnVector col1 = (LongColumnVector) batch.cols[0];
        for (int i = 0; i < batch.size; ++i) {
          assertEquals(1L + i, col1.vector[i]);
        }
        assertFalse(rows.nextBatch(batch));
      }
    }
  }

  @Test
  public void testSargSkipPickupGroupWithoutIndex() throws IOException {
    Configuration conf = new Configuration();
    // We use ORC files in two languages to test, the previous Java version could not work
    // well when orc.row.index.stride > 0 and orc.create.index=false, now it can skip these row groups.
    Path[] paths = new Path[] {
        // Writen by C++ API with schema struct<x:int,y:string> orc.row.index.stride=0
        new Path(workDir, "TestOrcFile.testSargSkipPickupGroupWithoutIndexCPlusPlus.orc"),
        // Writen by old Java API with schema struct<x:int,y:string> orc.row.index.stride=1000,orc.create.index=false
        new Path(workDir, "TestOrcFile.testSargSkipPickupGroupWithoutIndexJava.orc"),
    };
    for (Path path: paths) {
      FileSystem fs = path.getFileSystem(conf);
      try (ReaderImpl reader = (ReaderImpl) OrcFile.createReader(path,
          OrcFile.readerOptions(conf).filesystem(fs))) {

        SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startNot()
            .lessThan("x", PredicateLeaf.Type.LONG, 100L)
            .end().build();

        try (RecordReader rows = reader.rows(reader.options().searchArgument(sarg, new String[]{"x"}))) {
          TypeDescription schema = reader.getSchema();
          assertEquals("struct<x:int,y:string>", schema.toString());
          VectorizedRowBatch batch = schema.createRowBatchV2();
          assertTrue(rows.nextBatch(batch), "No rows read out!");
          assertEquals(1024, batch.size);
          LongColumnVector col1 = (LongColumnVector) batch.cols[0];
          for (int i = 0; i < batch.size; ++i) {
            assertEquals(i, col1.vector[i]);
          }
          assertTrue(rows.nextBatch(batch));
        }
      }
    }
  }
}
