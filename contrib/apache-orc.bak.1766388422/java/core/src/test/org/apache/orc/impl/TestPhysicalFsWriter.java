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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPhysicalFsWriter {

  final Configuration conf = new Configuration();

  static class MemoryOutputStream extends OutputStream {
    private final List<byte[]> contents;

    MemoryOutputStream(List<byte[]> contents) {
      this.contents = contents;
    }

    @Override
    public void write(int b) {
      contents.add(new byte[]{(byte) b});
    }

    @Override
    public void write(byte[] a, int offset, int length) {
      byte[] buffer = new byte[length];
      System.arraycopy(a, offset, buffer, 0, length);
      contents.add(buffer);
    }
  }

  static class MemoryFileSystem extends FileSystem {

    @Override
    public URI getUri() {
      try {
        return new URI("test:///");
      } catch (URISyntaxException e) {
        throw new IllegalStateException("bad url", e);
      }
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite, int bufferSize,
                                     short replication, long blockSize,
                                     Progressable progress) throws IOException {
      List<byte[]> contents = new ArrayList<>();
      fileContents.put(f, contents);
      return new FSDataOutputStream(new MemoryOutputStream(contents), null);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) {
      throw new UnsupportedOperationException("append not supported");
    }

    @Override
    public boolean rename(Path src, Path dst) {
      boolean result = fileContents.containsKey(src) &&
          !fileContents.containsKey(dst);
      if (result) {
        List<byte[]> contents = fileContents.remove(src);
        fileContents.put(dst, contents);
      }
      return result;
    }

    @Override
    public boolean delete(Path f, boolean recursive) {
      boolean result = fileContents.containsKey(f);
      fileContents.remove(f);
      return result;
    }

    @Override
    public FileStatus[] listStatus(Path f) {
      return new FileStatus[]{getFileStatus(f)};
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
      currentWorkingDirectory = new_dir;
    }

    @Override
    public Path getWorkingDirectory() {
      return currentWorkingDirectory;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) {
      List<byte[]> contents = fileContents.get(f);
      if (contents != null) {
        long sum = 0;
        for(byte[] b: contents) {
          sum += b.length;
        }
        return new FileStatus(sum, false, 1, 256 * 1024, 0, f);
      }
      return null;
    }

    private final Map<Path, List<byte[]>> fileContents = new HashMap<>();
    private Path currentWorkingDirectory = new Path("/");
  }

  @Test
  public void testStripePadding() throws IOException {
    TypeDescription schema = TypeDescription.fromString("int");
    OrcFile.WriterOptions opts =
        OrcFile.writerOptions(conf)
            .stripeSize(32 * 1024)
            .blockSize(64 * 1024)
            .compress(CompressionKind.NONE)
            .setSchema(schema);
    MemoryFileSystem fs = new MemoryFileSystem();
    PhysicalFsWriter writer = new PhysicalFsWriter(fs, new Path("test1.orc"),
        opts);
    writer.writeHeader();
    StreamName stream0 = new StreamName(0, OrcProto.Stream.Kind.DATA);
    PhysicalWriter.OutputReceiver output = writer.createDataStream(stream0);
    byte[] buffer = new byte[1024];
    for(int i=0; i < buffer.length; ++i) {
      buffer[i] = (byte) i;
    }
    for(int i=0; i < 63; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    OrcProto.StripeInformation.Builder dirEntry =
        OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // check to make sure that it laid it out without padding
    assertEquals(0L, dirEntry.getIndexLength());
    assertEquals(63 * 1024L, dirEntry.getDataLength());
    assertEquals(3, dirEntry.getOffset());
    for(int i=0; i < 62; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    footer = OrcProto.StripeFooter.newBuilder();
    dirEntry = OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // the second one should pad
    assertEquals(64 * 1024, dirEntry.getOffset());
    assertEquals(62 * 1024, dirEntry.getDataLength());
    long endOfStripe = dirEntry.getOffset() + dirEntry.getIndexLength() +
        dirEntry.getDataLength() + dirEntry.getFooterLength();

    for(int i=0; i < 3; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    footer = OrcProto.StripeFooter.newBuilder();
    dirEntry = OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // the third one should be over the padding limit
    assertEquals(endOfStripe, dirEntry.getOffset());
    assertEquals(3 * 1024, dirEntry.getDataLength());
  }

  @Test
  public void testNoStripePadding() throws IOException {
    TypeDescription schema = TypeDescription.fromString("int");
    OrcFile.WriterOptions opts =
        OrcFile.writerOptions(conf)
            .blockPadding(false)
            .stripeSize(32 * 1024)
            .blockSize(64 * 1024)
            .compress(CompressionKind.NONE)
            .setSchema(schema);
    MemoryFileSystem fs = new MemoryFileSystem();
    PhysicalFsWriter writer = new PhysicalFsWriter(fs, new Path("test1.orc"),
        opts);
    writer.writeHeader();
    StreamName stream0 = new StreamName(0, OrcProto.Stream.Kind.DATA);
    PhysicalWriter.OutputReceiver output = writer.createDataStream(stream0);
    byte[] buffer = new byte[1024];
    for(int i=0; i < buffer.length; ++i) {
      buffer[i] = (byte) i;
    }
    for(int i=0; i < 63; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    OrcProto.StripeInformation.Builder dirEntry =
        OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // check to make sure that it laid it out without padding
    assertEquals(0L, dirEntry.getIndexLength());
    assertEquals(63 * 1024L, dirEntry.getDataLength());
    assertEquals(3, dirEntry.getOffset());
    long endOfStripe = dirEntry.getOffset() + dirEntry.getDataLength()
        + dirEntry.getFooterLength();
    for(int i=0; i < 62; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    footer = OrcProto.StripeFooter.newBuilder();
    dirEntry = OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // no padding, because we turned it off
    assertEquals(endOfStripe, dirEntry.getOffset());
    assertEquals(62 * 1024, dirEntry.getDataLength());
  }

  static class MockHadoopShim implements HadoopShims {
    long lastShortBlock = -1;

    @Override
    public DirectDecompressor getDirectDecompressor(DirectCompressionType codec) {
      return null;
    }

    @Override
    public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                                ByteBufferPoolShim pool) {
      return null;
    }

    @Override
    public boolean endVariableLengthBlock(OutputStream output) throws IOException {
      if (output instanceof FSDataOutputStream) {
        lastShortBlock = ((FSDataOutputStream) output).getPos();
        return true;
      }
      return false;
    }

    @Override
    public KeyProvider getHadoopKeyProvider(Configuration conf, Random random) {
      return null;
    }
  }

  @Test
  public void testShortBlock() throws IOException {
    MockHadoopShim shim = new MockHadoopShim();
    TypeDescription schema = TypeDescription.fromString("int");
    OrcFile.WriterOptions opts =
        OrcFile.writerOptions(conf)
            .blockPadding(false)
            .stripeSize(32 * 1024)
            .blockSize(64 * 1024)
            .compress(CompressionKind.NONE)
            .setSchema(schema)
            .setShims(shim)
            .writeVariableLengthBlocks(true);
    MemoryFileSystem fs = new MemoryFileSystem();
    PhysicalFsWriter writer = new PhysicalFsWriter(fs, new Path("test1.orc"),
        opts);
    writer.writeHeader();
    StreamName stream0 = new StreamName(0, OrcProto.Stream.Kind.DATA);
    PhysicalWriter.OutputReceiver output = writer.createDataStream(stream0);
    byte[] buffer = new byte[1024];
    for(int i=0; i < buffer.length; ++i) {
      buffer[i] = (byte) i;
    }
    for(int i=0; i < 63; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    OrcProto.StripeInformation.Builder dirEntry =
        OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // check to make sure that it laid it out without padding
    assertEquals(0L, dirEntry.getIndexLength());
    assertEquals(63 * 1024L, dirEntry.getDataLength());
    assertEquals(3, dirEntry.getOffset());
    long endOfStripe = dirEntry.getOffset() + dirEntry.getDataLength()
        + dirEntry.getFooterLength();
    for(int i=0; i < 62; ++i) {
      output.output(ByteBuffer.wrap(buffer));
    }
    footer = OrcProto.StripeFooter.newBuilder();
    dirEntry = OrcProto.StripeInformation.newBuilder();
    writer.finalizeStripe(footer, dirEntry);
    // we should get a short block and no padding
    assertEquals(endOfStripe, dirEntry.getOffset());
    assertEquals(62 * 1024, dirEntry.getDataLength());
    assertEquals(endOfStripe, shim.lastShortBlock);
  }
}
