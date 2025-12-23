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

import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

public class TestInStream {

  public static class OutputCollector implements PhysicalWriter.OutputReceiver {
    public DynamicByteArray buffer = new DynamicByteArray();

    @Override
    public void output(ByteBuffer buffer) {
      this.buffer.add(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      // PASS
    }
  }

  static class PositionCollector
      implements PositionProvider, PositionRecorder {
    private List<Long> positions = new ArrayList<>();
    private int index = 0;

    @Override
    public long getNext() {
      return positions.get(index++);
    }

    @Override
    public void addPosition(long offset) {
      positions.add(offset);
    }

    public void reset() {
      index = 0;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("position: ");
      for(int i=0; i < positions.size(); ++i) {
        if (i != 0) {
          builder.append(", ");
        }
        builder.append(positions.get(i));
      }
      return builder.toString();
    }
  }

  static byte[] getUncompressed(PositionCollector[] positions) throws IOException {
    OutputCollector collect = new OutputCollector();
    try (OutStream out = new OutStream("test", new StreamOptions(100), collect)) {
      for (int i = 0; i < 1024; ++i) {
        positions[i] = new PositionCollector();
        out.getPosition(positions[i]);
        out.write(i);
      }
      out.flush();
    }
    assertEquals(1024, collect.buffer.size());
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, collect.buffer.get(i));
    }
    return collect.buffer.get();
  }

  @Test
  public void testUncompressed() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getUncompressed(positions);
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, bytes[i]);
    }
    ByteBuffer inBuf = ByteBuffer.wrap(bytes);
    InStream in = InStream.create("test", new BufferChunk(inBuf, 0),
        0, inBuf.remaining());
    assertEquals("uncompressed stream test position: 0 length: 1024" +
                 " range: 0 offset: 0 position: 0 limit: 1024",
                 in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testUncompressedPartial() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getUncompressed(positions);
    ByteBuffer inBuf = ByteBuffer.allocate(3 * 1024);
    inBuf.position(123);
    inBuf.put(bytes);
    inBuf.clear();
    InStream in = InStream.create("test", new BufferChunk(inBuf, 33),
        156, 1024);
    assertEquals("uncompressed stream test position: 0 length: 1024" +
                     " range: 0 offset: 33 position: 123 limit: 1147",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x, "value " + i);
    }
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read(), "value " + i);
    }
  }

  static byte[] getEncrypted(PositionCollector[] positions,
                             byte[] key,
                             byte[] iv,
                             EncryptionAlgorithm algorithm,
                             int ROW_COUNT,
                             long DATA_CONST) throws IOException {
    OutputCollector collect = new OutputCollector();
    for(int i=0; i < key.length; ++i) {
      key[i] = (byte) i;
    }
    Key decryptKey = new SecretKeySpec(key, algorithm.getAlgorithm());
    StreamOptions writerOptions = new StreamOptions(100)
                                      .withEncryption(algorithm, decryptKey);
    writerOptions.modifyIv(CryptoUtils.modifyIvForStream(0,
        OrcProto.Stream.Kind.DATA, 1));
    System.arraycopy(writerOptions.getIv(), 0, iv, 0, iv.length);
    try (OutStream out = new OutStream("test", writerOptions, collect);
         DataOutputStream outStream = new DataOutputStream(out)) {
      for (int i = 0; i < ROW_COUNT; ++i) {
        positions[i] = new PositionCollector();
        out.getPosition(positions[i]);
        outStream.writeLong(i * DATA_CONST);
      }
      out.flush();
    }
    byte[] result = collect.buffer.get();
    assertEquals(ROW_COUNT * 8L, result.length);
    return result;
  }

  @Test
  public void testEncrypted() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] rawKey = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    byte[] bytes = getEncrypted(positions, rawKey, iv, algorithm, ROW_COUNT,
        DATA_CONST);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    int[] rangeSizes = {1965, ROW_COUNT * 8 - 1965 - 15, 15};
    int offset = 0;
    BufferChunkList list = new BufferChunkList();
    for(int size: rangeSizes) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(bytes, offset, size);
      buffer.flip();
      list.add(new BufferChunk(buffer, offset));
      offset += size;
    }

    try (InStream in = InStream.create("test", list.get(), 0, bytes.length,
            InStream.options().withEncryption(EncryptionAlgorithm.AES_CTR_128,
                new SecretKeySpec(rawKey, algorithm.getAlgorithm()), iv));
         DataInputStream inputStream = new DataInputStream(in)) {
      assertEquals("encrypted uncompressed stream test position: 0 length: 8192" +
                       " range: 0 offset: 0 position: 0 limit: 1965",
          in.toString());
      for (int i = 0; i < ROW_COUNT; ++i) {
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
      for (int i = ROW_COUNT - 1; i >= 0; --i) {
        in.seek(positions[i]);
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
    }
  }

  @Test
  public void testEncryptedPartial() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] rawKey = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    byte[] bytes = getEncrypted(positions, rawKey, iv, algorithm, ROW_COUNT,
        DATA_CONST);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    BufferChunkList list = new BufferChunkList();
    ByteBuffer buffer = ByteBuffer.allocate(2000);
    buffer.position(35);
    buffer.put(bytes, 0, 1965);
    buffer.clear();
    list.add(new BufferChunk(buffer, 0));

    int SECOND_SIZE = ROW_COUNT * 8 - 1965 - 15;
    buffer = ByteBuffer.allocate(SECOND_SIZE);
    buffer.put(bytes, 1965, buffer.remaining());
    buffer.clear();
    list.add(new BufferChunk(buffer, 2000));

    buffer = ByteBuffer.allocate(2000);
    buffer.put(bytes, 1965 + SECOND_SIZE, 15);
    buffer.clear();
    list.add(new BufferChunk(buffer, 2000 + SECOND_SIZE));

    try (InStream in = InStream.create("test", list.get(), 35, bytes.length,
            InStream.options().withEncryption(EncryptionAlgorithm.AES_CTR_128,
                new SecretKeySpec(rawKey, algorithm.getAlgorithm()), iv));
         DataInputStream inputStream = new DataInputStream(in)) {
      assertEquals("encrypted uncompressed stream test position: 0 length: 8192" +
                       " range: 0 offset: 0 position: 0 limit: 1965",
          in.toString());
      for (int i = 0; i < ROW_COUNT; ++i) {
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
      for (int i = ROW_COUNT - 1; i >= 0; --i) {
        in.seek(positions[i]);
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
    }
  }

  static byte[] getCompressedEncrypted(byte[] key,
                                       byte[] iv,
                                       PositionCollector[] positions,
                                       EncryptionAlgorithm algorithm,
                                       int ROW_COUNT,
                                       long DATA_CONST) throws IOException {
    OutputCollector collect = new OutputCollector();
    for(int i=0; i < key.length; ++i) {
      key[i] = (byte) i;
    }
    Key decryptKey = new SecretKeySpec(key, algorithm.getAlgorithm());
    CompressionCodec codec = new ZlibCodec();
    StreamOptions writerOptions = new StreamOptions(500)
                                      .withCodec(codec, codec.getDefaultOptions())
                                      .withEncryption(algorithm, decryptKey);
    writerOptions.modifyIv(CryptoUtils.modifyIvForStream(0,
        OrcProto.Stream.Kind.DATA, 1));
    System.arraycopy(writerOptions.getIv(), 0, iv, 0, iv.length);
    try (OutStream out = new OutStream("test", writerOptions, collect);
         DataOutputStream outStream = new DataOutputStream(out)) {
      for (int i = 0; i < ROW_COUNT; ++i) {
        positions[i] = new PositionCollector();
        out.getPosition(positions[i]);
        outStream.writeLong(i * DATA_CONST);
      }
      out.flush();
    }
    return collect.buffer.get();
  }

  @Test
  public void testCompressedEncrypted() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] key = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    byte[] bytes= getCompressedEncrypted(key, iv, positions, algorithm, ROW_COUNT, DATA_CONST);

    // currently 3957 bytes
    assertEquals(3957, bytes.length);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    int[] rangeSizes = {1998, bytes.length - 1998 - 15, 15};
    int offset = 0;
    BufferChunkList list = new BufferChunkList();
    for(int size: rangeSizes) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(bytes, offset, size);
      buffer.flip();
      list.add(new BufferChunk(buffer, offset));
      offset += size;
    }

    try (InStream in = InStream.create("test", list.get(), 0, bytes.length,
            InStream.options()
              .withCodec(new ZlibCodec()).withBufferSize(500)
              .withEncryption(algorithm, new SecretKeySpec(key,
                  algorithm.getAlgorithm()), iv));
         DataInputStream inputStream = new DataInputStream(in)) {
      assertEquals("encrypted compressed stream test position: 0 length: " +
                       bytes.length +
                       " range: 0 offset: 0 limit: 1998 range 0 = 0 to" +
                       " 1998;  range 1 = 1998 to " + (bytes.length - 15) +
                       ";  range 2 = " +
                       (bytes.length - 15) + " to " + bytes.length,
          in.toString());
      for (int i = 0; i < ROW_COUNT; ++i) {
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
      for (int i = ROW_COUNT - 1; i >= 0; --i) {
        in.seek(positions[i]);
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
    }
  }

  @Test
  public void testCompressedEncryptedPartial() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] key = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    byte[] bytes= getCompressedEncrypted(key, iv, positions, algorithm, ROW_COUNT, DATA_CONST);

    // currently 3957 bytes
    assertEquals(3957, bytes.length);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    BufferChunkList list = new BufferChunkList();
    ByteBuffer buffer = ByteBuffer.allocate(2000);
    buffer.position(2);
    buffer.put(bytes, 0 , 1998);
    buffer.clear();
    list.add(new BufferChunk(buffer, 100));

    int SECOND_SIZE = bytes.length - 1998 - 15;
    buffer = ByteBuffer.allocate(SECOND_SIZE);
    buffer.put(bytes, 1998, SECOND_SIZE);
    buffer.clear();
    list.add(new BufferChunk(buffer, 2100));

    buffer = ByteBuffer.allocate(1000);
    buffer.put(bytes, 1998 + SECOND_SIZE, 15);
    buffer.clear();
    list.add(new BufferChunk(buffer, 2100 + SECOND_SIZE));

    try (InStream in = InStream.create("test", list.get(), 102, bytes.length,
            InStream.options()
               .withCodec(new ZlibCodec()).withBufferSize(500)
                .withEncryption(algorithm, new SecretKeySpec(key,
                    algorithm.getAlgorithm()), iv));
         DataInputStream inputStream = new DataInputStream(in)) {
      assertEquals("encrypted compressed stream test position: 0 length: " +
                       bytes.length +
                       " range: 0 offset: 0 limit: 1998 range 0 = 100 to 2100;" +
                       "  range 1 = 2100 to 4044;  range 2 = 4044 to 5044",
          in.toString());
      for (int i = 0; i < ROW_COUNT; ++i) {
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
      for (int i = ROW_COUNT - 1; i >= 0; --i) {
        in.seek(positions[i]);
        assertEquals(i * DATA_CONST, inputStream.readLong(), "row " + i);
      }
    }
  }

  byte[] getCompressed(PositionCollector[] positions) throws IOException {
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(300)
                                .withCodec(codec, codec.getDefaultOptions());
    OutputCollector collect = new OutputCollector();
    try (OutStream out = new OutStream("test", options, collect)) {
      for (int i = 0; i < 1024; ++i) {
        positions[i] = new PositionCollector();
        out.getPosition(positions[i]);
        out.write(i);
      }
      out.flush();
      assertEquals("test", out.toString());
    }
    return collect.buffer.get();
  }

  @Test
  public void testCompressed() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getCompressed(positions);

    assertEquals(961, bytes.length);
    InStream in = InStream.create("test", new BufferChunk(ByteBuffer.wrap(bytes), 0), 0,
        bytes.length, InStream.options().withCodec(new ZlibCodec()).withBufferSize(300));
    assertEquals("compressed stream test position: 0 length: 961 range: 0" +
                 " offset: 0 limit: 961 range 0 = 0 to 961",
                 in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  private long seekPosition(long prevPos,
                            PositionCollector[] positions,
                            int posIdx,
                            InStream in,
                            boolean needsSeek)
    throws IOException {
    if (needsSeek) {
      assertNotEquals(prevPos, positions[posIdx].getNext());
    } else {
      assertEquals(prevPos, positions[posIdx].getNext());
    }
    positions[posIdx].reset();
    ByteBuffer c = ((InStream.CompressedStream) in).compressed;
    in.seek(positions[posIdx]);
    assertEquals(posIdx & 0xff, in.read());
    if (needsSeek) {
      assertNotSame(c, ((InStream.CompressedStream) in).compressed);
    } else {
      assertSame(c, ((InStream.CompressedStream) in).compressed);
    }
    positions[posIdx].reset();
    return positions[posIdx].getNext();
  }

  @Test
  public void testCompressedSeeks() throws Exception {
    // We test two scenarios one where the stream is perfectly aligned with the DiskRange and the
    // other where it requires an offset
    for (int offset : new int[]{0, 10}) {
      int compValues = 1024;
      int origValues = 100;
      PositionCollector[] positions = new PositionCollector[compValues + origValues];
      byte[] compBytes = getCompressed(positions);
      assertEquals(961, compBytes.length);
      // Add an original chunk at the end
      byte[] bytes = new byte[compBytes.length + 3 + origValues + offset];
      System.arraycopy(compBytes, 0, bytes, offset, compBytes.length);
      int startPos = offset + compBytes.length;
      // Write original header
      bytes[startPos] = (byte) ((origValues << 1) + 1);
      bytes[startPos + 1] = (byte) (origValues >> 7);
      bytes[startPos + 2] = (byte) (origValues >> 15);
      for (int i = 0; i < 100; i++) {
        positions[compValues + i] = new PositionCollector();
        positions[compValues + i].addPosition(compBytes.length);
        positions[compValues + i].addPosition(i);
        bytes[startPos + 3 + i] = (byte) (compValues + i);
      }
      InStream in = InStream.create("test", new BufferChunk(ByteBuffer.wrap(bytes), 0), offset,
                                    compBytes.length + 3 + origValues,
                                    InStream.options()
                                      .withCodec(new ZlibCodec())
                                      .withBufferSize(300));
      assertEquals("compressed stream test position: 0 length: 1064 range: 0" +
                   String.format(" offset: %d limit: %d range 0 = 0 to %d",
                                 offset,
                                 bytes.length,
                                 bytes.length),
                   in.toString());

      // Position to the last
      long currPos = positions[positions.length - 1].getNext();
      positions[positions.length - 1].reset();
      in.seek(positions[positions.length - 1]);

      // Seek to the first should reposition compressed
      currPos = seekPosition(currPos, positions, 0, in, true);
      // Seek to next position should not require a seek
      currPos = seekPosition(currPos, positions, 1, in, false);

      // Seek to 301 which should require a seek
      currPos = seekPosition(currPos, positions, 301, in, true);
      // Seek to next position should not require a seek
      seekPosition(currPos, positions, 302, in, false);

      // Seek to 601 which should require a seek
      currPos = seekPosition(currPos, positions, 601, in, true);
      // Seek to next position should not require a seek
      seekPosition(currPos, positions, 602, in, false);

      // Seek to 1024 which should seek to original
      currPos = seekPosition(currPos, positions, 1024, in, true);
      // Seek to next position should not require a seek
      seekPosition(currPos, positions, 1025, in, false);
      seekPosition(currPos, positions, 1026, in, false);
    }
  }

  @Test
  public void testInvalidSeek() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getCompressed(positions);

    assertEquals(961, bytes.length);
    InStream in = InStream.create("test", new BufferChunk(ByteBuffer.wrap(bytes), 0), 0,
                                  bytes.length, InStream.options().withCodec(new ZlibCodec()).withBufferSize(300));
    assertEquals("compressed stream test position: 0 length: 961 range: 0" +
                 " offset: 0 limit: 961 range 0 = 0 to 961",
                 in.toString());

    PositionCollector invalidPosition = new PositionCollector();
    invalidPosition.addPosition(-1);
    invalidPosition.addPosition(0);
    in.seek(invalidPosition);
    assertEquals(0, in.read());
    assertEquals(1, in.read());
  }

  @Test
  public void testCompressedPartial() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getCompressed(positions);

    assertEquals(961, bytes.length);
    ByteBuffer buffer = ByteBuffer.allocate(1500);
    buffer.position(39);
    buffer.put(bytes, 0, bytes.length);
    buffer.clear();

    InStream in = InStream.create("test", new BufferChunk(buffer, 100), 139,
        bytes.length, InStream.options().withCodec(new ZlibCodec()).withBufferSize(300));
    assertEquals("compressed stream test position: 0 length: 961 range: 0" +
                     " offset: 39 limit: 1000 range 0 = 100 to 1600",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCorruptStream() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(500)
                                .withCodec(codec, codec.getDefaultOptions());
    try (OutStream out = new OutStream("test", options, collect)) {
      for (int i = 0; i < 1024; ++i) {
        out.write(i);
      }
      out.flush();
    }

    // now try to read the stream with a buffer that is too small
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", new BufferChunk(inBuf, 0), 0,
        inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(100));
    byte[] contents = new byte[1024];
    try {
      in.read(contents);
      fail();
    } catch(IllegalArgumentException iae) {
      // EXPECTED
    }

    // make a corrupted header
    inBuf.clear();
    inBuf.put((byte) 32);
    inBuf.put((byte) 0);
    inBuf.flip();
    in = InStream.create("test2", new BufferChunk(inBuf, 0), 0,
        inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(300));
    try {
      in.read();
      fail();
    } catch (IllegalStateException ise) {
      // EXPECTED
    }
  }

  @Test
  public void testDisjointBuffers() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(400)
                                .withCodec(codec, codec.getDefaultOptions());
    PositionCollector[] positions = new PositionCollector[1024];
    try (OutStream out = new OutStream("test", options, collect);
         DataOutputStream stream = new DataOutputStream(out)) {

      for (int i = 0; i < 1024; ++i) {
        positions[i] = new PositionCollector();
        out.getPosition(positions[i]);
        stream.writeInt(i);
      }
      out.flush();
    }
    assertEquals(1674, collect.buffer.size());
    ByteBuffer[] inBuf = new ByteBuffer[3];
    inBuf[0] = ByteBuffer.allocate(500);
    inBuf[1] = ByteBuffer.allocate(1200);
    inBuf[2] = ByteBuffer.allocate(500);
    collect.buffer.setByteBuffer(inBuf[0], 0, 483);
    collect.buffer.setByteBuffer(inBuf[1], 483, 1625 - 483);
    collect.buffer.setByteBuffer(inBuf[2], 1625, 1674 - 1625);

    BufferChunkList buffers = new BufferChunkList();
    int offset = 0;
    for(ByteBuffer buffer: inBuf) {
      buffer.flip();
      buffers.add(new BufferChunk(buffer, offset));
      offset += buffer.remaining();
    }
    InStream.StreamOptions inOptions = InStream.options()
        .withCodec(codec).withBufferSize(400);
    try (InStream in = InStream.create("test", buffers.get(), 0, 1674, inOptions);
         DataInputStream inStream = new DataInputStream(in)) {
      assertEquals("compressed stream test position: 0 length: 1674 range: 0" +
                       " offset: 0 limit: 483 range 0 = 0 to 483;" +
                       "  range 1 = 483 to 1625;  range 2 = 1625 to 1674",
          in.toString());
      for (int i = 0; i < 1024; ++i) {
        int x = inStream.readInt();
        assertEquals(i, x);
      }
      assertEquals(0, in.available());
      for (int i = 1023; i >= 0; --i) {
        in.seek(positions[i]);
        assertEquals(i, inStream.readInt());
      }
    }
    buffers.clear();
    buffers.add(new BufferChunk(inBuf[1], 483));
    buffers.add(new BufferChunk(inBuf[2], 1625));
    try (InStream in = InStream.create("test", buffers.get(), 0, 1674, inOptions);
         DataInputStream inStream = new DataInputStream(in)) {
      positions[303].reset();
      in.seek(positions[303]);
      for (int i = 303; i < 1024; ++i) {
        assertEquals(i, inStream.readInt());
      }
    }
    buffers.clear();
    buffers.add(new BufferChunk(inBuf[0], 0));
    buffers.add(new BufferChunk(inBuf[2], 1625));
    try (InStream in = InStream.create("test", buffers.get(), 0, 1674, inOptions);
         DataInputStream inStream = new DataInputStream(in)) {
      positions[1001].reset();
      for (int i = 0; i < 300; ++i) {
        assertEquals(i, inStream.readInt());
      }
      in.seek(positions[1001]);
      for (int i = 1001; i < 1024; ++i) {
        assertEquals(i, inStream.readInt());
      }
    }
  }

  @Test
  public void testUncompressedDisjointBuffers() throws Exception {
    OutputCollector collect = new OutputCollector();
    PositionCollector[] positions = new PositionCollector[1024];
    try (OutStream out = new OutStream("test", new StreamOptions(400), collect);
         DataOutputStream stream = new DataOutputStream(out)) {
      for (int i = 0; i < 1024; ++i) {
        positions[i] = new PositionCollector();
        out.getPosition(positions[i]);
        stream.writeInt(i);
      }
      out.flush();
    }
    assertEquals(4096, collect.buffer.size());
    ByteBuffer[] inBuf = new ByteBuffer[3];
    inBuf[0] = ByteBuffer.allocate(1100);
    inBuf[1] = ByteBuffer.allocate(2200);
    inBuf[2] = ByteBuffer.allocate(1100);
    collect.buffer.setByteBuffer(inBuf[0], 0, 1024);
    collect.buffer.setByteBuffer(inBuf[1], 1024, 2048);
    collect.buffer.setByteBuffer(inBuf[2], 3072, 1024);

    for(ByteBuffer buffer: inBuf) {
      buffer.flip();
    }
    BufferChunkList buffers = new BufferChunkList();
    buffers.add(new BufferChunk(inBuf[0], 0));
    buffers.add(new BufferChunk(inBuf[1], 1024));
    buffers.add(new BufferChunk(inBuf[2], 3072));
    try (InStream in = InStream.create("test", buffers.get(), 0, 4096);
         DataInputStream inStream = new DataInputStream(in)) {
      assertEquals("uncompressed stream test position: 0 length: 4096" +
                       " range: 0 offset: 0 position: 0 limit: 1024",
          in.toString());
      for (int i = 0; i < 1024; ++i) {
        int x = inStream.readInt();
        assertEquals(i, x);
      }
      assertEquals(0, in.available());
      for (int i = 1023; i >= 0; --i) {
        in.seek(positions[i]);
        assertEquals(i, inStream.readInt());
      }
    }

    buffers.clear();
    buffers.add(new BufferChunk(inBuf[1], 1024));
    buffers.add(new BufferChunk(inBuf[2], 3072));
    try (InStream in = InStream.create("test", buffers.get(), 0, 4096);
         DataInputStream inStream = new DataInputStream(in)) {
      positions[256].reset();
      in.seek(positions[256]);
      for (int i = 256; i < 1024; ++i) {
        assertEquals(i, inStream.readInt());
      }
    }

    buffers.clear();
    buffers.add(new BufferChunk(inBuf[0], 0));
    buffers.add(new BufferChunk(inBuf[2], 3072));
    try (InStream in = InStream.create("test", buffers.get(), 0, 4096);
         DataInputStream inStream = new DataInputStream(in)) {
      positions[768].reset();
      for (int i = 0; i < 256; ++i) {
        assertEquals(i, inStream.readInt());
      }
      in.seek(positions[768]);
      for (int i = 768; i < 1024; ++i) {
        assertEquals(i, inStream.readInt());
      }
    }
  }

  @Test
  public void testEmptyDiskRange() throws IOException {
    DiskRangeList range = new BufferChunk(ByteBuffer.allocate(0), 0);
    try (InStream stream = new InStream.UncompressedStream("test", range, 0, 0)) {
      assertEquals(0, stream.available());
      stream.seek(new PositionProvider() {
        @Override
        public long getNext() {
          return 0;
        }
      });
      assertEquals(0, stream.available());
    }
  }

  private static byte[] input(int... data) {
    byte[] result = new byte[data.length];
    for(int i = 0; i < data.length; ++i) {
      result[i] = (byte) data[i];
    }
    return result;
  }

  // a zlib stream of 16 sequences of [0..255]
  private static final byte[] compressed = input(
       106,    2,    0,   99,   96,  100,   98,  102,   97,  101,   99,  -25,
       -32,  -28,  -30,  -26,  -31,  -27,  -29,   23,   16,   20,   18,   22,
        17,   21,   19, -105, -112, -108, -110, -106, -111, -107, -109,   87,
        80,   84,   82,   86,   81,   85,   83,  -41,  -48,  -44,  -46,  -42,
       -47,  -43,  -45,   55,   48,   52,   50,   54,   49,   53,   51,  -73,
       -80,  -76,  -78,  -74,  -79,  -75,  -77,  119,  112,  116,  114,  118,
       113,  117,  115,   -9,  -16,  -12,  -14,  -10,  -15,  -11,  -13,   15,
         8,   12,   10,   14,    9,   13,   11, -113, -120, -116, -118, -114,
      -119, -115, -117,   79,   72,   76,   74,   78,   73,   77,   75,  -49,
       -56,  -52,  -54,  -50,  -55,  -51,  -53,   47,   40,   44,   42,   46,
        41,   45,   43,  -81,  -88,  -84,  -86,  -82,  -87,  -83,  -85,  111,
       104,  108,  106,  110,  105,  109,  107,  -17,  -24,  -20,  -22,  -18,
       -23,  -19,  -21,  -97,   48,  113,  -46,  -28,   41,   83,  -89,   77,
       -97,   49,  115,  -42,  -20,   57,  115,  -25,  -51,   95,  -80,  112,
       -47,  -30,   37,   75, -105,   45,   95,  -79,  114,  -43,  -22,   53,
       107,  -41,  -83,  -33,  -80,  113,  -45,  -26,   45,   91,  -73,  109,
       -33,  -79,  115,  -41,  -18,   61,  123,   -9,  -19,   63,  112,  -16,
       -48,  -31,   35,   71, -113,   29,   63,  113,  -14,  -44,  -23,   51,
       103,  -49,  -99,  -65,  112,  -15,  -46,  -27,   43,   87,  -81,   93,
       -65,  113,  -13,  -42,  -19,   59,  119,  -17,  -35,  127,  -16,  -16,
       -47,  -29,   39,   79,  -97,   61,  127,  -15,  -14,  -43,  -21,   55,
       111,  -33,  -67,   -1,  -16,  -15,  -45,  -25,   47,   95,  -65,  125,
        -1,  -15,  -13,  -41,  -17,   63,  127,   -1,   -3,  103,   24,  -11,
        -1,  -88,   -1,   71,   -3,   63,  -22,   -1,   81,   -1, -113,   -6,
       127,  -44,   -1,  -93,   -2,   31,  -11,   -1,  -88,   -1,   71,   -3,
        63,  -22,   -1,   81,   -1, -113,   -6,  127,    4,   -8,   31,    0);

  @Test
  public void testMultiRangeCompressed() throws IOException {
    // Set up an initial buffer of PREVIOUS_LENGTH followed by our stream
    // at START.
    final long START = 1_000_000_000;
    final int PREVIOUS_LENGTH = 3000;
    BufferChunkList list = new BufferChunkList();
    byte[] previous = new byte[PREVIOUS_LENGTH];
    Arrays.fill(previous, (byte) -1);
    list.add(new BufferChunk(ByteBuffer.wrap(previous), START - PREVIOUS_LENGTH));
    list.add(new BufferChunk(ByteBuffer.wrap(compressed), START));
    InStream.StreamOptions options =
        InStream.options().withCodec(new ZlibCodec()).withBufferSize(4096);
    InStream inStream = InStream.create("test", list.get(), START, 4096, options);
    byte[] inBuffer = new byte[4096];
    assertEquals(4096, inStream.read(inBuffer));
    for(int i=0; i < inBuffer.length; ++i) {
      assertEquals((byte)i, inBuffer[i], "position " + i);
    }
  }

  @Test
  public void testExtraFrontUncompressed() throws IOException {
    // Set up a stream that starts at START, which is divided in to regions
    // of CHUNK_LENGTH. There are two EXTRA_FRONT byte buffers in front of the
    // stream.
    final long START = 1_000_000_000;
    final int EXTRA_FRONT = 3_000;
    final int CHUNK_LENGTH = 100;
    final int STREAM_LENGTH = 4096;

    BufferChunkList list = new BufferChunkList();
    list.add(new BufferChunk(ByteBuffer.allocate(EXTRA_FRONT),
        START - 2 * EXTRA_FRONT));
    byte[] extraFront = new byte[EXTRA_FRONT + CHUNK_LENGTH];
    Arrays.fill(extraFront, (byte) -1);
    for(int i=0; i < CHUNK_LENGTH; ++i) {
      extraFront[EXTRA_FRONT + i] = (byte) i;
    }
    list.add(new BufferChunk(ByteBuffer.wrap(extraFront), START - EXTRA_FRONT));
    byte[] expected = new byte[STREAM_LENGTH];
    for(int i=CHUNK_LENGTH; i < expected.length; ++i) {
      expected[i] = (byte) i;
    }
    int posn = CHUNK_LENGTH;
    while (posn <= expected.length) {
      list.add(new BufferChunk(
          ByteBuffer.wrap(expected, posn,
              Math.min(CHUNK_LENGTH, expected.length - posn)),
          START + posn));
      posn += CHUNK_LENGTH;
    }

    // now set up the stream to read it
    InStream.StreamOptions options = InStream.options();
    InStream inStream = InStream.create("test", list.get(), START, STREAM_LENGTH,
        options);

    // ensure the data is correct
    byte[] inBuffer = new byte[STREAM_LENGTH];
    posn = 0;
    int read = inStream.read(inBuffer);
    while (read != -1) {
      assertEquals(Math.min(STREAM_LENGTH - posn, CHUNK_LENGTH), read, "Read length at " + posn);
      for(int i=0; i < read; ++i) {
        assertEquals((byte)(posn + i), inBuffer[i], "posn " + posn + " + " + i);
      }
      posn += read;
      read = inStream.read(inBuffer);
    }
  }

  @Test
  public void testExtraFrontCompressed() throws IOException {
    // Set up a stream that starts at START, which is divided in to regions
    // of CHUNK_LENGTH. There are two EXTRA_FRONT byte buffers in front of the
    // stream.
    final long START = 1_000_000_000;
    final int EXTRA_FRONT = 3_000;
    final int CHUNK_LENGTH = 100;

    BufferChunkList list = new BufferChunkList();
    list.add(new BufferChunk(ByteBuffer.allocate(EXTRA_FRONT),
        START - 2 * EXTRA_FRONT));
    byte[] extraFront = new byte[EXTRA_FRONT + CHUNK_LENGTH];
    Arrays.fill(extraFront, (byte) -1);
    System.arraycopy(compressed, 0, extraFront, EXTRA_FRONT, CHUNK_LENGTH);
    list.add(new BufferChunk(ByteBuffer.wrap(extraFront), START - EXTRA_FRONT));
    int posn = CHUNK_LENGTH;
    while (posn < compressed.length) {
      list.add(new BufferChunk(
          ByteBuffer.wrap(compressed, posn,
              Math.min(CHUNK_LENGTH, compressed.length - posn)),
          START + posn));
      posn += CHUNK_LENGTH;
    }

    // now set up the stream to read it
    InStream.StreamOptions options =
        InStream.options().withCodec(new ZlibCodec()).withBufferSize(4096);
    InStream inStream = InStream.create("test", list.get(), START, 4096, options);

    // ensure the data is correct
    byte[] inBuffer = new byte[4096];
    assertEquals(4096, inStream.read(inBuffer));
    for(int i=0; i < inBuffer.length; ++i) {
      assertEquals((byte)i, inBuffer[i], "position " + i);
    }
  }

  @Test
  public void testMultiRangeCompressHeader() throws IOException {
    // Set up a buffer where the first 5 bytes are each a chunk and then the
    // rest of the stream follows.
    final long START = 1_000_000_000;
    BufferChunkList list = new BufferChunkList();
    for(int i=0; i < 5; ++i) {
      list.add(new BufferChunk(ByteBuffer.wrap(compressed, i, 1), START + i));
    }
    list.add(new BufferChunk(
        ByteBuffer.wrap(compressed, 5, compressed.length - 5), START + 5));
    InStream.StreamOptions options =
        InStream.options().withCodec(new ZlibCodec()).withBufferSize(4096);
    InStream inStream = InStream.create("test", list.get(), START, 4096, options);
    byte[] inBuffer = new byte[4096];
    assertEquals(4096, inStream.read(inBuffer));
    for(int i=0; i < inBuffer.length; ++i) {
      assertEquals((byte)i, inBuffer[i], "position " + i);
    }
  }

  private static final byte[] uncompressed = input(
          0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  
  @Test
  public void testStreamResetWithIncreasedLength() throws IOException {
    // Set up an initial buffer of PREVIOUS_LENGTH followed by our stream
    // at START.
    final long START = 1_000;
    final int PREVIOUS_LENGTH = 30;
    BufferChunkList list = new BufferChunkList();
    byte[] previous = new byte[PREVIOUS_LENGTH];
    Arrays.fill(previous, (byte) -1);
    list.add(new BufferChunk(ByteBuffer.wrap(previous), START - PREVIOUS_LENGTH));
    list.add(new BufferChunk(ByteBuffer.wrap(uncompressed), START));
    // Creating a stream of 10 bytes, but with a length of 5
    InStream inStream = InStream.create("test", list.get(), START, 5, new InStream.StreamOptions());
    // Resetting the stream with the increased length
    inStream.reset(list.get(), 10);
    // Reading the stream and expecting to read 10 bytes
    byte[] inBuffer = new byte[10];
    assertEquals(10, inStream.read(inBuffer));
  }

  @Test
  public void testStreamResetWithoutIncreasedLength() throws IOException {
    // Set up an initial buffer of PREVIOUS_LENGTH followed by our stream
    // at START.
    final long START = 1_000;
    final int PREVIOUS_LENGTH = 30;
    BufferChunkList list = new BufferChunkList();
    byte[] previous = new byte[PREVIOUS_LENGTH];
    Arrays.fill(previous, (byte) -1);
    list.add(new BufferChunk(ByteBuffer.wrap(previous), START - PREVIOUS_LENGTH));
    list.add(new BufferChunk(ByteBuffer.wrap(uncompressed), START));
    // Creating a stream of 10 bytes, but with a shorter length of 5
    InStream inStream = InStream.create("test", list.get(), START, 5, new InStream.StreamOptions());
    // Resetting the stream without updating its length
    inStream.reset(list.get());
    // Reading the stream and expecting to read 5 bytes as the initial stream length
    byte[] inBuffer = new byte[5];
    assertEquals(5, inStream.read(inBuffer));
  }
}
