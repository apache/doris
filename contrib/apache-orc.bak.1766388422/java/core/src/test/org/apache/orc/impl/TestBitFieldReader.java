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

import org.apache.orc.CompressionCodec;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBitFieldReader {

  public void runSeekTest(CompressionCodec codec) throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    final int COUNT = 16384;
    StreamOptions options = new StreamOptions(500);
    if (codec != null) {
      options.withCodec(codec, codec.getDefaultOptions());
    }
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", options, collect), 1);
    TestInStream.PositionCollector[] positions =
        new TestInStream.PositionCollector[COUNT];
    for(int i=0; i < COUNT; ++i) {
      positions[i] = new TestInStream.PositionCollector();
      out.getPosition(positions[i]);
      // test runs, non-runs
      if (i < COUNT / 2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(500)));
    for(int i=0; i < COUNT; ++i) {
      int x = in.next();
      if (i < COUNT / 2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
    }
    for(int i=COUNT-1; i >= 0; --i) {
      in.seek(positions[i]);
      int x = in.next();
      if (i < COUNT / 2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
    }
  }

  @Test
  public void testUncompressedSeek() throws Exception {
    runSeekTest(null);
  }

  @Test
  public void testCompressedSeek() throws Exception {
    runSeekTest(new ZlibCodec());
  }

  @Test
  public void testSkips() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", new StreamOptions(100), collect), 1);
    final int COUNT = 16384;
    for(int i=0; i < COUNT; ++i) {
      if (i < COUNT/2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    for(int i=0; i < COUNT; i += 5) {
      int x = in.next();
      if (i < COUNT/2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
      if (i < COUNT - 5) {
        in.skip(4);
      }
      in.skip(0);
    }
  }

  @Test
  public void testSeekSkip() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", new StreamOptions(100), collect), 1);
    final int COUNT = 256;
    TestInStream.PositionCollector posn = new TestInStream.PositionCollector();
    for(int i=0; i < COUNT; ++i) {
      if (i == 200) {
        out.getPosition(posn);
      }
      if (i < COUNT/2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    in.seek(posn);
    in.skip(10);
    for(int r = 210; r < COUNT; ++r) {
      int x = in.next();
      if (r < COUNT/2) {
        assertEquals(r & 1, x);
      } else {
        assertEquals((r/3) & 1, x);
      }
    }
  }
}
