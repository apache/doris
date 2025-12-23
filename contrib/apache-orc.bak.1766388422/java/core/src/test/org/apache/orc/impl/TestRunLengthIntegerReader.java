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
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRunLengthIntegerReader {

  public void runSeekTest(CompressionCodec codec) throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    StreamOptions options = new StreamOptions(1000);
    if (codec != null) {
      options.withCodec(codec, codec.getDefaultOptions());
    }
    RunLengthIntegerWriter out = new RunLengthIntegerWriter(
        new OutStream("test", options, collect), true);
    TestInStream.PositionCollector[] positions =
        new TestInStream.PositionCollector[4096];
    Random random = new Random(99);
    int[] junk = new int[2048];
    for(int i=0; i < junk.length; ++i) {
      junk[i] = random.nextInt();
    }
    for(int i=0; i < 4096; ++i) {
      positions[i] = new TestInStream.PositionCollector();
      out.getPosition(positions[i]);
      // test runs, incrementing runs, non-runs
      if (i < 1024) {
        out.write(i/4);
      } else if (i < 2048) {
        out.write(2*i);
      } else {
        out.write(junk[i-2048]);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    RunLengthIntegerReader in = new RunLengthIntegerReader(InStream.create
        ("test", new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
            InStream.options().withCodec(codec).withBufferSize(1000)), true);
    for(int i=0; i < 2048; ++i) {
      int x = (int) in.next();
      if (i < 1024) {
        assertEquals(i/4, x);
      } else {
        assertEquals(2*i, x);
      }
    }
    for(int i=2047; i >= 0; --i) {
      in.seek(positions[i]);
      int x = (int) in.next();
      if (i < 1024) {
        assertEquals(i/4, x);
      } else {
        assertEquals(2*i, x);
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
    RunLengthIntegerWriter out = new RunLengthIntegerWriter(
        new OutStream("test", new StreamOptions(100), collect), true);
    for(int i=0; i < 2048; ++i) {
      if (i < 1024) {
        out.write(i);
      } else {
        out.write(256 * i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    RunLengthIntegerReader in = new RunLengthIntegerReader(InStream.create
        ("test", new BufferChunk(inBuf, 0), 0, inBuf.remaining()), true);
    for(int i=0; i < 2048; i += 10) {
      int x = (int) in.next();
      if (i < 1024) {
        assertEquals(i, x);
      } else {
        assertEquals(256 * i, x);
      }
      if (i < 2038) {
        in.skip(9);
      }
      in.skip(0);
    }
  }
}
