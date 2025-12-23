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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordReaderUtils {

  private final BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
    .range(1000, 1000)
    .range(2000, 1000)
    .range(4000, 1000)
    .range(4100, 100)
    .range(8000, 1000).build();

  private static void assertChunkEquals(BufferChunk expected, BufferChunk actual) {
    assertTrue(Objects.equals(expected, actual)
               && expected.getOffset() == actual.getOffset()
               && expected.getLength() == actual.getLength());
  }

  @Test
  public void testDeterminationOfSingleRead() {
    BufferChunk toChunk = RecordReaderUtils.ChunkReader.create(rangeList.get(), 0).getTo();
    assertChunkEquals(rangeList.get(1), toChunk);
    assertTrue(RecordReaderUtils.ChunkReader.create(rangeList.get(), toChunk)
                 .getExtraBytesFraction()
               < 0.001);

    toChunk = RecordReaderUtils.ChunkReader.create(rangeList.get(), 1000).getTo();
    assertChunkEquals(rangeList.get(3), toChunk);
    assertTrue(RecordReaderUtils.ChunkReader.create(rangeList.get(), toChunk)
                 .getExtraBytesFraction()
               >= .2);

    toChunk = RecordReaderUtils.ChunkReader.create(rangeList.get(), 999).getTo();
    assertChunkEquals(rangeList.get(1), toChunk);
    assertTrue(RecordReaderUtils.ChunkReader.create(rangeList.get(), toChunk)
                 .getExtraBytesFraction()
               < 0.001);
  }

  @Test
  public void testNoGapCombine() {
    BufferChunk toChunk = RecordReaderUtils.findSingleRead(rangeList.get());
    assertChunkEquals(rangeList.get(1), toChunk);
  }

  @Test
  public void testReadExtraBytes() {
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(3), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, false);
  }

  @Test
  public void testRemoveBytes() {
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(3), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, true);
  }

  @Test
  public void testRemoveBytesSmallerOverlapFirst() {
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(1000, 1000)
      .range(2000, 1000)
      .range(4000, 100)
      .range(4000, 1000)
      .range(8000, 1000).build();
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(3), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, true);
  }

  @Test
  public void testRemoveBytesWithOverlap() {
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(1000, 1000)
      .range(1800, 400)
      .range(2000, 1000)
      .range(4000, 100)
      .range(4000, 1000)
      .range(8000, 1000).build();
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(4), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, true);
  }

  @Test
  public void testExtraBytesReadWithinThreshold() {
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(1000, 1000)
      .range(1800, 400)
      .range(2000, 1000)
      .range(4000, 100)
      .range(4000, 1000)
      .range(8000, 1000).build();
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(4), chunkReader.getTo());
    chunkReader.populateChunks(makeByteBuffer(chunkReader.getReadBytes(),
                                              chunkReader.getFrom().getOffset()),
                               false,
                               1.0);
    validateChunks(chunkReader);
    assertNotEquals(chunkReader.getReadBytes(), chunkReader.getReqBytes());
    assertEquals(chunkReader.getReadBytes(), chunkReader.getFrom().getData().array().length);
  }

  private ByteBuffer makeByteBuffer(int length, long offset) {
    byte[] readBytes = new byte[length];
    for (int i = 0; i < readBytes.length; i++) {
      readBytes[i] = (byte) ((i + offset) % Byte.MAX_VALUE);
    }
    return ByteBuffer.wrap(readBytes);
  }

  private void populateAndValidateChunks(RecordReaderUtils.ChunkReader chunkReader,
                                         boolean withRemove) {
    if (withRemove) {
      assertTrue(chunkReader.getReadBytes() > chunkReader.getReqBytes());
    }
    ByteBuffer bytes = makeByteBuffer(chunkReader.getReadBytes(),
                                      chunkReader.getFrom().getOffset());
    if (withRemove) {
      chunkReader.populateChunksReduceSize(bytes, false);
      assertEquals(chunkReader.getReqBytes(), chunkReader.getFrom().getData().array().length);
    } else {
      chunkReader.populateChunksAsIs(bytes);
      assertEquals(chunkReader.getReadBytes(), chunkReader.getFrom().getData().array().length);
    }

    validateChunks(chunkReader);
  }

  private void validateChunks(RecordReaderUtils.ChunkReader chunkReader) {
    BufferChunk current = chunkReader.getFrom();
    while (current != chunkReader.getTo().next) {
      assertTrue(current.hasData());
      assertEquals(current.getOffset() % Byte.MAX_VALUE, current.getData().get(),
                   String.format("Failed for %s", current));
      current = (BufferChunk) current.next;
    }
  }
}
