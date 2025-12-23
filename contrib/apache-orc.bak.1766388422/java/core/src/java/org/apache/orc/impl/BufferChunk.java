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

import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;

import java.nio.ByteBuffer;

/**
 * The sections of stripe that we have read.
 * This might not match diskRange - 1 disk range can be multiple buffer chunks,
 * depending on DFS block boundaries.
 */
public class BufferChunk extends DiskRangeList {

  private ByteBuffer chunk;

  public BufferChunk(long offset, int length) {
    super(offset, offset + length);
    chunk = null;
  }

  public BufferChunk(ByteBuffer chunk, long offset) {
    super(offset, offset + chunk.remaining());
    this.chunk = chunk;
  }

  public void setChunk(ByteBuffer chunk) {
    this.chunk = chunk;
  }

  @Override
  public boolean hasData() {
    return chunk != null;
  }

  @Override
  public final String toString() {
    if (chunk == null) {
      return "data range[" + offset + ", " + end +")";
    } else {
      boolean makesSense = chunk.remaining() == (end - offset);
      return "data range [" + offset + ", " + end + "), size: " + chunk.remaining()
                 + (makesSense ? "" : "(!)") + " type: " +
                 (chunk.isDirect() ? "direct" : "array-backed");
    }
  }

  @Override
  public DiskRange sliceAndShift(long offset, long end, long shiftBy) {
    assert offset <= end && offset >= this.offset && end <= this.end;
    assert offset + shiftBy >= 0;
    ByteBuffer sliceBuf = chunk.slice();
    int newPos = (int) (offset - this.offset);
    int newLimit = newPos + (int) (end - offset);
    try {
      sliceBuf.position(newPos);
      sliceBuf.limit(newLimit);
    } catch (Throwable t) {
      throw new RuntimeException(
              "Failed to slice buffer chunk with range" + " [" + this.offset + ", " + this.end
              + "), position: " + chunk.position() + " limit: " + chunk.limit() + ", "
              + (chunk.isDirect() ? "direct" : "array") + "; to [" + offset + ", " + end + ") "
              + t.getClass(), t);
    }
    return new BufferChunk(sliceBuf, offset + shiftBy);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    BufferChunk ob = (BufferChunk) other;
    return chunk.equals(ob.chunk);
  }

  @Override
  public int hashCode() {
    return chunk.hashCode();
  }

  @Override
  public ByteBuffer getData() {
    return chunk;
  }
}
