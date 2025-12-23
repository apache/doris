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

import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

public class MockDataReader implements DataReader {
  private final List<MockStripe> stripes = new ArrayList<>();
  private final int numColumns;
  private long offset = 3;
  private MockStripe next;
  private Set<ByteBuffer> outBuffers =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final InStream.StreamOptions options;

  public MockDataReader(TypeDescription schema) {
    this(schema, new InStream.StreamOptions());
  }

  public MockDataReader(TypeDescription schema,
                        InStream.StreamOptions options) {
    numColumns = schema.getMaximumId() + 1;
    next = new MockStripe(numColumns, stripes.size(), offset);
    this.options = options;
  }

  public MockDataReader addStream(int column, OrcProto.Stream.Kind kind,
                                  ByteBuffer bytes) {
    next.addStream(column, kind, offset, bytes);
    offset += bytes.remaining();
    return this;
  }

  public MockDataReader addEncoding(OrcProto.ColumnEncoding.Kind kind) {
    next.addEncoding(kind);
    return this;
  }

  public MockDataReader addStripeFooter(int rows, String timezone) {
    next.close(rows, timezone);
    stripes.add(next);
    offset += next.getFooterLength();
    next = new MockStripe(numColumns, stripes.size(), offset);
    return this;
  }

  @Override
  public void open() { }

  @Override
  public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) {
    return stripes.get((int) stripe.getStripeId()).getFooter();
  }

  @Override
  public BufferChunkList readFileData(BufferChunkList list,
                                      boolean doForceDirect) {
    for(BufferChunk buffer = list.get(); buffer != null;
        buffer = (BufferChunk) buffer.next) {
      if (!buffer.hasData()) {
        MockStripe stripe = getStripeByOffset(buffer.getOffset());
        ByteBuffer data = stripe.getData(buffer.getOffset(), buffer.getLength());
        outBuffers.add(data);
        buffer.setChunk(data);
      }
    }
    return list;
  }

  @Override
  public boolean isTrackingDiskRanges() {
    return true;
  }

  @Override
  public void releaseBuffer(ByteBuffer toRelease) {
    outBuffers.remove(toRelease);
  }

  @Override
  public DataReader clone() {
    throw new UnsupportedOperationException("Clone not supported.");
  }

  @Override
  public void close() { }

  @Override
  public InStream.StreamOptions getCompressionOptions() {
    return options;
  }

  public MockStripe getStripe(int id) {
    return stripes.get(id);
  }

  private MockStripe getStripeByOffset(long offset) {
    for(MockStripe stripe: stripes) {
      if (stripe.getOffset() <= offset &&
              (offset - stripe.getOffset() < stripe.getLength())) {
        return stripe;
      }
    }
    throw new IllegalArgumentException("Can't find stripe at " + offset);
  }

  void resetCounts() {
    for(MockStripe stripe: stripes) {
      stripe.resetCounts();
    }
  }
}
