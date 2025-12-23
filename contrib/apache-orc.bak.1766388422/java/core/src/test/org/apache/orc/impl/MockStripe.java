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

import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MockStripe implements StripeInformation {
  private final int numColumns;
  private final List<MockStream> streams = new ArrayList<>();
  private final List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
  private final long startOffset;
  private final int stripeId;
  private int indexLength = 0;
  private int dataLength = 0;
  private int footerLength = 0;
  private OrcProto.StripeFooter footer;
  private int rows = 0;

  public MockStripe(int numColumns, int stripeId, long offset) {
    this.numColumns = numColumns;
    this.startOffset = offset;
    this.stripeId = stripeId;
  }

  public MockStream addStream(int column, OrcProto.Stream.Kind kind,
                              long offset, ByteBuffer bytes) {
    MockStream result = new MockStream(column, kind, offset, bytes);
    streams.add(result);
    int length = bytes.remaining();
    if (StreamName.getArea(kind) == StreamName.Area.INDEX) {
      indexLength += length;
    } else {
      dataLength += length;
    }
    return result;
  }

  public void addEncoding(OrcProto.ColumnEncoding.Kind kind) {
    OrcProto.ColumnEncoding.Builder result =
        OrcProto.ColumnEncoding.newBuilder();
    result.setKind(kind);
    encodings.add(result.build());
  }

  public void close(int rows, String timezone) {
    this.rows = rows;
    // make sure we have enough encodings
    while (encodings.size() < numColumns) {
      addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT);
    }
    OrcProto.StripeFooter.Builder foot = OrcProto.StripeFooter.newBuilder();
    for(MockStream stream: streams) {
      foot.addStreams(OrcProto.Stream.newBuilder()
                          .setKind(stream.kind)
                          .setLength(stream.length)
                          .setColumn(stream.column).build());
    }
    for(OrcProto.ColumnEncoding encoding: encodings) {
      foot.addColumns(encoding);
    }
    if (timezone != null) {
      foot.setWriterTimezone(timezone);
    }
    footer = foot.build();
    footerLength = footer.getSerializedSize();
  }

  @Override
  public long getOffset() {
    return startOffset;
  }

  @Override
  public long getLength() {
    return indexLength + dataLength + footerLength;
  }

  @Override
  public long getIndexLength() {
    return indexLength;
  }

  @Override
  public long getDataLength() {
    return dataLength;
  }

  @Override
  public long getFooterLength() {
    return footerLength;
  }

  @Override
  public long getNumberOfRows() {
    return rows;
  }

  @Override
  public long getStripeId() {
    return stripeId;
  }

  @Override
  public boolean hasEncryptionStripeId() {
    return false;
  }

  @Override
  public long getEncryptionStripeId() {
    return stripeId;
  }

  @Override
  public byte[][] getEncryptedLocalKeys() {
    return new byte[0][];
  }

  public MockStream getStream(int column, OrcProto.Stream.Kind kind) {
    for (MockStream stream: streams) {
      if (stream.column == column && stream.kind == kind) {
        return stream;
      }
    }
    throw new IllegalArgumentException("Can't find stream column " + column
                                           + " kind " + kind);
  }

  private MockStream getStream(long offset) {
    for (MockStream stream: streams) {
      if (stream.offset <= offset && offset < stream.offset + stream.length) {
        return stream;
      }
    }
    throw new IllegalArgumentException("Can't find stream at offset " + offset);
  }

  public ByteBuffer getData(long offset, int length) {
    MockStream stream = getStream(offset);
    stream.readCount += 1;
    return stream.getData(offset, length);
  }

  public void resetCounts() {
    for(MockStream stream: streams) {
      stream.readCount = 0;
    }
  }

  public OrcProto.StripeFooter getFooter() {
    return footer;
  }
}
