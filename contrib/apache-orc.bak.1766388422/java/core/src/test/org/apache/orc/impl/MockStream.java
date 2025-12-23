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

import java.nio.ByteBuffer;

class MockStream {
  final int column;
  final OrcProto.Stream.Kind kind;
  private final ByteBuffer bytes;
  final long offset;
  final int length;
  int readCount = 0;

  MockStream(int column, OrcProto.Stream.Kind kind, long offset,
             ByteBuffer bytes) {
    this.column = column;
    this.kind = kind;
    this.bytes = bytes;
    this.offset = offset;
    this.length = bytes.remaining();
  }

  ByteBuffer getData(long offset, int length) {
    if (offset < this.offset ||
        offset + length > this.offset + this.length) {
      throw new IllegalArgumentException("Bad getData [" + offset + ", " +
                                             (offset + length) + ") from [" +
                                            this.offset + ", " +
                                             (this.offset + this.length) + ")");
    }
    ByteBuffer copy = bytes.duplicate();
    int posn = (int) (offset - this.offset);
    copy.position(posn);
    copy.limit(posn + length);
    return copy;
  }
}
