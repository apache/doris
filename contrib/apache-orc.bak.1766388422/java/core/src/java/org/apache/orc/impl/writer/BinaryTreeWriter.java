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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.util.function.Consumer;

public class BinaryTreeWriter extends TreeWriterBase {
  private final PositionedOutputStream stream;
  private final IntegerWriter length;
  private boolean isDirectV2 = true;

  public BinaryTreeWriter(TypeDescription schema,
                          WriterEncryptionVariant encryption,
                          WriterContext context) throws IOException {
    super(schema, encryption, context);
    this.stream = context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    this.isDirectV2 = isNewWriteFormat(context);
    this.length = createIntegerWriter(context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.LENGTH, encryption)),
        false, isDirectV2, context);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (isDirectV2) {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
    } else {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    }
    return result;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    BytesColumnVector vec = (BytesColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        for (int i = 0; i < length; ++i) {
          stream.write(vec.vector[0], vec.start[0],
              vec.length[0]);
          this.length.write(vec.length[0]);
        }
        indexStatistics.updateBinary(vec.vector[0], vec.start[0],
            vec.length[0], length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
          }
          bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          stream.write(vec.vector[offset + i],
              vec.start[offset + i], vec.length[offset + i]);
          this.length.write(vec.length[offset + i]);
          indexStatistics.updateBinary(vec.vector[offset + i],
              vec.start[offset + i], vec.length[offset + i], 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addBytes(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
            }
            bloomFilterUtf8.addBytes(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
          }
        }
      }
    }
  }


  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    stream.getPosition(recorder);
    length.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + stream.getBufferSize() +
        length.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    // get total length of binary blob
    BinaryColumnStatistics bcs = (BinaryColumnStatistics) fileStatistics;
    return bcs.getSum();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    stream.flush();
    length.flush();
  }


  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    Consumer<byte[]> updater = CryptoUtils.modifyIvForStripe(stripeId);
    stream.changeIv(updater);
    length.changeIv(updater);
  }
}
