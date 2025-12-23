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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

public class ListTreeWriter extends TreeWriterBase {
  private final IntegerWriter lengths;
  private final boolean isDirectV2;
  private final TreeWriter childWriter;

  ListTreeWriter(TypeDescription schema,
                 WriterEncryptionVariant encryption,
                 WriterContext context) throws IOException {
    super(schema, encryption, context);
    this.isDirectV2 = isNewWriteFormat(context);
    childWriter = Factory.create(schema.getChildren().get(0), encryption, context);
    lengths = createIntegerWriter(context.createStream(
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
  public void createRowIndexEntry() throws IOException {
    super.createRowIndexEntry();
    childWriter.createRowIndexEntry();
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    ListColumnVector vec = (ListColumnVector) vector;
    /* update aggregate statistics */
    indexStatistics.updateCollectionLength(vec.lengths.length);

    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        int childOffset = (int) vec.offsets[0];
        int childLength = (int) vec.lengths[0];
        for (int i = 0; i < length; ++i) {
          lengths.write(childLength);
          childWriter.writeBatch(vec.child, childOffset, childLength);
        }
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(childLength);
          }
          bloomFilterUtf8.addLong(childLength);
        }
      }
    } else {
      // write the elements in runs
      int currentOffset = 0;
      int currentLength = 0;
      for (int i = 0; i < length; ++i) {
        if (!vec.isNull[i + offset]) {
          int nextLength = (int) vec.lengths[offset + i];
          int nextOffset = (int) vec.offsets[offset + i];
          lengths.write(nextLength);
          if (currentLength == 0) {
            currentOffset = nextOffset;
            currentLength = nextLength;
          } else if (currentOffset + currentLength != nextOffset) {
            childWriter.writeBatch(vec.child, currentOffset,
                currentLength);
            currentOffset = nextOffset;
            currentLength = nextLength;
          } else {
            currentLength += nextLength;
          }
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(nextLength);
            }
            bloomFilterUtf8.addLong(nextLength);
          }
        }
      }
      if (currentLength != 0) {
        childWriter.writeBatch(vec.child, currentOffset,
            currentLength);
      }
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    childWriter.writeStripe(requiredIndexEntries);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    lengths.getPosition(recorder);
  }

  @Override
  public void addStripeStatistics(StripeStatistics[] stats
                                  ) throws IOException {
    super.addStripeStatistics(stats);
    childWriter.addStripeStatistics(stats);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + lengths.estimateMemory() +
        childWriter.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return childWriter.getRawDataSize();
  }

  @Override
  public void writeFileStatistics() throws IOException {
    super.writeFileStatistics();
    childWriter.writeFileStatistics();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    lengths.flush();
    childWriter.flushStreams();
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    super.getCurrentStatistics(output);
    childWriter.getCurrentStatistics(output);
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    lengths.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
    childWriter.prepareStripe(stripeId);
  }
}
