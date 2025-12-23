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
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.RunLengthIntegerWriterV2;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

/**
 * Writer for short decimals in ORCv2.
 */
public class Decimal64TreeWriter extends TreeWriterBase {
  private final RunLengthIntegerWriterV2 valueWriter;
  private final int scale;

  public Decimal64TreeWriter(TypeDescription schema,
                             WriterEncryptionVariant encryption,
                             WriterContext context) throws IOException {
    super(schema, encryption, context);
    OutStream stream = context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    // Use RLEv2 until we have the new RLEv3.
    valueWriter = new RunLengthIntegerWriterV2(stream, true, true);
    scale = schema.getScale();
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  private void writeBatch(DecimalColumnVector vector, int offset,
                         int length) throws IOException {
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        HiveDecimalWritable value = vector.vector[0];
        long lg = value.serialize64(scale);
        indexStatistics.updateDecimal64(lg, scale);
        if (createBloomFilter) {
          bloomFilterUtf8.addLong(lg);
        }
        for (int i = 0; i < length; ++i) {
          valueWriter.write(lg);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vector.noNulls || !vector.isNull[i + offset]) {
          HiveDecimalWritable value = vector.vector[i + offset];
          long lg = value.serialize64(scale);
          valueWriter.write(lg);
          indexStatistics.updateDecimal64(lg, scale);
          if (createBloomFilter) {
            bloomFilterUtf8.addLong(lg);
          }
        }
      }
    }
  }

  private void writeBatch(Decimal64ColumnVector vector, int offset,
                          int length) throws IOException {
    assert(scale == vector.scale);
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        long lg = vector.vector[0];
        indexStatistics.updateDecimal64(lg, scale);
        if (createBloomFilter) {
          bloomFilterUtf8.addLong(lg);
        }
        for (int i = 0; i < length; ++i) {
          valueWriter.write(lg);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vector.noNulls || !vector.isNull[i + offset]) {
          long lg = vector.vector[i + offset];
          valueWriter.write(lg);
          indexStatistics.updateDecimal64(lg, scale);
          if (createBloomFilter) {
            bloomFilterUtf8.addLong(lg);
          }
        }
      }
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    if (vector instanceof Decimal64ColumnVector) {
      writeBatch((Decimal64ColumnVector) vector, offset, length);
    } else {
      writeBatch((DecimalColumnVector) vector, offset, length);
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
    valueWriter.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + valueWriter.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() * JavaDataModel.get().primitive2();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    valueWriter.flush();
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    valueWriter.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
  }
}
