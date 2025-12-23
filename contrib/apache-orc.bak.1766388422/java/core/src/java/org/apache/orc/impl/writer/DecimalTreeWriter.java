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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.SerializationUtils;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.util.function.Consumer;

public class DecimalTreeWriter extends TreeWriterBase {
  private final PositionedOutputStream valueStream;
  private final SerializationUtils utils = new SerializationUtils();

  // These scratch buffers allow us to serialize decimals much faster.
  private final long[] scratchLongs;
  private final byte[] scratchBuffer;

  private final IntegerWriter scaleStream;
  private final boolean isDirectV2;

  public DecimalTreeWriter(TypeDescription schema,
                           WriterEncryptionVariant encryption,
                           WriterContext context) throws IOException {
    super(schema, encryption, context);
    this.isDirectV2 = isNewWriteFormat(context);
    valueStream = context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
    scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    this.scaleStream = createIntegerWriter(context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.SECONDARY, encryption)),
        true, isDirectV2, context);
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

  private void writeBatch(DecimalColumnVector vector, int offset,
                         int length) throws IOException {
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        HiveDecimalWritable value = vector.vector[0];
        indexStatistics.updateDecimal(value);
        if (createBloomFilter) {
          String str = value.toString(scratchBuffer);
          if (bloomFilter != null) {
            bloomFilter.addString(str);
          }
          bloomFilterUtf8.addString(str);
        }
        for (int i = 0; i < length; ++i) {
          value.serializationUtilsWrite(valueStream,
              scratchLongs);
          scaleStream.write(value.scale());
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vector.noNulls || !vector.isNull[i + offset]) {
          HiveDecimalWritable value = vector.vector[i + offset];
          value.serializationUtilsWrite(valueStream, scratchLongs);
          scaleStream.write(value.scale());
          indexStatistics.updateDecimal(value);
          if (createBloomFilter) {
            String str = value.toString(scratchBuffer);
            if (bloomFilter != null) {
              bloomFilter.addString(str);
            }
            bloomFilterUtf8.addString(str);
          }
        }
      }
    }
  }

  private void writeBatch(Decimal64ColumnVector vector, int offset,
                          int length) throws IOException {
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        indexStatistics.updateDecimal64(vector.vector[0], vector.scale);
        if (createBloomFilter) {
          HiveDecimalWritable value = vector.getScratchWritable();
          value.setFromLongAndScale(vector.vector[0], vector.scale);
          String str = value.toString(scratchBuffer);
          if (bloomFilter != null) {
            bloomFilter.addString(str);
          }
          bloomFilterUtf8.addString(str);
        }
        for (int i = 0; i < length; ++i) {
          utils.writeVslong(valueStream, vector.vector[0]);
          scaleStream.write(vector.scale);
        }
      }
    } else {
      HiveDecimalWritable value = vector.getScratchWritable();
      for (int i = 0; i < length; ++i) {
        if (vector.noNulls || !vector.isNull[i + offset]) {
          long num = vector.vector[i + offset];
          utils.writeVslong(valueStream, num);
          scaleStream.write(vector.scale);
          indexStatistics.updateDecimal64(num, vector.scale);
          if (createBloomFilter) {
            value.setFromLongAndScale(num, vector.scale);
            String str = value.toString(scratchBuffer);
            if (bloomFilter != null) {
              bloomFilter.addString(str);
            }
            bloomFilterUtf8.addString(str);
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
    valueStream.getPosition(recorder);
    scaleStream.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + valueStream.getBufferSize() +
        scaleStream.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() *
        JavaDataModel.get().lengthOfDecimal();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    valueStream.flush();
    scaleStream.flush();
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    Consumer<byte[]> updater = CryptoUtils.modifyIvForStripe(stripeId);
    valueStream.changeIv(updater);
    scaleStream.changeIv(updater);
  }
}
