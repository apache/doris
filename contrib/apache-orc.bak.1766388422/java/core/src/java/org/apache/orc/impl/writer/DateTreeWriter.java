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
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.StreamName;

import java.io.IOException;

public class DateTreeWriter extends TreeWriterBase {
  private final IntegerWriter writer;
  private final boolean isDirectV2;
  private final boolean useProleptic;

  public DateTreeWriter(TypeDescription schema,
                        WriterEncryptionVariant encryption,
                        WriterContext context) throws IOException {
    super(schema, encryption, context);
    OutStream out = context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    this.isDirectV2 = isNewWriteFormat(context);
    this.writer = createIntegerWriter(out, true, isDirectV2, context);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    useProleptic = context.getProlepticGregorian();
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    LongColumnVector vec = (LongColumnVector) vector;
    if (vector instanceof  DateColumnVector) {
      ((DateColumnVector) vec).changeCalendar(useProleptic, true);
    } else if (useProleptic) {
      throw new IllegalArgumentException("Can't use LongColumnVector to write" +
                                             " proleptic dates");
    }
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        int value = (int) vec.vector[0];
        indexStatistics.updateDate(value);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(value);
          }
          bloomFilterUtf8.addLong(value);
        }
        for (int i = 0; i < length; ++i) {
          writer.write(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          int value = (int) vec.vector[i + offset];
          writer.write(value);
          indexStatistics.updateDate(value);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
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
    writer.getPosition(recorder);
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
  public long estimateMemory() {
    return super.estimateMemory() + writer.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() *
        JavaDataModel.get().lengthOfDate();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    writer.flush();
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    writer.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
  }
}
