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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.SerializationUtils;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.function.Consumer;

public class TimestampTreeWriter extends TreeWriterBase {
  public static final int MILLIS_PER_SECOND = 1000;
  public static final String BASE_TIMESTAMP_STRING = "2015-01-01 00:00:00";
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private final IntegerWriter seconds;
  private final IntegerWriter nanos;
  private final boolean isDirectV2;
  private final boolean alwaysUTC;
  private final TimeZone localTimezone;
  private final long epoch;
  private final boolean useProleptic;

  public TimestampTreeWriter(TypeDescription schema,
                             WriterEncryptionVariant encryption,
                             WriterContext context,
                             boolean instantType) throws IOException {
    super(schema, encryption, context);
    this.isDirectV2 = isNewWriteFormat(context);
    this.seconds = createIntegerWriter(context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption)),
        true, isDirectV2, context);
    this.nanos = createIntegerWriter(context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.SECONDARY, encryption)),
        false, isDirectV2, context);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    this.alwaysUTC = instantType || context.getUseUTCTimestamp();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      if (this.alwaysUTC) {
        dateFormat.setTimeZone(UTC);
        localTimezone = null;
        epoch = dateFormat.parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime() /
                       TimestampTreeWriter.MILLIS_PER_SECOND;

      } else {
        localTimezone = TimeZone.getDefault();
        dateFormat.setTimeZone(localTimezone);
        epoch = dateFormat.parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime() /
                         TimestampTreeWriter.MILLIS_PER_SECOND;
      }
    } catch (ParseException e) {
      throw new IOException("Unable to create base timestamp tree writer", e);
    }
    useProleptic = context.getProlepticGregorian();
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    result.setKind(isDirectV2 ? OrcProto.ColumnEncoding.Kind.DIRECT_V2
                       : OrcProto.ColumnEncoding.Kind.DIRECT);
    return result;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    TimestampColumnVector vec = (TimestampColumnVector) vector;
    vec.changeCalendar(useProleptic, true);
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        // ignore the bottom three digits from the vec.time field
        final long secs = vec.time[0] / MILLIS_PER_SECOND;
        final int newNanos = vec.nanos[0];
        // set the millis based on the top three digits of the nanos
        long millis = secs * MILLIS_PER_SECOND + newNanos / 1_000_000;
        if (millis < 0 && newNanos > 999_999) {
          millis -= MILLIS_PER_SECOND;
        }
        long utc = vec.isUTC() || alwaysUTC ?
            millis : SerializationUtils.convertToUtc(localTimezone, millis);
        indexStatistics.updateTimestamp(utc, newNanos % 1_000_000);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(millis);
          }
          bloomFilterUtf8.addLong(utc);
        }
        final long nano = formatNanos(vec.nanos[0]);
        for (int i = 0; i < length; ++i) {
          seconds.write(secs - epoch);
          nanos.write(nano);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          // ignore the bottom three digits from the vec.time field
          final long secs = vec.time[i + offset] / MILLIS_PER_SECOND;
          final int newNanos = vec.nanos[i + offset];
          // set the millis based on the top three digits of the nanos
          long millis = secs * MILLIS_PER_SECOND + newNanos / 1_000_000;
          if (millis < 0 && newNanos > 999_999) {
            millis -= MILLIS_PER_SECOND;
          }
          long utc = vec.isUTC() || alwaysUTC ?
              millis : SerializationUtils.convertToUtc(localTimezone, millis);
          seconds.write(secs - epoch);
          nanos.write(formatNanos(newNanos));
          indexStatistics.updateTimestamp(utc, newNanos % 1_000_000);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(millis);
            }
            bloomFilterUtf8.addLong(utc);
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

  static long formatNanos(int nanos) {
    if (nanos == 0) {
      return 0;
    } else if (nanos % 100 != 0) {
      return ((long) nanos) << 3;
    } else {
      nanos /= 100;
      int trailingZeros = 1;
      while (nanos % 10 == 0 && trailingZeros < 7) {
        nanos /= 10;
        trailingZeros += 1;
      }
      return ((long) nanos) << 3 | trailingZeros;
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    seconds.getPosition(recorder);
    nanos.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + seconds.estimateMemory() +
        nanos.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() *
        JavaDataModel.get().lengthOfTimestamp();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    seconds.flush();
    nanos.flush();
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    Consumer<byte[]> updater = CryptoUtils.modifyIvForStripe(stripeId);
    seconds.changeIv(updater);
    nanos.changeIv(updater);
  }
}
