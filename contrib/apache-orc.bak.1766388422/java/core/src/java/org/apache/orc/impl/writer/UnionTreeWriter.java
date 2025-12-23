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
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.RunLengthByteWriter;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.util.List;

public class UnionTreeWriter extends TreeWriterBase {
  private final RunLengthByteWriter tags;
  private final TreeWriter[] childrenWriters;

  UnionTreeWriter(TypeDescription schema,
                  WriterEncryptionVariant encryption,
                  WriterContext context) throws IOException {
    super(schema, encryption, context);
    List<TypeDescription> children = schema.getChildren();
    childrenWriters = new TreeWriterBase[children.size()];
    for (int i = 0; i < childrenWriters.length; ++i) {
      childrenWriters[i] = Factory.create(children.get(i), encryption, context);
    }
    tags =
        new RunLengthByteWriter(context.createStream(
            new StreamName(id, OrcProto.Stream.Kind.DATA, encryption)));
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    UnionColumnVector vec = (UnionColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        byte tag = (byte) vec.tags[0];
        for (int i = 0; i < length; ++i) {
          tags.write(tag);
        }
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(tag);
          }
          bloomFilterUtf8.addLong(tag);
        }
        childrenWriters[tag].writeBatch(vec.fields[tag], offset, length);
      }
    } else {
      // write the records in runs of the same tag
      int[] currentStart = new int[vec.fields.length];
      int[] currentLength = new int[vec.fields.length];
      for (int i = 0; i < length; ++i) {
        // only need to deal with the non-nulls, since the nulls were dealt
        // with in the super method.
        if (vec.noNulls || !vec.isNull[i + offset]) {
          byte tag = (byte) vec.tags[offset + i];
          tags.write(tag);
          if (currentLength[tag] == 0) {
            // start a new sequence
            currentStart[tag] = i + offset;
            currentLength[tag] = 1;
          } else if (currentStart[tag] + currentLength[tag] == i + offset) {
            // ok, we are extending the current run for that tag.
            currentLength[tag] += 1;
          } else {
            // otherwise, we need to close off the old run and start a new one
            childrenWriters[tag].writeBatch(vec.fields[tag],
                currentStart[tag], currentLength[tag]);
            currentStart[tag] = i + offset;
            currentLength[tag] = 1;
          }
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(tag);
            }
            bloomFilterUtf8.addLong(tag);
          }
        }
      }
      // write out any left over sequences
      for (int tag = 0; tag < currentStart.length; ++tag) {
        if (currentLength[tag] != 0) {
          childrenWriters[tag].writeBatch(vec.fields[tag], currentStart[tag],
              currentLength[tag]);
        }
      }
    }
  }

  @Override
  public void createRowIndexEntry() throws IOException {
    super.createRowIndexEntry();
    for (TreeWriter child : childrenWriters) {
      child.createRowIndexEntry();
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    for (TreeWriter child : childrenWriters) {
      child.writeStripe(requiredIndexEntries);
    }
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    tags.getPosition(recorder);
  }

  @Override
  public void addStripeStatistics(StripeStatistics[] stats
                                  ) throws IOException {
    super.addStripeStatistics(stats);
    for (TreeWriter child : childrenWriters) {
      child.addStripeStatistics(stats);
    }
  }

  @Override
  public long estimateMemory() {
    long children = 0;
    for (TreeWriter writer : childrenWriters) {
      children += writer.estimateMemory();
    }
    return children + super.estimateMemory() + tags.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.getRawDataSize();
    }
    return result;
  }

  @Override
  public void writeFileStatistics() throws IOException {
    super.writeFileStatistics();
    for (TreeWriter child : childrenWriters) {
      child.writeFileStatistics();
    }
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    tags.flush();
    for (TreeWriter child : childrenWriters) {
      child.flushStreams();
    }
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    super.getCurrentStatistics(output);
    for(TreeWriter child: childrenWriters) {
      child.getCurrentStatistics(output);
    }
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    tags.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
    for (TreeWriter child: childrenWriters) {
      child.prepareStripe(stripeId);
    }
  }
}
