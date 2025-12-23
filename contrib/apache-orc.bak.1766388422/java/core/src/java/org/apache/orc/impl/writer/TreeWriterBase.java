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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BitFieldWriter;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.RunLengthIntegerWriter;
import org.apache.orc.impl.RunLengthIntegerWriterV2;
import org.apache.orc.impl.StreamName;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.apache.orc.util.BloomFilterUtf8;

import java.io.IOException;
import java.util.List;

/**
 * The parent class of all of the writers for each column. Each column
 * is written by an instance of this class. The compound types (struct,
 * list, map, and union) have children tree writers that write the children
 * types.
 */
public abstract class TreeWriterBase implements TreeWriter {
  protected final int id;
  protected final BitFieldWriter isPresent;
  protected final TypeDescription schema;
  protected final WriterEncryptionVariant encryption;
  private final boolean isCompressed;
  protected final ColumnStatisticsImpl indexStatistics;
  protected final ColumnStatisticsImpl stripeColStatistics;
  protected final ColumnStatisticsImpl fileStatistics;
  protected final RowIndexPositionRecorder rowIndexPosition;
  private final OrcProto.RowIndex.Builder rowIndex;
  private final OrcProto.RowIndexEntry.Builder rowIndexEntry;
  protected final BloomFilter bloomFilter;
  protected final BloomFilterUtf8 bloomFilterUtf8;
  protected final boolean createBloomFilter;
  private final OrcProto.BloomFilterIndex.Builder bloomFilterIndex;
  private final OrcProto.BloomFilterIndex.Builder bloomFilterIndexUtf8;
  protected final OrcProto.BloomFilter.Builder bloomFilterEntry;
  private boolean foundNulls;
  private OutStream isPresentOutStream;
  protected final WriterContext context;

  /**
   * Create a tree writer.
   * @param schema the row schema
   * @param encryption the encryption variant or null if it is unencrypted
   * @param context limited access to the Writer's data.
   */
  TreeWriterBase(TypeDescription schema,
                 WriterEncryptionVariant encryption,
                 WriterContext context) throws IOException {
    this.schema = schema;
    this.encryption = encryption;
    this.context = context;
    this.isCompressed = context.isCompressed();
    this.id = schema.getId();
    isPresentOutStream = context.createStream(new StreamName(id,
          OrcProto.Stream.Kind.PRESENT, encryption));
    isPresent = new BitFieldWriter(isPresentOutStream, 1);
    this.foundNulls = false;
    createBloomFilter = context.getBloomFilterColumns()[id];
    boolean proleptic = context.getProlepticGregorian();
    indexStatistics = ColumnStatisticsImpl.create(schema, proleptic);
    stripeColStatistics = ColumnStatisticsImpl.create(schema, proleptic);
    fileStatistics = ColumnStatisticsImpl.create(schema, proleptic);
    if (context.buildIndex()) {
      rowIndex = OrcProto.RowIndex.newBuilder();
      rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
      rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
    } else {
      rowIndex = null;
      rowIndexEntry = null;
      rowIndexPosition = null;
    }
    if (createBloomFilter) {
      bloomFilterEntry = OrcProto.BloomFilter.newBuilder();
      if (context.getBloomFilterVersion() == OrcFile.BloomFilterVersion.ORIGINAL) {
        bloomFilter = new BloomFilter(context.getRowIndexStride(),
            context.getBloomFilterFPP());
        bloomFilterIndex = OrcProto.BloomFilterIndex.newBuilder();
      } else {
        bloomFilter = null;
        bloomFilterIndex = null;
      }
      bloomFilterUtf8 = new BloomFilterUtf8(context.getRowIndexStride(),
          context.getBloomFilterFPP());
      bloomFilterIndexUtf8 = OrcProto.BloomFilterIndex.newBuilder();
    } else {
      bloomFilterEntry = null;
      bloomFilterIndex = null;
      bloomFilterIndexUtf8 = null;
      bloomFilter = null;
      bloomFilterUtf8 = null;
    }
  }

  protected OrcProto.RowIndex.Builder getRowIndex() {
    return rowIndex;
  }

  protected ColumnStatisticsImpl getStripeStatistics() {
    return stripeColStatistics;
  }

  protected OrcProto.RowIndexEntry.Builder getRowIndexEntry() {
    return rowIndexEntry;
  }

  IntegerWriter createIntegerWriter(PositionedOutputStream output,
                                    boolean signed, boolean isDirectV2,
                                    WriterContext writer) {
    if (isDirectV2) {
      boolean alignedBitpacking =
          writer.getEncodingStrategy().equals(OrcFile.EncodingStrategy.SPEED);
      return new RunLengthIntegerWriterV2(output, signed, alignedBitpacking);
    } else {
      return new RunLengthIntegerWriter(output, signed);
    }
  }

  boolean isNewWriteFormat(WriterContext writer) {
    return writer.getVersion() != OrcFile.Version.V_0_11;
  }

  /**
   * Handle the top level object write.
   *
   * This default method is used for all types except structs, which are the
   * typical case. VectorizedRowBatch assumes the top level object is a
   * struct, so we use the first column for all other types.
   * @param batch the batch to write from
   * @param offset the row to start on
   * @param length the number of rows to write
   */
  @Override
  public void writeRootBatch(VectorizedRowBatch batch, int offset,
                             int length) throws IOException {
    writeBatch(batch.cols[0], offset, length);
  }

  /**
   * Write the values from the given vector from offset for length elements.
   * @param vector the vector to write from
   * @param offset the first value from the vector to write
   * @param length the number of values from the vector to write
   */
  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    if (vector.noNulls) {
      indexStatistics.increment(length);
      if (isPresent != null) {
        for (int i = 0; i < length; ++i) {
          isPresent.write(1);
        }
      }
    } else {
      if (vector.isRepeating) {
        boolean isNull = vector.isNull[0];
        if (isPresent != null) {
          for (int i = 0; i < length; ++i) {
            isPresent.write(isNull ? 0 : 1);
          }
        }
        if (isNull) {
          foundNulls = true;
          indexStatistics.setNull();
        } else {
          indexStatistics.increment(length);
        }
      } else {
        // count the number of non-null values
        int nonNullCount = 0;
        for(int i = 0; i < length; ++i) {
          boolean isNull = vector.isNull[i + offset];
          if (!isNull) {
            nonNullCount += 1;
          }
          if (isPresent != null) {
            isPresent.write(isNull ? 0 : 1);
          }
        }
        indexStatistics.increment(nonNullCount);
        if (nonNullCount != length) {
          foundNulls = true;
          indexStatistics.setNull();
        }
      }
    }
  }

  private void removeIsPresentPositions() {
    for(int i=0; i < rowIndex.getEntryCount(); ++i) {
      OrcProto.RowIndexEntry.Builder entry = rowIndex.getEntryBuilder(i);
      List<Long> positions = entry.getPositionsList();
      // bit streams use 3 positions if uncompressed, 4 if compressed
      positions = positions.subList(isCompressed ? 4 : 3, positions.size());
      entry.clearPositions();
      entry.addAllPositions(positions);
    }
  }

  @Override
  public void prepareStripe(int stripeId) {
    if (isPresent != null) {
      isPresent.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
    }
  }

  @Override
  public void flushStreams() throws IOException {
    if (isPresent != null) {
      isPresent.flush();
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {

    // if no nulls are found in a stream, then suppress the stream
    if (isPresent != null && !foundNulls) {
      isPresentOutStream.suppress();
      // since isPresent bitstream is suppressed, update the index to
      // remove the positions of the isPresent stream
      if (rowIndex != null) {
        removeIsPresentPositions();
      }
    }

    /* Update byte count */
    final long byteCount = context.getPhysicalWriter().getFileBytes(id, encryption);
    stripeColStatistics.updateByteCount(byteCount);

    // merge stripe-level column statistics to file statistics and write it to
    // stripe statistics
    fileStatistics.merge(stripeColStatistics);
    context.writeStatistics(
        new StreamName(id, OrcProto.Stream.Kind.STRIPE_STATISTICS, encryption),
        stripeColStatistics.serialize());
    stripeColStatistics.reset();

    // reset the flag for next stripe
    foundNulls = false;

    context.setEncoding(id, encryption, getEncoding().build());
    if (rowIndex != null) {
      if (rowIndex.getEntryCount() != requiredIndexEntries) {
        throw new IllegalArgumentException("Column has wrong number of " +
            "index entries found: " + rowIndex.getEntryCount() + " expected: " +
            requiredIndexEntries);
      }
      context.writeIndex(new StreamName(id, OrcProto.Stream.Kind.ROW_INDEX, encryption), rowIndex);
      rowIndex.clear();
      rowIndexEntry.clear();
    }

    // write the bloom filter to out stream
    if (bloomFilterIndex != null) {
      context.writeBloomFilter(new StreamName(id,
          OrcProto.Stream.Kind.BLOOM_FILTER), bloomFilterIndex);
      bloomFilterIndex.clear();
    }
    // write the bloom filter to out stream
    if (bloomFilterIndexUtf8 != null) {
      context.writeBloomFilter(new StreamName(id,
          OrcProto.Stream.Kind.BLOOM_FILTER_UTF8), bloomFilterIndexUtf8);
      bloomFilterIndexUtf8.clear();
    }
  }

  /**
   * Get the encoding for this column.
   * @return the information about the encoding of this column
   */
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder builder =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    if (createBloomFilter) {
      builder.setBloomEncoding(BloomFilterIO.Encoding.CURRENT.getId());
    }
    return builder;
  }

  /**
   * Create a row index entry with the previous location and the current
   * index statistics. Also merges the index statistics into the file
   * statistics before they are cleared. Finally, it records the start of the
   * next index and ensures all of the children columns also create an entry.
   */
  @Override
  public void createRowIndexEntry() throws IOException {
    stripeColStatistics.merge(indexStatistics);
    rowIndexEntry.setStatistics(indexStatistics.serialize());
    indexStatistics.reset();
    rowIndex.addEntry(rowIndexEntry);
    rowIndexEntry.clear();
    addBloomFilterEntry();
    recordPosition(rowIndexPosition);
  }

  void addBloomFilterEntry() {
    if (createBloomFilter) {
      if (bloomFilter != null) {
        BloomFilterIO.serialize(bloomFilterEntry, bloomFilter);
        bloomFilterIndex.addBloomFilter(bloomFilterEntry.build());
        bloomFilter.reset();
      }
      if (bloomFilterUtf8 != null) {
        BloomFilterIO.serialize(bloomFilterEntry, bloomFilterUtf8);
        bloomFilterIndexUtf8.addBloomFilter(bloomFilterEntry.build());
        bloomFilterUtf8.reset();
      }
    }
  }

  @Override
  public void addStripeStatistics(StripeStatistics[] stats
                                  ) throws IOException {
    // pick out the correct statistics for this writer
    int variantId;
    int relativeColumn;
    if (encryption == null) {
      variantId = stats.length - 1;
      relativeColumn = id;
    } else {
      variantId = encryption.getVariantId();
      relativeColumn = id - encryption.getRoot().getId();
    }
    OrcProto.ColumnStatistics colStats = stats[variantId].getColumn(relativeColumn);
    // update the file statistics
    fileStatistics.merge(ColumnStatisticsImpl.deserialize(schema, colStats));
    // write them out to the file
    context.writeStatistics(
        new StreamName(id, OrcProto.Stream.Kind.STRIPE_STATISTICS, encryption),
        colStats.toBuilder());
  }

  /**
   * Record the current position in each of this column's streams.
   * @param recorder where should the locations be recorded
   */
  void recordPosition(PositionRecorder recorder) throws IOException {
    if (isPresent != null) {
      isPresent.getPosition(recorder);
    }
  }

  /**
   * Estimate how much memory the writer is consuming excluding the streams.
   * @return the number of bytes.
   */
  @Override
  public long estimateMemory() {
    long result = 0;
    if (isPresent != null) {
      result = isPresentOutStream.getBufferSize();
    }
    return result;
  }

  @Override
  public void writeFileStatistics() throws IOException {
    context.writeStatistics(new StreamName(id,
            OrcProto.Stream.Kind.FILE_STATISTICS, encryption),
        fileStatistics.serialize());
  }

  static class RowIndexPositionRecorder implements PositionRecorder {
    private final OrcProto.RowIndexEntry.Builder builder;

    RowIndexPositionRecorder(OrcProto.RowIndexEntry.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void addPosition(long position) {
      builder.addPositions(position);
    }
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    output[id] = fileStatistics;
  }
}
