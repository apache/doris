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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.Dictionary;
import org.apache.orc.impl.DynamicIntArray;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.StringHashTableDictionary;
import org.apache.orc.impl.StringRedBlackTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.orc.OrcConf.DICTIONARY_IMPL;
import static org.apache.orc.impl.Dictionary.INITIAL_DICTIONARY_SIZE;


public abstract class StringBaseTreeWriter extends TreeWriterBase {
  // Stream for dictionary's key
  private final OutStream stringOutput;
  protected final IntegerWriter lengthOutput;
  // Stream for dictionary-encoded value
  private final IntegerWriter rowOutput;
  protected final DynamicIntArray rows = new DynamicIntArray();
  protected final PositionedOutputStream directStreamOutput;
  private final List<OrcProto.RowIndexEntry> savedRowIndex =
      new ArrayList<>();
  private final boolean buildIndex;
  private final List<Long> rowIndexValueCount = new ArrayList<>();
  // If the number of keys in a dictionary is greater than this fraction of
  //the total number of non-null rows, turn off dictionary encoding
  private final double dictionaryKeySizeThreshold;
  protected Dictionary dictionary;
  protected boolean useDictionaryEncoding = true;
  private boolean isDirectV2 = true;
  private boolean doneDictionaryCheck;
  private final boolean strideDictionaryCheck;

  private static Dictionary createDict(Configuration conf) {
    String dictImpl = conf.get(DICTIONARY_IMPL.getAttribute(),
        DICTIONARY_IMPL.getDefaultValue().toString()).toUpperCase();
    switch (Dictionary.IMPL.valueOf(dictImpl)) {
      case RBTREE:
        return new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
      case HASH:
        return new StringHashTableDictionary(INITIAL_DICTIONARY_SIZE);
      default:
        throw new UnsupportedOperationException("Unknown implementation:" + dictImpl);
    }
  }

  StringBaseTreeWriter(TypeDescription schema,
                       WriterEncryptionVariant encryption,
                       WriterContext context) throws IOException {
    super(schema, encryption, context);
    Configuration conf = context.getConfiguration();

    this.dictionary = createDict(conf);
    this.isDirectV2 = isNewWriteFormat(context);
    directStreamOutput = context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DATA, encryption));
    stringOutput = context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.DICTIONARY_DATA, encryption));
    lengthOutput = createIntegerWriter(context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.LENGTH, encryption)),
        false, isDirectV2, context);
    rowOutput = createIntegerWriter(directStreamOutput, false, isDirectV2,
        context);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    rowIndexValueCount.add(0L);
    buildIndex = context.buildIndex();
    dictionaryKeySizeThreshold = context.getDictionaryKeySizeThreshold(id);
    strideDictionaryCheck =
        OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK.getBoolean(conf);
    if (dictionaryKeySizeThreshold <= 0.0) {
      useDictionaryEncoding = false;
      doneDictionaryCheck = true;
      recordDirectStreamPosition();
    } else {
      doneDictionaryCheck = false;
    }
  }

  private void checkDictionaryEncoding() {
    if (!doneDictionaryCheck) {
      // Set the flag indicating whether or not to use dictionary encoding
      // based on whether or not the fraction of distinct keys over number of
      // non-null rows is less than the configured threshold
      float ratio = rows.size() > 0 ? (float) (dictionary.size()) / rows.size() : 0.0f;
      useDictionaryEncoding = !isDirectV2 || ratio <= dictionaryKeySizeThreshold;
      doneDictionaryCheck = true;
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    // if rows in stripe is less than dictionaryCheckAfterRows, dictionary
    // checking would not have happened. So do it again here.
    checkDictionaryEncoding();

    if (!useDictionaryEncoding) {
      stringOutput.suppress();
    }

    // we need to build the rowindex before calling super, since it
    // writes it out.
    super.writeStripe(requiredIndexEntries);
    // reset all of the fields to be ready for the next stripe.
    dictionary.clear();
    savedRowIndex.clear();
    rowIndexValueCount.clear();
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    rowIndexValueCount.add(0L);

    if (!useDictionaryEncoding) {
      // record the start positions of first index stride of next stripe i.e
      // beginning of the direct streams when dictionary is disabled
      recordDirectStreamPosition();
    }
  }

  private void flushDictionary() throws IOException {
    final int[] dumpOrder = new int[dictionary.size()];

    if (useDictionaryEncoding) {
      // Write the dictionary by traversing the dictionary writing out
      // the bytes and lengths; and creating the map from the original order
      // to the final sorted order.
      dictionary.visit(new Dictionary.Visitor() {
        private int currentId = 0;

        @Override
        public void visit(Dictionary.VisitorContext context
        ) throws IOException {
          context.writeBytes(stringOutput);
          lengthOutput.write(context.getLength());
          dumpOrder[context.getOriginalPosition()] = currentId++;
        }
      });
    } else {
      // for direct encoding, we don't want the dictionary data stream
      stringOutput.suppress();
    }
    int length = rows.size();
    int rowIndexEntry = 0;
    OrcProto.RowIndex.Builder rowIndex = getRowIndex();
    // write the values translated into the dump order.
    for (int i = 0; i <= length; ++i) {
      // now that we are writing out the row values, we can finalize the
      // row index
      if (buildIndex) {
        while (i == rowIndexValueCount.get(rowIndexEntry) &&
            rowIndexEntry < savedRowIndex.size()) {
          OrcProto.RowIndexEntry.Builder base =
              savedRowIndex.get(rowIndexEntry++).toBuilder();
          if (useDictionaryEncoding) {
            rowOutput.getPosition(new RowIndexPositionRecorder(base));
          } else {
            PositionRecorder posn = new RowIndexPositionRecorder(base);
            directStreamOutput.getPosition(posn);
            lengthOutput.getPosition(posn);
          }
          rowIndex.addEntry(base.build());
        }
      }
      if (i != length) {
        if (useDictionaryEncoding) {
          rowOutput.write(dumpOrder[rows.get(i)]);
        } else {
          final int writeLen = dictionary.writeTo(directStreamOutput, rows.get(i));
          lengthOutput.write(writeLen);
        }
      }
    }
    rows.clear();
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (useDictionaryEncoding) {
      result.setDictionarySize(dictionary.size());
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY);
      }
    } else {
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
    }
    return result;
  }

  /**
   * This method doesn't call the super method, because unlike most of the
   * other TreeWriters, this one can't record the position in the streams
   * until the stripe is being flushed. Therefore it saves all of the entries
   * and augments them with the final information as the stripe is written.
   */
  @Override
  public void createRowIndexEntry() throws IOException {
    getStripeStatistics().merge(indexStatistics);
    OrcProto.RowIndexEntry.Builder rowIndexEntry = getRowIndexEntry();
    rowIndexEntry.setStatistics(indexStatistics.serialize());
    indexStatistics.reset();
    OrcProto.RowIndexEntry base = rowIndexEntry.build();
    savedRowIndex.add(base);
    rowIndexEntry.clear();
    addBloomFilterEntry();
    recordPosition(rowIndexPosition);
    rowIndexValueCount.add((long) rows.size());
    if (strideDictionaryCheck) {
      checkDictionaryEncoding();
    }
    if (!useDictionaryEncoding) {
      if (rows.size() > 0) {
        flushDictionary();
        // just record the start positions of next index stride
        recordDirectStreamPosition();
      } else {
        // record the start positions of next index stride
        recordDirectStreamPosition();
        getRowIndex().addEntry(base);
      }
    }
  }

  private void recordDirectStreamPosition() throws IOException {
    if (rowIndexPosition != null) {
      directStreamOutput.getPosition(rowIndexPosition);
      lengthOutput.getPosition(rowIndexPosition);
    }
  }

  @Override
  public long estimateMemory() {
    long parent = super.estimateMemory();
    if (useDictionaryEncoding) {
      return parent + dictionary.getSizeInBytes() + rows.getSizeInBytes();
    } else {
      return parent + lengthOutput.estimateMemory() +
          directStreamOutput.getBufferSize();
    }
  }

  @Override
  public long getRawDataSize() {
    // ORC strings are converted to java Strings. so use JavaDataModel to
    // compute the overall size of strings
    StringColumnStatistics scs = (StringColumnStatistics) fileStatistics;
    long numVals = fileStatistics.getNumberOfValues();
    if (numVals == 0) {
      return 0;
    } else {
      int avgSize = (int) (scs.getSum() / numVals);
      return numVals * JavaDataModel.get().lengthForStringOfLength(avgSize);
    }
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    // if rows in stripe is less than dictionaryCheckAfterRows, dictionary
    // checking would not have happened. So do it again here.
    checkDictionaryEncoding();

    if (useDictionaryEncoding) {
      flushDictionary();
      stringOutput.flush();
      lengthOutput.flush();
      rowOutput.flush();
    } else {
      // flushout any left over entries from dictionary
      if (rows.size() > 0) {
        flushDictionary();
      }

      // suppress the stream for every stripe if dictionary is disabled
      stringOutput.suppress();

      directStreamOutput.flush();
      lengthOutput.flush();
    }
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    Consumer<byte[]> updater = CryptoUtils.modifyIvForStripe(stripeId);
    stringOutput.changeIv(updater);
    lengthOutput.changeIv(updater);
    rowOutput.changeIv(updater);
    directStreamOutput.changeIv(updater);
  }

}
