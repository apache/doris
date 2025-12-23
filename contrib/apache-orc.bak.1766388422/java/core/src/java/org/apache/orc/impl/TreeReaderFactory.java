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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.filter.FilterContext;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.tree.BatchReader;
import org.apache.orc.impl.reader.tree.PrimitiveBatchReader;
import org.apache.orc.impl.reader.tree.StructBatchReader;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.impl.writer.TimestampTreeWriter;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * Factory for creating ORC tree readers.
 */
public class TreeReaderFactory {
  public interface Context {
    SchemaEvolution getSchemaEvolution();

    Set<Integer> getColumnFilterIds();

    Consumer<OrcFilterContext> getColumnFilterCallback();

    boolean isSkipCorrupt();

    boolean getUseUTCTimestamp();

    String getWriterTimezone();

    OrcFile.Version getFileFormat();

    ReaderEncryption getEncryption();

    boolean useProlepticGregorian();

    boolean fileUsedProlepticGregorian();

    TypeReader.ReaderCategory getReaderCategory(int columnId);
  }

  public static class ReaderContext implements Context {
    private SchemaEvolution evolution;
    private boolean skipCorrupt = false;
    private boolean useUTCTimestamp = false;
    private String writerTimezone;
    private OrcFile.Version fileFormat;
    private ReaderEncryption encryption;
    private boolean useProlepticGregorian;
    private boolean fileUsedProlepticGregorian;
    private Set<Integer> filterColumnIds = Collections.emptySet();
    Consumer<OrcFilterContext> filterCallback;

    public ReaderContext setSchemaEvolution(SchemaEvolution evolution) {
      this.evolution = evolution;
      return this;
    }

    public ReaderContext setEncryption(ReaderEncryption value) {
      encryption = value;
      return this;
    }

    public ReaderContext setFilterCallback(
        Set<Integer> filterColumnsList, Consumer<OrcFilterContext> filterCallback) {
      this.filterColumnIds = filterColumnsList;
      this.filterCallback = filterCallback;
      return this;
    }

    public ReaderContext skipCorrupt(boolean skipCorrupt) {
      this.skipCorrupt = skipCorrupt;
      return this;
    }

    public ReaderContext useUTCTimestamp(boolean useUTCTimestamp) {
      this.useUTCTimestamp = useUTCTimestamp;
      return this;
    }

    public ReaderContext writerTimeZone(String writerTimezone) {
      this.writerTimezone = writerTimezone;
      return this;
    }

    public ReaderContext fileFormat(OrcFile.Version version) {
      this.fileFormat = version;
      return this;
    }

    public ReaderContext setProlepticGregorian(boolean file,
                                               boolean reader) {
      this.useProlepticGregorian = reader;
      this.fileUsedProlepticGregorian = file;
      return this;
    }

    @Override
    public SchemaEvolution getSchemaEvolution() {
      return evolution;
    }

    @Override
    public Set<Integer> getColumnFilterIds() {
      return filterColumnIds;
    }

    @Override
    public Consumer<OrcFilterContext> getColumnFilterCallback() {
      return filterCallback;
    }

    @Override
    public boolean isSkipCorrupt() {
      return skipCorrupt;
    }

    @Override
    public boolean getUseUTCTimestamp() {
      return useUTCTimestamp;
    }

    @Override
    public String getWriterTimezone() {
      return writerTimezone;
    }

    @Override
    public OrcFile.Version getFileFormat() {
      return fileFormat;
    }

    @Override
    public ReaderEncryption getEncryption() {
      return encryption;
    }

    @Override
    public boolean useProlepticGregorian() {
      return useProlepticGregorian;
    }

    @Override
    public boolean fileUsedProlepticGregorian() {
      return fileUsedProlepticGregorian;
    }

    @Override
    public TypeReader.ReaderCategory getReaderCategory(int columnId) {
      TypeReader.ReaderCategory result;
      if (getColumnFilterIds().contains(columnId)) {
        // parent filter columns that might include non-filter children. These are classified as
        // FILTER_PARENT. This is used during the reposition for non-filter read. Only Struct and
        // Union Readers are supported currently
        TypeDescription col = columnId == -1 ? null : getSchemaEvolution()
            .getFileSchema()
            .findSubtype(columnId);
        if (col == null || col.getChildren() == null || col.getChildren().isEmpty()) {
          result = TypeReader.ReaderCategory.FILTER_CHILD;
        } else {
          result = TypeReader.ReaderCategory.FILTER_PARENT;
        }
      } else {
        result = TypeReader.ReaderCategory.NON_FILTER;
      }
      return result;
    }
  }

  public abstract static class TreeReader implements TypeReader {
    protected final int columnId;
    protected BitFieldReader present = null;
    protected final Context context;
    protected final ReaderCategory readerCategory;

    static final long[] powerOfTenTable = {
        1L,                   // 0
        10L,
        100L,
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
        10_000_000L,
        100_000_000L,           // 8
        1_000_000_000L,
        10_000_000_000L,
        100_000_000_000L,
        1_000_000_000_000L,
        10_000_000_000_000L,
        100_000_000_000_000L,
        1_000_000_000_000_000L,
        10_000_000_000_000_000L,   // 16
        100_000_000_000_000_000L,
        1_000_000_000_000_000_000L, // 18
    };

    TreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, context);
    }

    protected TreeReader(int columnId, InStream in, @NotNull Context context) throws IOException {
      this.columnId = columnId;
      this.context = context;
      if (in == null) {
        present = null;
      } else {
        present = new BitFieldReader(in);
      }
      this.readerCategory = context.getReaderCategory(columnId);
    }

    @Override
    public ReaderCategory getReaderCategory() {
      return readerCategory;
    }

    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    protected static IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
        InStream in,
        boolean signed,
        Context context) throws IOException {
      switch (kind) {
        case DIRECT_V2:
        case DICTIONARY_V2:
          return new RunLengthIntegerReaderV2(in, signed,
                                              context != null && context.isSkipCorrupt());
        case DIRECT:
        case DICTIONARY:
          return new RunLengthIntegerReader(in, signed);
        default:
          throw new IllegalArgumentException("Unknown encoding " + kind);
      }
    }

    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      checkEncoding(planner.getEncoding(columnId));
      InStream in = planner.getStream(new StreamName(columnId,
          OrcProto.Stream.Kind.PRESENT));
      if (in == null) {
        present = null;
      } else {
        present = new BitFieldReader(in);
      }
    }

    /**
     * Seek to the given position.
     *
     * @param index the indexes loaded from the file
     * @param readPhase the current readPhase
     * @throws IOException
     */
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      if (present != null) {
        present.seek(index);
      }
    }

    protected static int countNonNullRowsInRange(boolean[] isNull, int start, int end) {
      int result = 0;
      while (start < end) {
        if (!isNull[start++]) {
          result++;
        }
      }
      return result;
    }

    protected long countNonNulls(long rows) throws IOException {
      if (present != null) {
        long result = 0;
        for (long c = 0; c < rows; ++c) {
          if (present.next() == 1) {
            result += 1;
          }
        }
        return result;
      } else {
        return rows;
      }
    }

    /**
     * Populates the isNull vector array in the previousVector object based on
     * the present stream values. This function is called from all the child
     * readers, and they all set the values based on isNull field value.
     *
     * @param previous The columnVector object whose isNull value is populated
     * @param isNull Whether the each value was null at a higher level. If
     *               isNull is null, all values are non-null.
     * @param batchSize      Size of the column vector
     * @param filterContext the information about the rows that were selected
     *                      by the filter.
     * @param readPhase The read level
     * @throws IOException
     */
    public void nextVector(ColumnVector previous,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (present != null || isNull != null) {
        // Set noNulls and isNull vector of the ColumnVector based on
        // present stream
        previous.noNulls = true;
        boolean allNull = true;
        for (int i = 0; i < batchSize; i++) {
          if (isNull == null || !isNull[i]) {
            if (present != null && present.next() != 1) {
              previous.noNulls = false;
              previous.isNull[i] = true;
            } else {
              previous.isNull[i] = false;
              allNull = false;
            }
          } else {
            previous.noNulls = false;
            previous.isNull[i] = true;
          }
        }
        previous.isRepeating = !previous.noNulls && allNull;
      } else {
        // There is no present stream, this means that all the values are
        // present.
        previous.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          previous.isNull[i] = false;
        }
      }
    }

    public BitFieldReader getPresent() {
      return present;
    }

    @Override
    public int getColumnId() {
      return columnId;
    }
  }

  public static class NullTreeReader extends TreeReader {

    public NullTreeReader(int columnId, Context context) throws IOException {
      super(columnId, context);
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) {
      // PASS
    }

    @Override
    public void skipRows(long rows, ReadPhase readPhase) {
      // PASS
    }

    @Override
    public void seek(PositionProvider position, ReadPhase readPhase) {
      // PASS
    }

    @Override
    public void seek(PositionProvider[] position,
                     ReadPhase readPhase) {
      // PASS
    }

    @Override
    public void nextVector(ColumnVector vector, boolean[] isNull, int size,
                           FilterContext filterContext, ReadPhase readPhase) {
      vector.noNulls = false;
      vector.isNull[0] = true;
      vector.isRepeating = true;
    }
  }

  public static class BooleanTreeReader extends TreeReader {
    protected BitFieldReader reader = null;

    BooleanTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, context);
    }

    protected BooleanTreeReader(int columnId,
                                InStream present,
                                InStream data,
                                Context context) throws IOException {
      super(columnId, present, context);
      if (data != null) {
        reader = new BitFieldReader(data);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      reader = new BitFieldReader(planner.getStream(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      if (filterContext.isSelectedInUse()) {
        reader.nextVector(result, filterContext, batchSize);
      } else {
        // Read value entries based on isNull entries
        reader.nextVector(result, batchSize);
      }
    }
  }

  public static class ByteTreeReader extends TreeReader {
    protected RunLengthByteReader reader = null;

    ByteTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, context);
    }

    protected ByteTreeReader(
        int columnId, InStream present, InStream data, Context context) throws IOException {
      super(columnId, present, context);
      this.reader = new RunLengthByteReader(data);
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      reader = new RunLengthByteReader(planner.getStream(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA)));
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class ShortTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    ShortTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, context);
    }

    protected ShortTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding, Context context)
        throws IOException {
      super(columnId, present, context);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(name), true, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class IntTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    IntTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, context);
    }

    protected IntTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding, Context context)
        throws IOException {
      super(columnId, present, context);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(name), true, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class LongTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    LongTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, context);
    }

    protected LongTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding,
        Context context)
        throws IOException {
      super(columnId, present, context);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(name), true, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class FloatTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    FloatTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, context);
    }

    protected FloatTreeReader(int columnId,
                              InStream present,
                              InStream data,
                              Context context) throws IOException {
      super(columnId, present, context);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = planner.getStream(name);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      stream.seek(index);
    }

    private void nextVector(DoubleColumnVector result,
                            boolean[] isNull,
                            final int batchSize) throws IOException {
      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;

      if (batchSize > 0) {
        if (hasNulls) {
          // conditions to ensure bounds checks skips
          for (int i = 0; batchSize <= result.isNull.length && i < batchSize; i++) {
            allNulls = allNulls & result.isNull[i];
          }
          if (allNulls) {
            result.vector[0] = Double.NaN;
            result.isRepeating = true;
          } else {
            // some nulls
            result.isRepeating = false;
            // conditions to ensure bounds checks skips
            for (int i = 0; batchSize <= result.isNull.length &&
                batchSize <= result.vector.length && i < batchSize; i++) {
              if (!result.isNull[i]) {
                result.vector[i] = utils.readFloat(stream);
              } else {
                // If the value is not present then set NaN
                result.vector[i] = Double.NaN;
              }
            }
          }
        } else {
          // no nulls & > 1 row (check repeating)
          boolean repeating = (batchSize > 1);
          final float f1 = utils.readFloat(stream);
          result.vector[0] = f1;
          // conditions to ensure bounds checks skips
          for (int i = 1; i < batchSize && batchSize <= result.vector.length; i++) {
            final float f2 = utils.readFloat(stream);
            repeating = repeating && (f1 == f2);
            result.vector[i] = f2;
          }
          result.isRepeating = repeating;
        }
      }
    }

    private void nextVector(DoubleColumnVector result,
                            boolean[] isNull,
                            FilterContext filterContext,
                            final int batchSize) throws IOException {
      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;
      result.isRepeating = false;
      int previousIdx = 0;

      if (batchSize > 0) {
        if (hasNulls) {
          // conditions to ensure bounds checks skips
          for (int i = 0; batchSize <= result.isNull.length && i < batchSize; i++) {
            allNulls = allNulls & result.isNull[i];
          }
          if (allNulls) {
            result.vector[0] = Double.NaN;
            result.isRepeating = true;
          } else {
            // some nulls
            // conditions to ensure bounds checks skips
            for (int i = 0; i != filterContext.getSelectedSize(); i++) {
              int idx = filterContext.getSelected()[i];
              if (idx - previousIdx > 0) {
                utils.skipFloat(stream, countNonNullRowsInRange(result.isNull, previousIdx, idx));
              }
              if (!result.isNull[idx]) {
                result.vector[idx] = utils.readFloat(stream);
              } else {
                // If the value is not present then set NaN
                result.vector[idx] = Double.NaN;
              }
              previousIdx = idx + 1;
            }
            utils.skipFloat(stream, countNonNullRowsInRange(result.isNull, previousIdx, batchSize));
          }
        } else {
          // Read only the selected row indexes and skip the rest
          for (int i = 0; i != filterContext.getSelectedSize(); i++) {
            int idx = filterContext.getSelected()[i];
            if (idx - previousIdx > 0) {
              utils.skipFloat(stream,idx - previousIdx);
            }
            result.vector[idx] = utils.readFloat(stream);
            previousIdx = idx + 1;
          }
          utils.skipFloat(stream,batchSize - previousIdx);
        }
      }

    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final DoubleColumnVector result = (DoubleColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      if (filterContext.isSelectedInUse()) {
        nextVector(result, isNull, filterContext, batchSize);
      } else {
        nextVector(result, isNull, batchSize);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      for (int i = 0; i < items; ++i) {
        utils.readFloat(stream);
      }
    }
  }

  public static class DoubleTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    DoubleTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, context);
    }

    protected DoubleTreeReader(int columnId,
                               InStream present,
                               InStream data,
                               Context context) throws IOException {
      super(columnId, present, context);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name =
          new StreamName(columnId,
              OrcProto.Stream.Kind.DATA);
      stream = planner.getStream(name);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      stream.seek(index);
    }

    private void nextVector(DoubleColumnVector result,
                            boolean[] isNull,
                            FilterContext filterContext,
                            final int batchSize) throws IOException {

      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;
      result.isRepeating = false;
      if (batchSize != 0) {
        if (hasNulls) {
          // conditions to ensure bounds checks skips
          for (int i = 0; i < batchSize && batchSize <= result.isNull.length; i++) {
            allNulls = allNulls & result.isNull[i];
          }
          if (allNulls) {
            result.vector[0] = Double.NaN;
            result.isRepeating = true;
          } else {
            // some nulls
            int previousIdx = 0;
            // conditions to ensure bounds checks skips
            for (int i = 0; batchSize <= result.isNull.length &&
                i != filterContext.getSelectedSize(); i++) {
              int idx = filterContext.getSelected()[i];
              if (idx - previousIdx > 0) {
                utils.skipDouble(stream, countNonNullRowsInRange(result.isNull, previousIdx, idx));
              }
              if (!result.isNull[idx]) {
                result.vector[idx] = utils.readDouble(stream);
              } else {
                // If the value is not present then set NaN
                result.vector[idx] = Double.NaN;
              }
              previousIdx = idx + 1;
            }
            utils.skipDouble(stream,
                countNonNullRowsInRange(result.isNull, previousIdx, batchSize));
          }
        } else {
          // no nulls
          int previousIdx = 0;
          // Read only the selected row indexes and skip the rest
          for (int i = 0; i != filterContext.getSelectedSize(); i++) {
            int idx = filterContext.getSelected()[i];
            if (idx - previousIdx > 0) {
              utils.skipDouble(stream, idx - previousIdx);
            }
            result.vector[idx] = utils.readDouble(stream);
            previousIdx = idx + 1;
          }
          utils.skipDouble(stream, batchSize - previousIdx);
        }
      }
    }

    private void nextVector(DoubleColumnVector result,
                           boolean[] isNull,
                           final int batchSize) throws IOException {

      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;
      if (batchSize != 0) {
        if (hasNulls) {
          // conditions to ensure bounds checks skips
          for (int i = 0; i < batchSize && batchSize <= result.isNull.length; i++) {
            allNulls = allNulls & result.isNull[i];
          }
          if (allNulls) {
            result.vector[0] = Double.NaN;
            result.isRepeating = true;
          } else {
            // some nulls
            result.isRepeating = false;
            // conditions to ensure bounds checks skips
            for (int i = 0; batchSize <= result.isNull.length &&
                batchSize <= result.vector.length && i < batchSize; i++) {
              if (!result.isNull[i]) {
                result.vector[i] = utils.readDouble(stream);
              } else {
                // If the value is not present then set NaN
                result.vector[i] = Double.NaN;
              }
            }
          }
        } else {
          // no nulls
          boolean repeating = (batchSize > 1);
          final double d1 = utils.readDouble(stream);
          result.vector[0] = d1;
          // conditions to ensure bounds checks skips
          for (int i = 1; i < batchSize && batchSize <= result.vector.length; i++) {
            final double d2 = utils.readDouble(stream);
            repeating = repeating && (d1 == d2);
            result.vector[i] = d2;
          }
          result.isRepeating = repeating;
        }
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final DoubleColumnVector result = (DoubleColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      if (filterContext.isSelectedInUse()) {
        nextVector(result, isNull, filterContext, batchSize);
      } else {
        nextVector(result, isNull, batchSize);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      long len = items * 8;
      while (len > 0) {
        len -= stream.skip(len);
      }
    }
  }

  public static class BinaryTreeReader extends TreeReader {
    protected InStream stream;
    protected IntegerReader lengths = null;
    protected final LongColumnVector scratchlcv;

    BinaryTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, null, context);
    }

    protected BinaryTreeReader(int columnId, InStream present, InStream data, InStream length,
        OrcProto.ColumnEncoding encoding, Context context) throws IOException {
      super(columnId, present, context);
      scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        checkEncoding(encoding);
        this.lengths = createIntegerReader(encoding.getKind(), length, false, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = planner.getStream(name);
      lengths = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)), false, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      stream.seek(index);
      lengths.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final BytesColumnVector result = (BytesColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      scratchlcv.ensureSize(batchSize, false);
      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for (int i = 0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }
      while (lengthToSkip > 0) {
        lengthToSkip -= stream.skip(lengthToSkip);
      }
    }
  }

  public static class TimestampTreeReader extends TreeReader {
    protected IntegerReader data = null;
    protected IntegerReader nanos = null;
    private final Map<String, Long> baseTimestampMap;
    protected long base_timestamp;
    private final TimeZone readerTimeZone;
    private final boolean instantType;
    private TimeZone writerTimeZone;
    private boolean hasSameTZRules;
    private final ThreadLocal<DateFormat> threadLocalDateFormat;
    private final boolean useProleptic;
    private final boolean fileUsesProleptic;

    TimestampTreeReader(int columnId, Context context,
                        boolean instantType) throws IOException {
      this(columnId, null, null, null, null, context, instantType);
    }

    protected TimestampTreeReader(int columnId, InStream presentStream, InStream dataStream,
                                  InStream nanosStream,
                                  OrcProto.ColumnEncoding encoding,
                                  Context context,
                                  boolean instantType) throws IOException {
      super(columnId, presentStream, context);
      this.instantType = instantType;
      this.threadLocalDateFormat = new ThreadLocal<>();
      this.threadLocalDateFormat.set(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
      this.baseTimestampMap = new HashMap<>();
      if (instantType || context.getUseUTCTimestamp()) {
        this.readerTimeZone = TimeZone.getTimeZone("UTC");
      } else {
        this.readerTimeZone = TimeZone.getDefault();
      }
      if (context.getWriterTimezone() == null || context.getWriterTimezone().isEmpty()) {
        if (instantType) {
          this.base_timestamp = getBaseTimestamp(readerTimeZone.getID()); // UTC
        } else {
          this.base_timestamp = getBaseTimestamp(TimeZone.getDefault().getID());
        }
      } else {
        this.base_timestamp = getBaseTimestamp(context.getWriterTimezone());
      }
      if (encoding != null) {
        checkEncoding(encoding);

        if (dataStream != null) {
          this.data = createIntegerReader(encoding.getKind(), dataStream, true, context);
        }

        if (nanosStream != null) {
          this.nanos = createIntegerReader(encoding.getKind(), nanosStream, false, context);
        }
      }
      fileUsesProleptic = context.fileUsedProlepticGregorian();
      useProleptic = context.useProlepticGregorian();
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      OrcProto.ColumnEncoding.Kind kind = planner.getEncoding(columnId).getKind();
      data = createIntegerReader(kind,
          planner.getStream(new StreamName(columnId,
              OrcProto.Stream.Kind.DATA)), true, context);
      nanos = createIntegerReader(kind,
          planner.getStream(new StreamName(columnId,
              OrcProto.Stream.Kind.SECONDARY)), false, context);
      if (!instantType) {
        base_timestamp = getBaseTimestamp(planner.getWriterTimezone());
      }
    }

    protected long getBaseTimestamp(String timeZoneId) throws IOException {
      // to make sure new readers read old files in the same way
      if (timeZoneId == null || timeZoneId.isEmpty()) {
        timeZoneId = writerTimeZone.getID();
      }

      if (writerTimeZone == null || !timeZoneId.equals(writerTimeZone.getID())) {
        writerTimeZone = TimeZone.getTimeZone(timeZoneId);
        hasSameTZRules = writerTimeZone.hasSameRules(readerTimeZone);
        if (!baseTimestampMap.containsKey(timeZoneId)) {
          threadLocalDateFormat.get().setTimeZone(writerTimeZone);
          try {
            long epoch = threadLocalDateFormat.get()
                             .parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime() /
                             TimestampTreeWriter.MILLIS_PER_SECOND;
            baseTimestampMap.put(timeZoneId, epoch);
            return epoch;
          } catch (ParseException e) {
            throw new IOException("Unable to create base timestamp", e);
          } finally {
            threadLocalDateFormat.get().setTimeZone(readerTimeZone);
          }
        } else {
          return baseTimestampMap.get(timeZoneId);
        }
      }

      return base_timestamp;
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      data.seek(index);
      nanos.seek(index);
    }

    public void readTimestamp(TimestampColumnVector result, int idx) throws IOException {
      final int newNanos = parseNanos(nanos.next());
      long millis = (data.next() + base_timestamp)
          * TimestampTreeWriter.MILLIS_PER_SECOND + newNanos / 1_000_000;
      if (millis < 0 && newNanos > 999_999) {
        millis -= TimestampTreeWriter.MILLIS_PER_SECOND;
      }
      long offset = 0;
      // If reader and writer time zones have different rules, adjust the timezone difference
      // between reader and writer taking day light savings into account.
      if (!hasSameTZRules) {
        offset = SerializationUtils.convertBetweenTimezones(writerTimeZone,
            readerTimeZone, millis);
      }
      result.time[idx] = millis + offset;
      result.nanos[idx] = newNanos;
    }

    public void nextVector(TimestampColumnVector result,
                           boolean[] isNull,
                           final int batchSize) throws IOException {

      for (int i = 0; i < batchSize; i++) {
        if (result.noNulls || !result.isNull[i]) {
          readTimestamp(result, i);
          if (result.isRepeating && i != 0 &&
              (result.time[0] != result.time[i] ||
                  result.nanos[0] != result.nanos[i])) {
            result.isRepeating = false;
          }
        }
      }
      result.changeCalendar(useProleptic, true);
    }

    public void nextVector(TimestampColumnVector result,
                           boolean[] isNull,
                           FilterContext filterContext,
                           final int batchSize) throws IOException {
      result.isRepeating = false;
      int previousIdx = 0;
      if (result.noNulls) {
        for (int i = 0; i != filterContext.getSelectedSize(); i++) {
          int idx = filterContext.getSelected()[i];
          if (idx - previousIdx > 0) {
            skipStreamRows(idx - previousIdx);
          }
          readTimestamp(result, idx);
          previousIdx = idx + 1;
        }
        skipStreamRows(batchSize - previousIdx);
      } else {
        for (int i = 0; i != filterContext.getSelectedSize(); i++) {
          int idx = filterContext.getSelected()[i];
          if (idx - previousIdx > 0) {
            skipStreamRows(countNonNullRowsInRange(result.isNull, previousIdx, idx));
          }
          if (!result.isNull[idx]) {
            readTimestamp(result, idx);
          }
          previousIdx = idx + 1;
        }
        skipStreamRows(countNonNullRowsInRange(result.isNull, previousIdx, batchSize));
      }
      result.changeCalendar(useProleptic, true);

    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      TimestampColumnVector result = (TimestampColumnVector) previousVector;
      result.changeCalendar(fileUsesProleptic, false);
      super.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);

      result.setIsUTC(context.getUseUTCTimestamp());

      if (filterContext.isSelectedInUse()) {
        nextVector(result, isNull, filterContext, batchSize);
      } else {
        nextVector(result, isNull, batchSize);
      }
    }

    private static int parseNanos(long serialized) {
      int zeros = 7 & (int) serialized;
      int result = (int) (serialized >>> 3);
      if (zeros != 0) {
        result *= (int) powerOfTenTable[zeros + 1];
      }
      return result;
    }

    void skipStreamRows(long items) throws IOException {
      data.skip(items);
      nanos.skip(items);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      data.skip(items);
      nanos.skip(items);
    }
  }

  public static class DateTreeReader extends TreeReader {
    protected IntegerReader reader = null;
    private final boolean needsDateColumnVector;
    private final boolean useProleptic;
    private final boolean fileUsesProleptic;

    DateTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, context);
    }

    protected DateTreeReader(int columnId, InStream present, InStream data,
        OrcProto.ColumnEncoding encoding, Context context) throws IOException {
      super(columnId, present, context);
      useProleptic = context.useProlepticGregorian();
      fileUsesProleptic = context.fileUsedProlepticGregorian();
      // if either side is proleptic, we need a DateColumnVector
      needsDateColumnVector = useProleptic || fileUsesProleptic;
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        reader = createIntegerReader(encoding.getKind(), data, true, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(name), true, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final LongColumnVector result = (LongColumnVector) previousVector;
      if (needsDateColumnVector) {
        if (result instanceof DateColumnVector) {
          ((DateColumnVector) result).changeCalendar(fileUsesProleptic, false);
        } else {
          throw new IllegalArgumentException("Can't use LongColumnVector to " +
                                                 "read proleptic Gregorian dates.");
        }
      }

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      // Read value entries based on isNull entries
      reader.nextVector(result, result.vector, batchSize);

      if (needsDateColumnVector) {
        ((DateColumnVector) result).changeCalendar(useProleptic, true);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  public static class DecimalTreeReader extends TreeReader {
    protected final int precision;
    protected final int scale;
    protected InStream valueStream;
    protected IntegerReader scaleReader = null;
    private int[] scratchScaleVector;
    private final byte[] scratchBytes;

    DecimalTreeReader(int columnId,
                      int precision,
                      int scale,
                      Context context) throws IOException {
      this(columnId, null, null, null, null, precision, scale, context);
    }

    protected DecimalTreeReader(int columnId,
                                InStream present,
                                InStream valueStream,
                                InStream scaleStream,
                                OrcProto.ColumnEncoding encoding,
                                int precision,
                                int scale,
                                Context context) throws IOException {
      super(columnId, present, context);
      this.precision = precision;
      this.scale = scale;
      this.scratchScaleVector = new int[VectorizedRowBatch.DEFAULT_SIZE];
      this.valueStream = valueStream;
      this.scratchBytes = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ];
      if (scaleStream != null && encoding != null) {
        checkEncoding(encoding);
        this.scaleReader = createIntegerReader(encoding.getKind(), scaleStream, true, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      valueStream = planner.getStream(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA));
      scaleReader = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(new StreamName(columnId, OrcProto.Stream.Kind.SECONDARY)),
          true, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      valueStream.seek(index);
      scaleReader.seek(index);
    }

    private void nextVector(DecimalColumnVector result,
                            boolean[] isNull,
                            final int batchSize) throws IOException {
      if (batchSize > scratchScaleVector.length) {
        scratchScaleVector = new int[(int) batchSize];
      }
      // read the scales
      scaleReader.nextVector(result, scratchScaleVector, batchSize);
      // Read value entries based on isNull entries
      // Use the fast ORC deserialization method that emulates SerializationUtils.readBigInteger
      // provided by HiveDecimalWritable.
      HiveDecimalWritable[] vector = result.vector;
      HiveDecimalWritable decWritable;
      if (result.noNulls) {
        result.isRepeating = true;
        for (int r = 0; r < batchSize; ++r) {
          decWritable = vector[r];
          if (!decWritable.serializationUtilsRead(
              valueStream, scratchScaleVector[r],
              scratchBytes)) {
            result.isNull[r] = true;
            result.noNulls = false;
          }
          setIsRepeatingIfNeeded(result, r);
        }
      } else if (!result.isRepeating || !result.isNull[0]) {
        result.isRepeating = true;
        for (int r = 0; r < batchSize; ++r) {
          if (!result.isNull[r]) {
            decWritable = vector[r];
            if (!decWritable.serializationUtilsRead(
                valueStream, scratchScaleVector[r],
                scratchBytes)) {
              result.isNull[r] = true;
              result.noNulls = false;
            }
          }
          setIsRepeatingIfNeeded(result, r);
        }
      }
    }

    private void nextVector(DecimalColumnVector result,
                            boolean[] isNull,
                            FilterContext filterContext,
                            final int batchSize) throws IOException {
      // Allocate space for the whole array
      if (batchSize > scratchScaleVector.length) {
        scratchScaleVector = new int[(int) batchSize];
      }
      // But read only read the scales that are needed
      scaleReader.nextVector(result, scratchScaleVector, batchSize);
      // Read value entries based on isNull entries
      // Use the fast ORC deserialization method that emulates SerializationUtils.readBigInteger
      // provided by HiveDecimalWritable.
      HiveDecimalWritable[] vector = result.vector;
      HiveDecimalWritable decWritable;
      if (result.noNulls) {
        result.isRepeating = true;
        int previousIdx = 0;
        for (int r = 0; r != filterContext.getSelectedSize(); ++r) {
          int idx = filterContext.getSelected()[r];
          if (idx - previousIdx > 0) {
            skipStreamRows(idx - previousIdx);
          }
          decWritable = vector[idx];
          if (!decWritable.serializationUtilsRead(
              valueStream, scratchScaleVector[idx],
              scratchBytes)) {
            result.isNull[idx] = true;
            result.noNulls = false;
          }
          setIsRepeatingIfNeeded(result, idx);
          previousIdx = idx + 1;
        }
        skipStreamRows(batchSize - previousIdx);
      } else if (!result.isRepeating || !result.isNull[0]) {
        result.isRepeating = true;
        int previousIdx = 0;
        for (int r = 0; r != filterContext.getSelectedSize(); ++r) {
          int idx = filterContext.getSelected()[r];
          if (idx - previousIdx > 0) {
            skipStreamRows(countNonNullRowsInRange(result.isNull, previousIdx, idx));
          }
          if (!result.isNull[idx]) {
            decWritable = vector[idx];
            if (!decWritable.serializationUtilsRead(
                valueStream, scratchScaleVector[idx],
                scratchBytes)) {
              result.isNull[idx] = true;
              result.noNulls = false;
            }
          }
          setIsRepeatingIfNeeded(result, idx);
          previousIdx = idx + 1;
        }
        skipStreamRows(countNonNullRowsInRange(result.isNull, previousIdx, batchSize));
      }
    }

    private void nextVector(Decimal64ColumnVector result,
                            boolean[] isNull,
                            final int batchSize) throws IOException {
      if (precision > TypeDescription.MAX_DECIMAL64_PRECISION) {
        throw new IllegalArgumentException("Reading large precision type into" +
            " Decimal64ColumnVector.");
      }

      if (batchSize > scratchScaleVector.length) {
        scratchScaleVector = new int[(int) batchSize];
      }
      // read the scales
      scaleReader.nextVector(result, scratchScaleVector, batchSize);
      if (result.noNulls) {
        result.isRepeating = true;
        for (int r = 0; r < batchSize; ++r) {
          final long scaleFactor = powerOfTenTable[scale - scratchScaleVector[r]];
          result.vector[r] = SerializationUtils.readVslong(valueStream) * scaleFactor;
          setIsRepeatingIfNeeded(result, r);
        }
      } else if (!result.isRepeating || !result.isNull[0]) {
        result.isRepeating = true;
        for (int r = 0; r < batchSize; ++r) {
          if (!result.isNull[r]) {
            final long scaleFactor = powerOfTenTable[scale - scratchScaleVector[r]];
            result.vector[r] = SerializationUtils.readVslong(valueStream) * scaleFactor;
          }
          setIsRepeatingIfNeeded(result, r);
        }
      }
      result.precision = (short) precision;
      result.scale = (short) scale;
    }

    private void nextVector(Decimal64ColumnVector result,
        boolean[] isNull,
        FilterContext filterContext,
        final int batchSize) throws IOException {
      if (precision > TypeDescription.MAX_DECIMAL64_PRECISION) {
        throw new IllegalArgumentException("Reading large precision type into" +
            " Decimal64ColumnVector.");
      }
      // Allocate space for the whole array
      if (batchSize > scratchScaleVector.length) {
        scratchScaleVector = new int[(int) batchSize];
      }
      // Read all the scales
      scaleReader.nextVector(result, scratchScaleVector, batchSize);
      if (result.noNulls) {
        result.isRepeating = true;
        int previousIdx = 0;
        for (int r = 0; r != filterContext.getSelectedSize(); r++) {
          int idx = filterContext.getSelected()[r];
          if (idx - previousIdx > 0) {
            skipStreamRows(idx - previousIdx);
          }
          result.vector[idx] = SerializationUtils.readVslong(valueStream);
          for (int s=scratchScaleVector[idx]; s < scale; ++s) {
            result.vector[idx] *= 10;
          }
          setIsRepeatingIfNeeded(result, idx);
          previousIdx = idx + 1;
        }
        skipStreamRows(batchSize - previousIdx);
      } else if (!result.isRepeating || !result.isNull[0]) {
        result.isRepeating = true;
        int previousIdx = 0;
        for (int r = 0; r != filterContext.getSelectedSize(); r++) {
          int idx = filterContext.getSelected()[r];
          if (idx - previousIdx > 0) {
            skipStreamRows(countNonNullRowsInRange(result.isNull, previousIdx, idx));
          }
          if (!result.isNull[idx]) {
            result.vector[idx] = SerializationUtils.readVslong(valueStream);
            for (int s=scratchScaleVector[idx]; s < scale; ++s) {
              result.vector[idx] *= 10;
            }
          }
          setIsRepeatingIfNeeded(result, idx);
          previousIdx = idx + 1;
        }
        skipStreamRows(countNonNullRowsInRange(result.isNull, previousIdx, batchSize));
      }
      result.precision = (short) precision;
      result.scale = (short) scale;
    }

    private void setIsRepeatingIfNeeded(Decimal64ColumnVector result, int index) {
      if (result.isRepeating && index > 0 && (result.vector[0] != result.vector[index] ||
          result.isNull[0] != result.isNull[index])) {
        result.isRepeating = false;
      }
    }

    private void setIsRepeatingIfNeeded(DecimalColumnVector result, int index) {
      if (result.isRepeating && index > 0 && (!result.vector[0].equals(result.vector[index]) ||
          result.isNull[0] != result.isNull[index])) {
        result.isRepeating = false;
      }
    }

    @Override
    public void nextVector(ColumnVector result,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);
      if (result instanceof Decimal64ColumnVector) {
        if (filterContext.isSelectedInUse()) {
          nextVector((Decimal64ColumnVector) result, isNull, filterContext, batchSize);
        } else {
          nextVector((Decimal64ColumnVector) result, isNull, batchSize);
        }
      } else {
        if (filterContext.isSelectedInUse()) {
          nextVector((DecimalColumnVector) result, isNull, filterContext, batchSize);
        } else {
          nextVector((DecimalColumnVector) result, isNull, batchSize);
        }
      }
    }

    void skipStreamRows(long items) throws IOException {
      for (int i = 0; i < items; i++) {
        int input;
        do {
          input = valueStream.read();
          if (input == -1) {
            throw new EOFException("Reading BigInteger past EOF from " + valueStream);
          }
        } while(input >= 128);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      HiveDecimalWritable scratchDecWritable = new HiveDecimalWritable();
      for (int i = 0; i < items; i++) {
        scratchDecWritable.serializationUtilsRead(valueStream, 0, scratchBytes);
      }
      scaleReader.skip(items);
    }
  }

  public static class Decimal64TreeReader extends TreeReader {
    protected final int precision;
    protected final int scale;
    protected final boolean skipCorrupt;
    protected RunLengthIntegerReaderV2 valueReader;

    Decimal64TreeReader(int columnId,
                      int precision,
                      int scale,
                      Context context) throws IOException {
      this(columnId, null, null, null, precision, scale, context);
    }

    protected Decimal64TreeReader(int columnId,
                                InStream present,
                                InStream valueStream,
                                OrcProto.ColumnEncoding encoding,
                                int precision,
                                int scale,
                                Context context) throws IOException {
      super(columnId, present, context);
      this.precision = precision;
      this.scale = scale;
      valueReader = new RunLengthIntegerReaderV2(valueStream, true,
          context.isSkipCorrupt());
      skipCorrupt = context.isSkipCorrupt();
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      InStream stream = planner.getStream(new StreamName(columnId,
          OrcProto.Stream.Kind.DATA));
      valueReader = new RunLengthIntegerReaderV2(stream, true, skipCorrupt);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      valueReader.seek(index);
    }

    private void nextVector(DecimalColumnVector result,
                            FilterContext filterContext,
                            final int batchSize) throws IOException {
      if (result.noNulls) {
        if (filterContext.isSelectedInUse()) {
          result.isRepeating = true;
          int previousIdx = 0;
          for (int r = 0; r != filterContext.getSelectedSize(); ++r) {
            int idx = filterContext.getSelected()[r];
            if (idx - previousIdx > 0) {
              valueReader.skip(idx - previousIdx);
            }
            result.vector[idx].setFromLongAndScale(valueReader.next(), scale);
            setIsRepeatingIfNeeded(result, idx);
            previousIdx = idx + 1;
          }
          valueReader.skip(batchSize - previousIdx);
        } else {
          result.isRepeating = true;
          for (int r = 0; r < batchSize; ++r) {
            result.vector[r].setFromLongAndScale(valueReader.next(), scale);
            setIsRepeatingIfNeeded(result, r);
          }
        }
      } else if (!result.isRepeating || !result.isNull[0]) {
        if (filterContext.isSelectedInUse()) {
          result.isRepeating = true;
          int previousIdx = 0;
          for (int r = 0; r != filterContext.getSelectedSize(); ++r) {
            int idx = filterContext.getSelected()[r];
            if (idx - previousIdx > 0) {
              valueReader.skip(countNonNullRowsInRange(result.isNull, previousIdx, idx));
            }
            if (!result.isNull[r]) {
              result.vector[idx].setFromLongAndScale(valueReader.next(), scale);
            }
            setIsRepeatingIfNeeded(result, idx);
            previousIdx = idx + 1;
          }
          valueReader.skip(countNonNullRowsInRange(result.isNull, previousIdx, batchSize));
        } else {
          result.isRepeating = true;
          for (int r = 0; r < batchSize; ++r) {
            if (!result.isNull[r]) {
              result.vector[r].setFromLongAndScale(valueReader.next(), scale);
            }
            setIsRepeatingIfNeeded(result, r);
          }
        }
      }
      result.precision = (short) precision;
      result.scale = (short) scale;
    }

    private void nextVector(Decimal64ColumnVector result,
                            FilterContext filterContext,
                            final int batchSize) throws IOException {
      valueReader.nextVector(result, result.vector, batchSize);
      result.precision = (short) precision;
      result.scale = (short) scale;
    }

    private void setIsRepeatingIfNeeded(DecimalColumnVector result, int index) {
      if (result.isRepeating && index > 0 && (!result.vector[0].equals(result.vector[index]) ||
          result.isNull[0] != result.isNull[index])) {
        result.isRepeating = false;
      }
    }

    @Override
    public void nextVector(ColumnVector result,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);
      if (result instanceof Decimal64ColumnVector) {
        nextVector((Decimal64ColumnVector) result, filterContext, batchSize);
      } else {
        nextVector((DecimalColumnVector) result, filterContext, batchSize);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      valueReader.skip(items);
    }
  }

  /**
   * A tree reader that will read string columns. At the start of the
   * stripe, it creates an internal reader based on whether a direct or
   * dictionary encoding was used.
   */
  public static class StringTreeReader extends TreeReader {
    protected TypeReader reader;

    StringTreeReader(int columnId, Context context) throws IOException {
      super(columnId, context);
    }

    protected StringTreeReader(int columnId, InStream present, InStream data, InStream length,
        InStream dictionary, OrcProto.ColumnEncoding encoding, Context context) throws IOException {
      super(columnId, present, context);
      if (encoding != null) {
        switch (encoding.getKind()) {
          case DIRECT:
          case DIRECT_V2:
            reader = new StringDirectTreeReader(columnId, present, data, length,
                encoding.getKind(), context);
            break;
          case DICTIONARY:
          case DICTIONARY_V2:
            reader = new StringDictionaryTreeReader(columnId, present, data, length, dictionary,
                encoding, context);
            break;
          default:
            throw new IllegalArgumentException("Unsupported encoding " +
                encoding.getKind());
        }
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      reader.checkEncoding(encoding);
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      // For each stripe, checks the encoding and initializes the appropriate
      // reader
      switch (planner.getEncoding(columnId).getKind()) {
        case DIRECT:
        case DIRECT_V2:
          reader = new StringDirectTreeReader(columnId, context);
          break;
        case DICTIONARY:
        case DICTIONARY_V2:
          reader = new StringDictionaryTreeReader(columnId, context);
          break;
        default:
          throw new IllegalArgumentException("Unsupported encoding " +
              planner.getEncoding(columnId).getKind());
      }
      reader.startStripe(planner, readPhase);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      reader.seek(index, readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      reader.seek(index, readPhase);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      reader.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skipRows(items, readPhase);
    }
  }

  // This class collects together very similar methods for reading an ORC vector of byte arrays and
  // creating the BytesColumnVector.
  //
  public static class BytesColumnVectorUtil {

    private static byte[] commonReadByteArrays(InStream stream, IntegerReader lengths,
        LongColumnVector scratchlcv,
        BytesColumnVector result, final int batchSize) throws IOException {
      // Read lengths
      scratchlcv.isRepeating = result.isRepeating;
      scratchlcv.noNulls = result.noNulls;
      scratchlcv.isNull = result.isNull;  // Notice we are replacing the isNull vector here...
      lengths.nextVector(scratchlcv, scratchlcv.vector, batchSize);
      int totalLength = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            totalLength += (int) scratchlcv.vector[i];
          }
        }
      } else {
        if (!scratchlcv.isNull[0]) {
          totalLength = (int) (batchSize * scratchlcv.vector[0]);
        }
      }
      if (totalLength < 0) {
        StringBuilder sb = new StringBuilder("totalLength:" + totalLength
                + " is a negative number.");
        if (batchSize > 1) {
          sb.append(" The current batch size is ");
          sb.append(batchSize);
          sb.append(", you can reduce the value by '");
          sb.append(OrcConf.ROW_BATCH_SIZE.getAttribute());
          sb.append("'.");
        }
        throw new IOException(sb.toString());
      }
      // Read all the strings for this batch
      byte[] allBytes = new byte[totalLength];
      int offset = 0;
      int len = totalLength;
      while (len > 0) {
        int bytesRead = stream.read(allBytes, offset, len);
        if (bytesRead < 0) {
          throw new EOFException("Can't finish byte read from " + stream);
        }
        len -= bytesRead;
        offset += bytesRead;
      }

      return allBytes;
    }

    // This method has the common code for reading in bytes into a BytesColumnVector.
    public static void readOrcByteArrays(InStream stream,
                                         IntegerReader lengths,
                                         LongColumnVector scratchlcv,
                                         BytesColumnVector result,
                                         final int batchSize) throws IOException {
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        byte[] allBytes =
            commonReadByteArrays(stream, lengths, scratchlcv, result, batchSize);

        // Too expensive to figure out 'repeating' by comparisons.
        result.isRepeating = false;
        int offset = 0;
        if (!scratchlcv.isRepeating) {
          for (int i = 0; i < batchSize; i++) {
            if (!scratchlcv.isNull[i]) {
              result.setRef(i, allBytes, offset, (int) scratchlcv.vector[i]);
              offset += scratchlcv.vector[i];
            } else {
              result.setRef(i, allBytes, 0, 0);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!scratchlcv.isNull[i]) {
              result.setRef(i, allBytes, offset, (int) scratchlcv.vector[0]);
              offset += scratchlcv.vector[0];
            } else {
              result.setRef(i, allBytes, 0, 0);
            }
          }
        }
      }
    }
  }

  /**
   * A reader for string columns that are direct encoded in the current
   * stripe.
   */
  public static class StringDirectTreeReader extends TreeReader {
    private static final HadoopShims SHIMS = HadoopShimsFactory.get();
    protected InStream stream;
    protected IntegerReader lengths;
    private final LongColumnVector scratchlcv;

    StringDirectTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, null, context);
    }

    protected StringDirectTreeReader(int columnId, InStream present, InStream data,
                                     InStream length, OrcProto.ColumnEncoding.Kind encoding,
                                     Context context) throws IOException {
      super(columnId, present, context);
      this.scratchlcv = new LongColumnVector();
      this.stream = data;
      if (length != null && encoding != null) {
        this.lengths = createIntegerReader(encoding, length, false, context);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      stream = planner.getStream(name);
      lengths = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
          false, context);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      stream.seek(index);
      // don't seek data stream
      lengths.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final BytesColumnVector result = (BytesColumnVector) previousVector;

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);

      scratchlcv.ensureSize(batchSize, false);
      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv,
          result, batchSize);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for (int i = 0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }

      while (lengthToSkip > 0) {
        lengthToSkip -= stream.skip(lengthToSkip);
      }
    }

    public IntegerReader getLengths() {
      return lengths;
    }

    public InStream getStream() {
      return stream;
    }
  }

  /**
   * A reader for string columns that are dictionary encoded in the current
   * stripe.
   */
  public static class StringDictionaryTreeReader extends TreeReader {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private int[] dictionaryOffsets;
    protected IntegerReader reader;
    private InStream lengthStream;
    private InStream dictionaryStream;
    private OrcProto.ColumnEncoding lengthEncoding;

    private byte[] dictionaryBuffer = null;
    private final LongColumnVector scratchlcv;
    private boolean initDictionary = false;

    StringDictionaryTreeReader(int columnId, Context context) throws IOException {
      this(columnId, null, null, null, null, null, context);
    }

    protected StringDictionaryTreeReader(int columnId, InStream present, InStream data,
        InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding,
        Context context) throws IOException {
      super(columnId, present, context);
      scratchlcv = new LongColumnVector();
      if (data != null && encoding != null) {
        this.reader = createIntegerReader(encoding.getKind(), data, false, context);
      }
      lengthStream = length;
      dictionaryStream = dictionary;
      lengthEncoding = encoding;
      initDictionary = false;
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);

      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      dictionaryStream = planner.getStream(name);
      initDictionary = false;

      // read the lengths
      name = new StreamName(columnId, OrcProto.Stream.Kind.LENGTH);
      InStream in = planner.getStream(name);
      OrcProto.ColumnEncoding encoding = planner.getEncoding(columnId);
      readDictionaryLengthStream(in, encoding);

      // set up the row reader
      name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(encoding.getKind(),
          planner.getStream(name), false, context);
    }

    private void readDictionaryLengthStream(InStream in, OrcProto.ColumnEncoding encoding)
        throws IOException {
      int dictionarySize = encoding.getDictionarySize();
      if (in != null) { // Guard against empty LENGTH stream.
        IntegerReader lenReader = createIntegerReader(encoding.getKind(), in, false, context);
        int offset = 0;
        if (dictionaryOffsets == null ||
            dictionaryOffsets.length < dictionarySize + 1) {
          dictionaryOffsets = new int[dictionarySize + 1];
        }
        for (int i = 0; i < dictionarySize; ++i) {
          dictionaryOffsets[i] = offset;
          offset += (int) lenReader.next();
        }
        dictionaryOffsets[dictionarySize] = offset;
        in.close();
      }

    }

    private void readDictionaryStream(InStream in) throws IOException {
      if (in != null) { // Guard against empty dictionary stream.
        if (in.available() > 0) {
          // remove reference to previous dictionary buffer
          dictionaryBuffer = null;
          int dictionaryBufferSize = dictionaryOffsets[dictionaryOffsets.length - 1];
          dictionaryBuffer = new byte[dictionaryBufferSize];
          int pos = 0;
          // check if dictionary size is smaller than available stream size
          // to avoid ArrayIndexOutOfBoundsException
          int readSize = Math.min(in.available(), dictionaryBufferSize);
          byte[] chunkBytes = new byte[readSize];
          while (pos < dictionaryBufferSize) {
            int currentLength = in.read(chunkBytes, 0, readSize);
            // check if dictionary size is smaller than available stream size
            // to avoid ArrayIndexOutOfBoundsException
            currentLength = Math.min(currentLength, dictionaryBufferSize - pos);
            System.arraycopy(chunkBytes, 0, dictionaryBuffer, pos, currentLength);
            pos += currentLength;
          }
        }
        in.close();
      } else {
        dictionaryBuffer = null;
      }
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      seek(index[columnId], readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      reader.seek(index);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      final BytesColumnVector result = (BytesColumnVector) previousVector;

      // remove reference to previous dictionary buffer
      for (int i = 0; i < batchSize; i++) {
        result.vector[i] = null;
      }

      // lazy read dictionary buffer,
      // ensure there is at most one dictionary buffer in memory when reading cross different file stripes
      if (!initDictionary) {
        if (lengthStream != null && lengthEncoding != null) {
          readDictionaryLengthStream(lengthStream, lengthEncoding);
        }
        if (dictionaryStream != null) {
          readDictionaryStream(dictionaryStream);
        }
        initDictionary = true;
      }

      // Read present/isNull stream
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);
      readDictionaryByteArray(result, filterContext, batchSize);
    }

    private void readDictionaryByteArray(BytesColumnVector result,
                                         FilterContext filterContext,
                                         int batchSize) throws IOException {
      int offset;
      int length;

      if (dictionaryBuffer != null) {

        // Read string offsets
        scratchlcv.isRepeating = result.isRepeating;
        scratchlcv.noNulls = result.noNulls;
        scratchlcv.isNull = result.isNull;
        scratchlcv.ensureSize(batchSize, false);
        reader.nextVector(scratchlcv, scratchlcv.vector, batchSize);
        if (!scratchlcv.isRepeating) {
          // The vector has non-repeating strings. Iterate thru the batch
          // and set strings one by one
          if (filterContext.isSelectedInUse()) {
            // Set all string values to null - offset and length is zero
            for (int i = 0; i < batchSize; i++) {
              result.setRef(i, dictionaryBuffer, 0, 0);
            }
            // Read selected rows from stream
            for (int i = 0; i != filterContext.getSelectedSize(); i++) {
              int idx = filterContext.getSelected()[i];
              if (!scratchlcv.isNull[idx]) {
                offset = dictionaryOffsets[(int) scratchlcv.vector[idx]];
                length = getDictionaryEntryLength((int) scratchlcv.vector[idx], offset);
                result.setRef(idx, dictionaryBuffer, offset, length);
              }
            }
          } else {
            for (int i = 0; i < batchSize; i++) {
              if (!scratchlcv.isNull[i]) {
                offset = dictionaryOffsets[(int) scratchlcv.vector[i]];
                length = getDictionaryEntryLength((int) scratchlcv.vector[i], offset);
                result.setRef(i, dictionaryBuffer, offset, length);
              } else {
                // If the value is null then set offset and length to zero (null string)
                result.setRef(i, dictionaryBuffer, 0, 0);
              }
            }
          }
        } else {
          // If the value is repeating then just set the first value in the
          // vector and set the isRepeating flag to true. No need to iterate thru and
          // set all the elements to the same value
          offset = dictionaryOffsets[(int) scratchlcv.vector[0]];
          length = getDictionaryEntryLength((int) scratchlcv.vector[0], offset);
          result.setRef(0, dictionaryBuffer, offset, length);
        }
        result.isRepeating = scratchlcv.isRepeating;
      } else {
        if (dictionaryOffsets == null) {
          // Entire stripe contains null strings.
          result.isRepeating = true;
          result.noNulls = false;
          result.isNull[0] = true;
          result.setRef(0, EMPTY_BYTE_ARRAY, 0, 0);
        } else {
          // stripe contains nulls and empty strings
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              result.setRef(i, EMPTY_BYTE_ARRAY, 0, 0);
            }
          }
        }
      }
    }

    int getDictionaryEntryLength(int entry, int offset) {
      final int length;
      // if it isn't the last entry, subtract the offsets otherwise use
      // the buffer length.
      if (entry < dictionaryOffsets.length - 1) {
        length = dictionaryOffsets[entry + 1] - offset;
      } else {
        length = dictionaryBuffer.length - offset;
      }
      return length;
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      reader.skip(countNonNulls(items));
    }

    public IntegerReader getReader() {
      return reader;
    }
  }

  public static class CharTreeReader extends StringTreeReader {
    int maxLength;

    CharTreeReader(int columnId, int maxLength, Context context) throws IOException {
      this(columnId, maxLength, null, null, null, null, null, context);
    }

    protected CharTreeReader(int columnId, int maxLength, InStream present, InStream data,
                             InStream length, InStream dictionary, OrcProto.ColumnEncoding encoding,
                             Context context) throws IOException {
      super(columnId, present, data, length, dictionary, encoding, context);
      this.maxLength = maxLength;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (right trim and truncate) if necessary.
      super.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);
      BytesColumnVector result = (BytesColumnVector) previousVector;
      int adjustedDownLen;
      if (result.isRepeating) {
        if (result.noNulls || !result.isNull[0]) {
          adjustedDownLen = StringExpr
              .rightTrimAndTruncate(result.vector[0], result.start[0], result.length[0], maxLength);
          if (adjustedDownLen < result.length[0]) {
            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
          }
        }
      } else {
        if (result.noNulls) {
          for (int i = 0; i < batchSize; i++) {
            adjustedDownLen = StringExpr
                .rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i],
                    maxLength);
            if (adjustedDownLen < result.length[i]) {
              result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              adjustedDownLen = StringExpr
                  .rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i],
                      maxLength);
              if (adjustedDownLen < result.length[i]) {
                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
              }
            }
          }
        }
      }
    }
  }

  public static class VarcharTreeReader extends StringTreeReader {
    int maxLength;

    VarcharTreeReader(int columnId, int maxLength, Context context) throws IOException {
      this(columnId, maxLength, null, null, null, null, null, context);
    }

    protected VarcharTreeReader(int columnId, int maxLength, InStream present, InStream data,
                                InStream length, InStream dictionary,
                                OrcProto.ColumnEncoding encoding,
                                Context context) throws IOException {
      super(columnId, present, data, length, dictionary, encoding, context);
      this.maxLength = maxLength;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      // Get the vector of strings from StringTreeReader, then make a 2nd pass to
      // adjust down the length (truncate) if necessary.
      super.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);
      BytesColumnVector result = (BytesColumnVector) previousVector;

      int adjustedDownLen;
      if (result.isRepeating) {
        if (result.noNulls || !result.isNull[0]) {
          adjustedDownLen = StringExpr
              .truncate(result.vector[0], result.start[0], result.length[0], maxLength);
          if (adjustedDownLen < result.length[0]) {
            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
          }
        }
      } else {
        if (result.noNulls) {
          for (int i = 0; i < batchSize; i++) {
            adjustedDownLen = StringExpr
                .truncate(result.vector[i], result.start[i], result.length[i], maxLength);
            if (adjustedDownLen < result.length[i]) {
              result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
            }
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!result.isNull[i]) {
              adjustedDownLen = StringExpr
                  .truncate(result.vector[i], result.start[i], result.length[i], maxLength);
              if (adjustedDownLen < result.length[i]) {
                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
              }
            }
          }
        }
      }
    }
  }

  public static class StructTreeReader extends TreeReader {
    public final TypeReader[] fields;

    protected StructTreeReader(int columnId,
                               TypeDescription readerSchema,
                               Context context) throws IOException {
      super(columnId, null, context);

      List<TypeDescription> childrenTypes = readerSchema.getChildren();
      this.fields = new TypeReader[childrenTypes.size()];
      for (int i = 0; i < fields.length; ++i) {
        TypeDescription subtype = childrenTypes.get(i);
        this.fields[i] = createTreeReader(subtype, context);
      }
    }

    public TypeReader[] getChildReaders() {
      return fields;
    }

    protected StructTreeReader(int columnId, InStream present,
                               Context context,
                               OrcProto.ColumnEncoding encoding,
                               TypeReader[] childReaders) throws IOException {
      super(columnId, present, context);
      if (encoding != null) {
        checkEncoding(encoding);
      }
      this.fields = childReaders;
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      if (readPhase.contains(this.readerCategory)) {
        super.seek(index, readPhase);
      }
      for (TypeReader kid : fields) {
        if (kid != null && TypeReader.shouldProcessChild(kid, readPhase)) {
          kid.seek(index, readPhase);
        }
      }
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      if (readPhase.contains(this.readerCategory)) {
        super.seek(index, readPhase);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      StructColumnVector result = (StructColumnVector) previousVector;
      if(readPhase.contains(this.readerCategory)) {
        super.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);
        if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
          result.isRepeating = false;
        }
      }
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        // Read all the members of struct as column vectors
        boolean[] mask = result.noNulls ? null : result.isNull;
        for (int f = 0; f < fields.length; f++) {
          if (fields[f] != null && TypeReader.shouldProcessChild(fields[f], readPhase)) {
            fields[f].nextVector(result.fields[f], mask, batchSize, filterContext, readPhase);
          }
        }
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      if (readPhase.contains(this.readerCategory)) {
        super.startStripe(planner, readPhase);
      }

      for (TypeReader field : fields) {
        if (field != null && TypeReader.shouldProcessChild(field, readPhase)) {
          field.startStripe(planner, readPhase);
        }
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      if (!readPhase.contains(this.readerCategory)) {
        return;
      }
      items = countNonNulls(items);
      for (TypeReader field : fields) {
        if (field != null && TypeReader.shouldProcessChild(field, readPhase)) {
          field.skipRows(items, readPhase);
        }
      }
    }
  }

  public static class UnionTreeReader extends TreeReader {
    protected final TypeReader[] fields;
    protected RunLengthByteReader tags;

    protected UnionTreeReader(int fileColumn,
                              TypeDescription readerSchema,
                              Context context) throws IOException {
      super(fileColumn, null, context);
      List<TypeDescription> childrenTypes = readerSchema.getChildren();
      int fieldCount = childrenTypes.size();
      this.fields = new TypeReader[fieldCount];
      for (int i = 0; i < fieldCount; ++i) {
        TypeDescription subtype = childrenTypes.get(i);
        this.fields[i] = createTreeReader(subtype, context);
      }
    }

    protected UnionTreeReader(int columnId, InStream present,
                              Context context,
                              OrcProto.ColumnEncoding encoding,
                              TypeReader[] childReaders) throws IOException {
      super(columnId, present, context);
      if (encoding != null) {
        checkEncoding(encoding);
      }
      this.fields = childReaders;
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      if (readPhase.contains(this.readerCategory)) {
        super.seek(index, readPhase);
        tags.seek(index[columnId]);
      }
      for (TypeReader kid : fields) {
        if (TypeReader.shouldProcessChild(kid, readPhase)) {
          kid.seek(index, readPhase);
        }
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      UnionColumnVector result = (UnionColumnVector) previousVector;
      if (readPhase.contains(this.readerCategory)) {
        super.nextVector(result, isNull, batchSize, filterContext, readPhase);
        if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
          result.isRepeating = false;
          tags.nextVector(result.noNulls ? null : result.isNull, result.tags,
                          batchSize);
        }
      }

      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        boolean[] ignore = new boolean[(int) batchSize];
        for (int f = 0; f < result.fields.length; ++f) {
          if (TypeReader.shouldProcessChild(fields[f], readPhase)) {
            // build the ignore list for this tag
            for (int r = 0; r < batchSize; ++r) {
              ignore[r] = (!result.noNulls && result.isNull[r]) ||
                          result.tags[r] != f;
            }
            fields[f].nextVector(result.fields[f], ignore, batchSize, filterContext, readPhase);
          }
        }
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      if (readPhase.contains(this.readerCategory)) {
        super.startStripe(planner, readPhase);
        tags = new RunLengthByteReader(planner.getStream(
            new StreamName(columnId, OrcProto.Stream.Kind.DATA)));
      }
      for (TypeReader field : fields) {
        if (field != null && TypeReader.shouldProcessChild(field, readPhase)) {
          field.startStripe(planner, readPhase);
        }
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      if (!readPhase.contains(this.readerCategory)) {
        return;
      }

      items = countNonNulls(items);
      long[] counts = new long[fields.length];
      for (int i = 0; i < items; ++i) {
        counts[tags.next()] += 1;
      }
      for (int i = 0; i < counts.length; ++i) {
        if (TypeReader.shouldProcessChild(fields[i], readPhase)) {
          fields[i].skipRows(counts[i], readPhase);
        }
      }
    }
  }

  private static final FilterContext NULL_FILTER = new FilterContext() {
    @Override
    public void reset() {
    }

    @Override
    public boolean isSelectedInUse() {
      return false;
    }

    @Override
    public int[] getSelected() {
      return new int[0];
    }

    @Override
    public int getSelectedSize() {
      return 0;
    }
  };

  public static class ListTreeReader extends TreeReader {
    protected final TypeReader elementReader;
    protected IntegerReader lengths = null;

    protected ListTreeReader(int fileColumn,
                             TypeDescription readerSchema,
                             Context context) throws IOException {
      super(fileColumn, context);
      TypeDescription elementType = readerSchema.getChildren().get(0);
      elementReader = createTreeReader(elementType, context);
    }

    protected ListTreeReader(int columnId,
                             InStream present,
                             Context context,
                             InStream data,
                             OrcProto.ColumnEncoding encoding,
                             TypeReader elementReader) throws IOException {
      super(columnId, present, context);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.lengths = createIntegerReader(encoding.getKind(), data, false, context);
      }
      this.elementReader = elementReader;
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      lengths.seek(index[columnId]);
      elementReader.seek(index, readPhase);
    }

    @Override
    public void nextVector(ColumnVector previous,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      ListColumnVector result = (ListColumnVector) previous;
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);
      // if we have some none-null values, then read them
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        lengths.nextVector(result, result.lengths, batchSize);
        // even with repeating lengths, the list doesn't repeat
        result.isRepeating = false;
        // build the offsets vector and figure out how many children to read
        result.childCount = 0;
        for (int r = 0; r < batchSize; ++r) {
          if (result.noNulls || !result.isNull[r]) {
            result.offsets[r] = result.childCount;
            result.childCount += result.lengths[r];
          }
        }
        result.child.ensureSize(result.childCount, false);
        // We always read all of the children, because the parent filter wouldn't apply right.
        elementReader.nextVector(result.child, null, result.childCount, NULL_FILTER, readPhase);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      lengths = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false, context);
      if (elementReader != null) {
        elementReader.startStripe(planner, readPhase);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for (long i = 0; i < items; ++i) {
        childSkip += lengths.next();
      }
      elementReader.skipRows(childSkip, readPhase);
    }
  }

  public static class MapTreeReader extends TreeReader {
    protected final TypeReader keyReader;
    protected final TypeReader valueReader;
    protected IntegerReader lengths = null;

    protected MapTreeReader(int fileColumn,
                            TypeDescription readerSchema,
                            Context context) throws IOException {
      super(fileColumn, context);
      TypeDescription keyType = readerSchema.getChildren().get(0);
      TypeDescription valueType = readerSchema.getChildren().get(1);
      keyReader = createTreeReader(keyType, context);
      valueReader = createTreeReader(valueType, context);
    }

    protected MapTreeReader(int columnId,
                            InStream present,
                            Context context,
                            InStream data,
                            OrcProto.ColumnEncoding encoding,
                            TypeReader keyReader,
                            TypeReader valueReader) throws IOException {
      super(columnId, present, context);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.lengths = createIntegerReader(encoding.getKind(), data, false, context);
      }
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      super.seek(index, readPhase);
      lengths.seek(index[columnId]);
      keyReader.seek(index, readPhase);
      valueReader.seek(index, readPhase);
    }

    @Override
    public void nextVector(ColumnVector previous,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      MapColumnVector result = (MapColumnVector) previous;
      super.nextVector(result, isNull, batchSize, filterContext, readPhase);
      if (result.noNulls || !(result.isRepeating && result.isNull[0])) {
        lengths.nextVector(result, result.lengths, batchSize);
        // even with repeating lengths, the map doesn't repeat
        result.isRepeating = false;
        // build the offsets vector and figure out how many children to read
        result.childCount = 0;
        for (int r = 0; r < batchSize; ++r) {
          if (result.noNulls || !result.isNull[r]) {
            result.offsets[r] = result.childCount;
            result.childCount += result.lengths[r];
          }
        }
        result.keys.ensureSize(result.childCount, false);
        result.values.ensureSize(result.childCount, false);
        keyReader.nextVector(result.keys, null, result.childCount, NULL_FILTER, readPhase);
        valueReader.nextVector(result.values, null, result.childCount, NULL_FILTER, readPhase);
      }
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      super.startStripe(planner, readPhase);
      lengths = createIntegerReader(planner.getEncoding(columnId).getKind(),
          planner.getStream(new StreamName(columnId,
              OrcProto.Stream.Kind.LENGTH)), false, context);
      if (keyReader != null) {
        keyReader.startStripe(planner, readPhase);
      }
      if (valueReader != null) {
        valueReader.startStripe(planner, readPhase);
      }
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      items = countNonNulls(items);
      long childSkip = 0;
      for (long i = 0; i < items; ++i) {
        childSkip += lengths.next();
      }
      keyReader.skipRows(childSkip, readPhase);
      valueReader.skipRows(childSkip, readPhase);
    }
  }

  public static TypeReader createTreeReader(TypeDescription readerType,
                                            Context context) throws IOException {
    OrcFile.Version version = context.getFileFormat();
    final SchemaEvolution evolution = context.getSchemaEvolution();
    TypeDescription fileType = evolution.getFileType(readerType);
    if (fileType == null || !evolution.includeReaderColumn(readerType.getId())){
      return new NullTreeReader(-1, context);
    }
    TypeDescription.Category readerTypeCategory = readerType.getCategory();
    // We skip attribute checks when comparing types since they are not used to
    // create the ConvertTreeReaders
    if (!fileType.equals(readerType, false) &&
        (readerTypeCategory != TypeDescription.Category.STRUCT &&
         readerTypeCategory != TypeDescription.Category.MAP &&
         readerTypeCategory != TypeDescription.Category.LIST &&
         readerTypeCategory != TypeDescription.Category.UNION)) {
      // We only convert complex children.
      return ConvertTreeReaderFactory.createConvertTreeReader(readerType, context);
    }
    switch (readerTypeCategory) {
      case BOOLEAN:
        return new BooleanTreeReader(fileType.getId(), context);
      case BYTE:
        return new ByteTreeReader(fileType.getId(), context);
      case DOUBLE:
        return new DoubleTreeReader(fileType.getId(), context);
      case FLOAT:
        return new FloatTreeReader(fileType.getId(), context);
      case SHORT:
        return new ShortTreeReader(fileType.getId(), context);
      case INT:
        return new IntTreeReader(fileType.getId(), context);
      case LONG:
        return new LongTreeReader(fileType.getId(), context);
      case STRING:
        return new StringTreeReader(fileType.getId(), context);
      case CHAR:
        return new CharTreeReader(fileType.getId(), readerType.getMaxLength(), context);
      case VARCHAR:
        return new VarcharTreeReader(fileType.getId(), readerType.getMaxLength(), context);
      case BINARY:
        return new BinaryTreeReader(fileType.getId(), context);
      case TIMESTAMP:
        return new TimestampTreeReader(fileType.getId(), context, false);
      case TIMESTAMP_INSTANT:
        return new TimestampTreeReader(fileType.getId(), context, true);
      case DATE:
        return new DateTreeReader(fileType.getId(), context);
      case DECIMAL:
        if (isDecimalAsLong(version, fileType.getPrecision())){
          return new Decimal64TreeReader(fileType.getId(), fileType.getPrecision(),
              fileType.getScale(), context);
        }
        return new DecimalTreeReader(fileType.getId(), fileType.getPrecision(),
            fileType.getScale(), context);
      case STRUCT:
        return new StructTreeReader(fileType.getId(), readerType, context);
      case LIST:
        return new ListTreeReader(fileType.getId(), readerType, context);
      case MAP:
        return new MapTreeReader(fileType.getId(), readerType, context);
      case UNION:
        return new UnionTreeReader(fileType.getId(), readerType, context);
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerTypeCategory);
    }
  }

  public static boolean isDecimalAsLong(OrcFile.Version version, int precision) {
    return version == OrcFile.Version.UNSTABLE_PRE_2_0 &&
        precision <= TypeDescription.MAX_DECIMAL64_PRECISION;
  }

  public static BatchReader createRootReader(TypeDescription readerType, Context context)
          throws IOException {
    TypeReader reader = createTreeReader(readerType, context);
    if (reader instanceof StructTreeReader) {
      return new StructBatchReader(reader, context);
    } else {
      return new PrimitiveBatchReader(reader);
    }
  }
}
