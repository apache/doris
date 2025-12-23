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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.CollectionColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.BatchFilter;
import org.apache.orc.impl.filter.FilterFactory;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.tree.BatchReader;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.chrono.ChronoLocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.function.Consumer;

public class RecordReaderImpl implements RecordReader {
  static final Logger LOG = LoggerFactory.getLogger(RecordReaderImpl.class);
  private static final boolean isLogDebugEnabled = LOG.isDebugEnabled();
  // as public for use with test cases
  public static final OrcProto.ColumnStatistics EMPTY_COLUMN_STATISTICS =
      OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(0)
          .setHasNull(false)
          .setBytesOnDisk(0)
          .build();
  protected final Path path;
  private final long firstRow;
  private final List<StripeInformation> stripes = new ArrayList<>();
  private OrcProto.StripeFooter stripeFooter;
  private final long totalRowCount;
  protected final TypeDescription schema;
  // the file included columns indexed by the file's column ids.
  private final boolean[] fileIncluded;
  private final long rowIndexStride;
  private long rowInStripe = 0;
  // position of the follow reader within the stripe
  private long followRowInStripe = 0;
  private int currentStripe = -1;
  private long rowBaseInStripe = 0;
  private long rowCountInStripe = 0;
  private final BatchReader reader;
  private final OrcIndex indexes;
  // identifies the columns requiring row indexes
  private final boolean[] rowIndexColsToRead;
  private final SargApplier sargApp;
  // an array about which row groups aren't skipped
  private boolean[] includedRowGroups = null;
  private final DataReader dataReader;
  private final int maxDiskRangeChunkLimit;
  private final StripePlanner planner;
  // identifies the type of read, ALL(read everything), LEADERS(read only the filter columns)
  private final TypeReader.ReadPhase startReadPhase;
  // identifies that follow columns bytes must be read
  private boolean needsFollowColumnsRead;
  private final boolean noSelectedVector;
  // identifies whether the file has bad bloom filters that we should not use.
  private final boolean skipBloomFilters;
  static final String[] BAD_CPP_BLOOM_FILTER_VERSIONS = {
      "1.6.0", "1.6.1", "1.6.2", "1.6.3", "1.6.4", "1.6.5", "1.6.6", "1.6.7", "1.6.8",
      "1.6.9", "1.6.10", "1.6.11", "1.7.0"};

  /**
   * Given a list of column names, find the given column and return the index.
   *
   * @param evolution the mapping from reader to file schema
   * @param columnName  the fully qualified column name to look for
   * @return the file column number or -1 if the column wasn't found in the file schema
   * @throws IllegalArgumentException if the column was not found in the reader schema
   */
  static int findColumns(SchemaEvolution evolution,
                         String columnName) {
    TypeDescription fileColumn = findColumnType(evolution, columnName);
    return fileColumn == null ? -1 : fileColumn.getId();
  }

  static TypeDescription findColumnType(SchemaEvolution evolution, String columnName) {
    try {
      TypeDescription readerColumn = evolution.getReaderBaseSchema().findSubtype(
          columnName, evolution.isSchemaEvolutionCaseAware);
      return evolution.getFileType(readerColumn);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Filter could not find column with name: " +
                                         columnName + " on " + evolution.getReaderBaseSchema(),
                                         e);
    }
  }

  /**
   * Given a column name such as 'a.b.c', this method returns the column 'a.b.c' if present in the
   * file. In case 'a.b.c' is not found in file then it tries to look for 'a.b', then 'a'. If none
   * are present then it shall return null.
   *
   * @param evolution the mapping from reader to file schema
   * @param columnName the fully qualified column name to look for
   * @return the file column type or null in case none of the branch columns are present in the file
   * @throws IllegalArgumentException if the column was not found in the reader schema
   */
  static TypeDescription findMostCommonColumn(SchemaEvolution evolution, String columnName) {
    try {
      TypeDescription readerColumn = evolution.getReaderBaseSchema().findSubtype(
          columnName, evolution.isSchemaEvolutionCaseAware);
      TypeDescription fileColumn;
      do {
        fileColumn = evolution.getFileType(readerColumn);
        if (fileColumn == null) {
          readerColumn = readerColumn.getParent();
        } else {
          return fileColumn;
        }
      } while (readerColumn != null);
      return null;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Filter could not find column with name: " +
                                         columnName + " on " + evolution.getReaderBaseSchema(),
                                         e);
    }
  }

  /**
   * Find the mapping from predicate leaves to columns.
   * @param sargLeaves the search argument that we need to map
   * @param evolution the mapping from reader to file schema
   * @return an array mapping the sarg leaves to concrete column numbers in the
   * file
   */
  public static int[] mapSargColumnsToOrcInternalColIdx(
                            List<PredicateLeaf> sargLeaves,
                            SchemaEvolution evolution) {
    int[] result = new int[sargLeaves.size()];
    for (int i = 0; i < sargLeaves.size(); ++i) {
      int colNum = -1;
      try {
        String colName = sargLeaves.get(i).getColumnName();
        colNum = findColumns(evolution, colName);
      } catch (IllegalArgumentException e) {
        LOG.debug("{}", e.getMessage());
      }
      result[i] = colNum;
    }
    return result;
  }

  protected RecordReaderImpl(ReaderImpl fileReader,
                             Reader.Options options) throws IOException {
    OrcFile.WriterVersion writerVersion = fileReader.getWriterVersion();
    SchemaEvolution evolution;
    if (options.getSchema() == null) {
      LOG.info("Reader schema not provided -- using file schema " +
          fileReader.getSchema());
      evolution = new SchemaEvolution(fileReader.getSchema(), null, options);
    } else {

      // Now that we are creating a record reader for a file, validate that
      // the schema to read is compatible with the file schema.
      //
      evolution = new SchemaEvolution(fileReader.getSchema(),
                                      options.getSchema(),
                                      options);
      if (LOG.isDebugEnabled() && evolution.hasConversion()) {
        LOG.debug("ORC file " + fileReader.path.toString() +
            " has data type conversion --\n" +
            "reader schema: " + options.getSchema().toString() + "\n" +
            "file schema:   " + fileReader.getSchema());
      }
    }

    this.noSelectedVector = !options.useSelected();
    LOG.debug("noSelectedVector={}", this.noSelectedVector);
    this.schema = evolution.getReaderSchema();
    this.path = fileReader.path;
    this.rowIndexStride = fileReader.rowIndexStride;
    boolean ignoreNonUtf8BloomFilter =
        OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(fileReader.conf);
    ReaderEncryption encryption = fileReader.getEncryption();
    this.fileIncluded = evolution.getFileIncluded();
    SearchArgument sarg = options.getSearchArgument();
    boolean[] rowIndexCols = new boolean[evolution.getFileIncluded().length];
    if (sarg != null && rowIndexStride > 0) {
      sargApp = new SargApplier(sarg,
          rowIndexStride,
          evolution,
          writerVersion,
          fileReader.useUTCTimestamp,
          fileReader.writerUsedProlepticGregorian(),
          fileReader.options.getConvertToProlepticGregorian());
      sargApp.setRowIndexCols(rowIndexCols);
    } else {
      sargApp = null;
    }

    long rows = 0;
    long skippedRows = 0;
    long offset = options.getOffset();
    long maxOffset = options.getMaxOffset();
    for(StripeInformation stripe: fileReader.getStripes()) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < maxOffset) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }
    this.maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(fileReader.conf);
    Boolean zeroCopy = options.getUseZeroCopy();
    if (zeroCopy == null) {
      zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(fileReader.conf);
    }
    if (options.getDataReader() != null) {
      this.dataReader = options.getDataReader().clone();
    } else {
      InStream.StreamOptions unencryptedOptions =
          InStream.options()
              .withCodec(OrcCodecPool.getCodec(fileReader.getCompressionKind()))
              .withBufferSize(fileReader.getCompressionSize());
      DataReaderProperties.Builder builder =
          DataReaderProperties.builder()
              .withCompression(unencryptedOptions)
              .withFileSystemSupplier(fileReader.getFileSystemSupplier())
              .withPath(fileReader.path)
              .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
              .withZeroCopy(zeroCopy)
              .withMinSeekSize(options.minSeekSize())
              .withMinSeekSizeTolerance(options.minSeekSizeTolerance());
      FSDataInputStream file = fileReader.takeFile();
      if (file != null) {
        builder.withFile(file);
      }
      this.dataReader = RecordReaderUtils.createDefaultDataReader(
          builder.build());
    }
    firstRow = skippedRows;
    totalRowCount = rows;
    Boolean skipCorrupt = options.getSkipCorruptRecords();
    if (skipCorrupt == null) {
      skipCorrupt = OrcConf.SKIP_CORRUPT_DATA.getBoolean(fileReader.conf);
    }

    String[] filterCols = null;
    Consumer<OrcFilterContext> filterCallBack = null;
    String filePath = options.allowPluginFilters() ?
        fileReader.getFileSystem().makeQualified(fileReader.path).toString() : null;
    BatchFilter filter = FilterFactory.createBatchFilter(options,
                                                         evolution.getReaderBaseSchema(),
                                                         evolution.isSchemaEvolutionCaseAware(),
                                                         fileReader.getFileVersion(),
                                                         false,
                                                         filePath,
                                                         fileReader.conf);
    if (filter != null) {
      // If a filter is determined then use this
      filterCallBack = filter;
      filterCols = filter.getColumnNames();
    }

    // Map columnNames to ColumnIds
    SortedSet<Integer> filterColIds = new TreeSet<>();
    if (filterCols != null) {
      for (String colName : filterCols) {
        TypeDescription expandCol = findColumnType(evolution, colName);
        // If the column is not present in the file then this can be ignored from read.
        if (expandCol == null || expandCol.getId() == -1) {
          // Add -1 to filter columns so that the NullTreeReader is invoked during the LEADERS phase
          filterColIds.add(-1);
          // Determine the common parent and include these
          expandCol = findMostCommonColumn(evolution, colName);
        }
        while (expandCol != null && expandCol.getId() != -1) {
          // classify the column and the parent branch as LEAD
          filterColIds.add(expandCol.getId());
          rowIndexCols[expandCol.getId()] = true;
          expandCol = expandCol.getParent();
        }
      }
      this.startReadPhase = TypeReader.ReadPhase.LEADERS;
      LOG.debug("Using startReadPhase: {} with filter columns: {}", startReadPhase, filterColIds);
    } else {
      this.startReadPhase = TypeReader.ReadPhase.ALL;
    }

    this.rowIndexColsToRead = ArrayUtils.contains(rowIndexCols, true) ? rowIndexCols : null;
    TreeReaderFactory.ReaderContext readerContext =
        new TreeReaderFactory.ReaderContext()
          .setSchemaEvolution(evolution)
          .setFilterCallback(filterColIds, filterCallBack)
          .skipCorrupt(skipCorrupt)
          .fileFormat(fileReader.getFileVersion())
          .useUTCTimestamp(fileReader.useUTCTimestamp)
          .setProlepticGregorian(fileReader.writerUsedProlepticGregorian(),
              fileReader.options.getConvertToProlepticGregorian())
          .setEncryption(encryption);
    reader = TreeReaderFactory.createRootReader(evolution.getReaderSchema(), readerContext);
    skipBloomFilters = hasBadBloomFilters(fileReader.getFileTail().getFooter());

    int columns = evolution.getFileSchema().getMaximumId() + 1;
    indexes = new OrcIndex(new OrcProto.RowIndex[columns],
        new OrcProto.Stream.Kind[columns],
        new OrcProto.BloomFilterIndex[columns]);

    planner = new StripePlanner(evolution.getFileSchema(), encryption,
                                dataReader, writerVersion, ignoreNonUtf8BloomFilter,
                                maxDiskRangeChunkLimit, filterColIds);

    try {
      advanceToNextRow(reader, 0L, true);
    } catch (Exception e) {
      // Try to close since this happens in constructor.
      close();
      long stripeId = stripes.size() == 0 ? 0 : stripes.get(0).getStripeId();
      throw new IOException(String.format("Problem opening stripe %d footer in %s.",
          stripeId, path), e);
    }
  }

  /**
   * Check if the file has inconsistent bloom filters. We will skip using them
   * in the following reads.
   * @return true if it has.
   */
  private boolean hasBadBloomFilters(OrcProto.Footer footer) {
    // Only C++ writer in old releases could have bad bloom filters.
    if (footer.getWriter() != 1) return false;
    // 'softwareVersion' is added in 1.5.13, 1.6.11, and 1.7.0.
    // 1.6.x releases before 1.6.11 won't have it. On the other side, the C++ writer
    // supports writing bloom filters since 1.6.0. So files written by the C++ writer
    // and with 'softwareVersion' unset would have bad bloom filters.
    if (!footer.hasSoftwareVersion()) return true;
    String fullVersion = footer.getSoftwareVersion();
    String version = fullVersion;
    // Deal with snapshot versions, e.g. 1.6.12-SNAPSHOT.
    if (fullVersion.contains("-")) {
      version = fullVersion.substring(0, fullVersion.indexOf('-'));
    }
    for (String v : BAD_CPP_BLOOM_FILTER_VERSIONS) {
      if (v.equals(version)) {
        return true;
      }
    }
    return false;
  }

  public static final class PositionProviderImpl implements PositionProvider {
    private final OrcProto.RowIndexEntry entry;
    private int index;

    public PositionProviderImpl(OrcProto.RowIndexEntry entry) {
      this(entry, 0);
    }

    public PositionProviderImpl(OrcProto.RowIndexEntry entry, int startPos) {
      this.entry = entry;
      this.index = startPos;
    }

    @Override
    public long getNext() {
      return entry.getPositions(index++);
    }
  }

  public static final class ZeroPositionProvider implements PositionProvider {
    @Override
    public long getNext() {
      return 0;
    }
  }

  public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe
                                                ) throws IOException {
    return dataReader.readStripeFooter(stripe);
  }

  enum Location {
    BEFORE, MIN, MIDDLE, MAX, AFTER
  }

  static class ValueRange<T extends Comparable> {
    final Comparable lower;
    final Comparable upper;
    final boolean onlyLowerBound;
    final boolean onlyUpperBound;
    final boolean hasNulls;
    final boolean hasValue;
    final boolean comparable;

    ValueRange(PredicateLeaf predicate,
               T lower, T upper,
               boolean hasNulls,
               boolean onlyLowerBound,
               boolean onlyUpperBound,
               boolean hasValue,
               boolean comparable) {
      PredicateLeaf.Type type = predicate.getType();
      this.lower = getBaseObjectForComparison(type, lower);
      this.upper = getBaseObjectForComparison(type, upper);
      this.hasNulls = hasNulls;
      this.onlyLowerBound = onlyLowerBound;
      this.onlyUpperBound = onlyUpperBound;
      this.hasValue = hasValue;
      this.comparable = comparable;
    }

    ValueRange(PredicateLeaf predicate,
               T lower, T upper,
               boolean hasNulls,
               boolean onlyLowerBound,
               boolean onlyUpperBound) {
      this(predicate, lower, upper, hasNulls, onlyLowerBound, onlyUpperBound,
          lower != null, lower != null);
    }

    ValueRange(PredicateLeaf predicate, T lower, T upper,
               boolean hasNulls) {
      this(predicate, lower, upper, hasNulls, false, false);
    }

    /**
     * A value range where the data is either missing or all null.
     * @param predicate the predicate to test
     * @param hasNulls whether there are nulls
     */
    ValueRange(PredicateLeaf predicate, boolean hasNulls) {
      this(predicate, null, null, hasNulls, false, false);
    }

    boolean hasValues() {
      return hasValue;
    }

    /**
     * Whether min or max is provided for comparison
     * @return is it comparable
     */
    boolean isComparable() {
      return hasValue && comparable;
    }

    /**
     * value range is invalid if the column statistics are non-existent
     * @see ColumnStatisticsImpl#isStatsExists()
     * this method is similar to isStatsExists
     * @return value range is valid or not
     */
    boolean isValid() {
      return hasValue || hasNulls;
    }

    /**
     * Given a point and min and max, determine if the point is before, at the
     * min, in the middle, at the max, or after the range.
     * @param point the point to test
     * @return the location of the point
     */
    Location compare(Comparable point) {
      int minCompare = point.compareTo(lower);
      if (minCompare < 0) {
        return Location.BEFORE;
      } else if (minCompare == 0) {
        return onlyLowerBound ? Location.BEFORE : Location.MIN;
      }
      int maxCompare = point.compareTo(upper);
      if (maxCompare > 0) {
        return Location.AFTER;
      } else if (maxCompare == 0) {
        return onlyUpperBound ? Location.AFTER : Location.MAX;
      }
      return Location.MIDDLE;
    }

    /**
     * Is this range a single point?
     * @return true if min == max
     */
    boolean isSingleton() {
      return lower != null && !onlyLowerBound && !onlyUpperBound &&
                 lower.equals(upper);
    }

    /**
     * Add the null option to the truth value, if the range includes nulls.
     * @param value the original truth value
     * @return the truth value extended with null if appropriate
     */
    TruthValue addNull(TruthValue value) {
      if (hasNulls) {
        switch (value) {
          case YES:
            return TruthValue.YES_NULL;
          case NO:
            return TruthValue.NO_NULL;
          case YES_NO:
            return TruthValue.YES_NO_NULL;
          default:
            return value;
        }
      } else {
        return value;
      }
    }
  }

  /**
   * Get the maximum value out of an index entry.
   * Includes option to specify if timestamp column stats values
   * should be in UTC.
   * @param index the index entry
   * @param predicate the kind of predicate
   * @param useUTCTimestamp use UTC for timestamps
   * @return the object for the maximum value or null if there isn't one
   */
  static ValueRange getValueRange(ColumnStatistics index,
                                  PredicateLeaf predicate,
                                  boolean useUTCTimestamp) {
    if (index.getNumberOfValues() == 0) {
      return new ValueRange<>(predicate, index.hasNull());
    } else if (index instanceof IntegerColumnStatistics) {
      IntegerColumnStatistics stats = (IntegerColumnStatistics) index;
      Long min = stats.getMinimum();
      Long max = stats.getMaximum();
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    } else if (index instanceof CollectionColumnStatistics) {
      CollectionColumnStatistics stats = (CollectionColumnStatistics) index;
      Long min = stats.getMinimumChildren();
      Long max = stats.getMaximumChildren();
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    }else if (index instanceof DoubleColumnStatistics) {
      DoubleColumnStatistics stats = (DoubleColumnStatistics) index;
      Double min = stats.getMinimum();
      Double max = stats.getMaximum();
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    } else if (index instanceof StringColumnStatistics) {
      StringColumnStatistics stats = (StringColumnStatistics) index;
      return new ValueRange<>(predicate, stats.getLowerBound(),
          stats.getUpperBound(), stats.hasNull(), stats.getMinimum() == null,
          stats.getMaximum() == null);
    } else if (index instanceof DateColumnStatistics) {
      DateColumnStatistics stats = (DateColumnStatistics) index;
      ChronoLocalDate min = stats.getMinimumLocalDate();
      ChronoLocalDate max = stats.getMaximumLocalDate();
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    } else if (index instanceof DecimalColumnStatistics) {
      DecimalColumnStatistics stats = (DecimalColumnStatistics) index;
      HiveDecimal min = stats.getMinimum();
      HiveDecimal max = stats.getMaximum();
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    } else if (index instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics stats = (TimestampColumnStatistics) index;
      Timestamp min = useUTCTimestamp ? stats.getMinimumUTC() : stats.getMinimum();
      Timestamp max = useUTCTimestamp ? stats.getMaximumUTC() : stats.getMaximum();
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    } else if (index instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics stats = (BooleanColumnStatistics) index;
      Boolean min = stats.getFalseCount() == 0;
      Boolean max = stats.getTrueCount() != 0;
      return new ValueRange<>(predicate, min, max, stats.hasNull());
    } else {
      return new ValueRange(predicate, null, null, index.hasNull(), false, false, true, false);
    }
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * @param statsProto the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @param bloomFilter the bloom filter
   * @param writerVersion the version of software that wrote the file
   * @param type what is the kind of this column
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */
  static TruthValue evaluatePredicateProto(OrcProto.ColumnStatistics statsProto,
                                           PredicateLeaf predicate,
                                           OrcProto.Stream.Kind kind,
                                           OrcProto.ColumnEncoding encoding,
                                           OrcProto.BloomFilter bloomFilter,
                                           OrcFile.WriterVersion writerVersion,
                                           TypeDescription type) {
    return evaluatePredicateProto(statsProto, predicate, kind, encoding, bloomFilter,
        writerVersion, type, true, false);
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * Includes option to specify if timestamp column stats values
   * should be in UTC and if the file writer used proleptic Gregorian calendar.
   * @param statsProto the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @param bloomFilter the bloom filter
   * @param writerVersion the version of software that wrote the file
   * @param type what is the kind of this column
   * @param writerUsedProlepticGregorian file written using the proleptic Gregorian calendar
   * @param useUTCTimestamp
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */

  static TruthValue evaluatePredicateProto(OrcProto.ColumnStatistics statsProto,
                                           PredicateLeaf predicate,
                                           OrcProto.Stream.Kind kind,
                                           OrcProto.ColumnEncoding encoding,
                                           OrcProto.BloomFilter bloomFilter,
                                           OrcFile.WriterVersion writerVersion,
                                           TypeDescription type,
                                           boolean writerUsedProlepticGregorian,
                                           boolean useUTCTimestamp) {
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(
        null, statsProto, writerUsedProlepticGregorian, true);
    ValueRange range = getValueRange(cs, predicate, useUTCTimestamp);

    // files written before ORC-135 stores timestamp wrt to local timezone causing issues with PPD.
    // disable PPD for timestamp for all old files
    TypeDescription.Category category = type.getCategory();
    if (category == TypeDescription.Category.TIMESTAMP) {
      if (!writerVersion.includes(OrcFile.WriterVersion.ORC_135)) {
        LOG.debug("Not using predication pushdown on {} because it doesn't " +
                  "include ORC-135. Writer version: {}",
            predicate.getColumnName(), writerVersion);
        return range.addNull(TruthValue.YES_NO);
      }
      if (predicate.getType() != PredicateLeaf.Type.TIMESTAMP &&
          predicate.getType() != PredicateLeaf.Type.DATE &&
          predicate.getType() != PredicateLeaf.Type.STRING) {
        return range.addNull(TruthValue.YES_NO);
      }
    } else if (writerVersion == OrcFile.WriterVersion.ORC_135 &&
               category == TypeDescription.Category.DECIMAL &&
               type.getPrecision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
      // ORC 1.5.0 to 1.5.5, which use WriterVersion.ORC_135, have broken
      // min and max values for decimal64. See ORC-517.
      LOG.debug("Not using predicate push down on {}, because the file doesn't"+
                   " include ORC-517. Writer version: {}",
          predicate.getColumnName(), writerVersion);
      return TruthValue.YES_NO_NULL;
    } else if (category == TypeDescription.Category.DOUBLE ||
        category == TypeDescription.Category.FLOAT) {
      DoubleColumnStatistics dstas = (DoubleColumnStatistics) cs;
      if (Double.isNaN(dstas.getSum())) {
        LOG.debug("Not using predication pushdown on {} because stats contain NaN values",
                predicate.getColumnName());
        return dstas.hasNull() ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
      }
    }
    return evaluatePredicateRange(predicate, range,
        BloomFilterIO.deserialize(kind, encoding, writerVersion, type.getCategory(),
            bloomFilter), useUTCTimestamp);
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * @param stats the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */
  public static TruthValue evaluatePredicate(ColumnStatistics stats,
                                             PredicateLeaf predicate,
                                             BloomFilter bloomFilter) {
    return evaluatePredicate(stats, predicate, bloomFilter, false);
  }

  /**
   * Evaluate a predicate with respect to the statistics from the column
   * that is referenced in the predicate.
   * Includes option to specify if timestamp column stats values
   * should be in UTC.
   * @param stats the statistics for the column mentioned in the predicate
   * @param predicate the leaf predicate we need to evaluation
   * @param bloomFilter
   * @param useUTCTimestamp
   * @return the set of truth values that may be returned for the given
   *   predicate.
   */
  public static TruthValue evaluatePredicate(ColumnStatistics stats,
                                             PredicateLeaf predicate,
                                             BloomFilter bloomFilter,
                                             boolean useUTCTimestamp) {
    ValueRange range = getValueRange(stats, predicate, useUTCTimestamp);

    return evaluatePredicateRange(predicate, range, bloomFilter, useUTCTimestamp);
  }

  static TruthValue evaluatePredicateRange(PredicateLeaf predicate,
                                           ValueRange range,
                                           BloomFilter bloomFilter,
                                           boolean useUTCTimestamp) {
    if (!range.isValid()) {
      return TruthValue.YES_NO_NULL;
    }

    // if we didn't have any values, everything must have been null
    if (!range.hasValues()) {
      if (predicate.getOperator() == PredicateLeaf.Operator.IS_NULL) {
        return TruthValue.YES;
      } else {
        return TruthValue.NULL;
      }
    } else if (!range.isComparable()) {
      return range.hasNulls ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
    }

    TruthValue result;
    Comparable baseObj = (Comparable) predicate.getLiteral();
    // Predicate object and stats objects are converted to the type of the predicate object.
    Comparable predObj = getBaseObjectForComparison(predicate.getType(), baseObj);

    result = evaluatePredicateMinMax(predicate, predObj, range);
    if (shouldEvaluateBloomFilter(predicate, result, bloomFilter)) {
      return evaluatePredicateBloomFilter(
          predicate, predObj, bloomFilter, range.hasNulls, useUTCTimestamp);
    } else {
      return result;
    }
  }

  private static boolean shouldEvaluateBloomFilter(PredicateLeaf predicate,
      TruthValue result, BloomFilter bloomFilter) {
    // evaluate bloom filter only when
    // 1) Bloom filter is available
    // 2) Min/Max evaluation yield YES or MAYBE
    // 3) Predicate is EQUALS or IN list
    return bloomFilter != null &&
           result != TruthValue.NO_NULL && result != TruthValue.NO &&
           (predicate.getOperator().equals(PredicateLeaf.Operator.EQUALS) ||
               predicate.getOperator().equals(PredicateLeaf.Operator.NULL_SAFE_EQUALS) ||
               predicate.getOperator().equals(PredicateLeaf.Operator.IN));
  }

  private static TruthValue evaluatePredicateMinMax(PredicateLeaf predicate,
                                                    Comparable predObj,
                                                    ValueRange range) {
    Location loc;

    switch (predicate.getOperator()) {
      case NULL_SAFE_EQUALS:
        loc = range.compare(predObj);
        if (loc == Location.BEFORE || loc == Location.AFTER) {
          return TruthValue.NO;
        } else {
          return TruthValue.YES_NO;
        }
      case EQUALS:
        loc = range.compare(predObj);
        if (range.isSingleton() && loc == Location.MIN) {
          return range.addNull(TruthValue.YES);
        } else if (loc == Location.BEFORE || loc == Location.AFTER) {
          return range.addNull(TruthValue.NO);
        } else {
          return range.addNull(TruthValue.YES_NO);
        }
      case LESS_THAN:
        loc = range.compare(predObj);
        if (loc == Location.AFTER) {
          return range.addNull(TruthValue.YES);
        } else if (loc == Location.BEFORE || loc == Location.MIN) {
          return range.addNull(TruthValue.NO);
        } else {
          return range.addNull(TruthValue.YES_NO);
        }
      case LESS_THAN_EQUALS:
        loc = range.compare(predObj);
        if (loc == Location.AFTER || loc == Location.MAX ||
            (loc == Location.MIN && range.isSingleton())) {
          return range.addNull(TruthValue.YES);
        } else if (loc == Location.BEFORE) {
          return range.addNull(TruthValue.NO);
        } else {
          return range.addNull(TruthValue.YES_NO);
        }
      case IN:
        if (range.isSingleton()) {
          // for a single value, look through to see if that value is in the
          // set
          for (Object arg : predicate.getLiteralList()) {
            predObj = getBaseObjectForComparison(predicate.getType(), (Comparable) arg);
            if (range.compare(predObj) == Location.MIN) {
              return range.addNull(TruthValue.YES);
            }
          }
          return range.addNull(TruthValue.NO);
        } else {
          // are all of the values outside of the range?
          for (Object arg : predicate.getLiteralList()) {
            predObj = getBaseObjectForComparison(predicate.getType(), (Comparable) arg);
            loc = range.compare(predObj);
            if (loc == Location.MIN || loc == Location.MIDDLE ||
                loc == Location.MAX) {
              return range.addNull(TruthValue.YES_NO);
            }
          }
          return range.addNull(TruthValue.NO);
        }
      case BETWEEN:
        List<Object> args = predicate.getLiteralList();
        if (args == null || args.isEmpty()) {
          return range.addNull(TruthValue.YES_NO);
        }
        Comparable predObj1 = getBaseObjectForComparison(predicate.getType(),
            (Comparable) args.get(0));

        loc = range.compare(predObj1);
        if (loc == Location.BEFORE || loc == Location.MIN) {
          Comparable predObj2 = getBaseObjectForComparison(predicate.getType(),
              (Comparable) args.get(1));
          Location loc2 = range.compare(predObj2);
          if (loc2 == Location.AFTER || loc2 == Location.MAX) {
            return range.addNull(TruthValue.YES);
          } else if (loc2 == Location.BEFORE) {
            return range.addNull(TruthValue.NO);
          } else {
            return range.addNull(TruthValue.YES_NO);
          }
        } else if (loc == Location.AFTER) {
          return range.addNull(TruthValue.NO);
        } else {
          return range.addNull(TruthValue.YES_NO);
        }
      case IS_NULL:
        // min = null condition above handles the all-nulls YES case
        return range.hasNulls ? TruthValue.YES_NO : TruthValue.NO;
      default:
        return range.addNull(TruthValue.YES_NO);
    }
  }

  private static TruthValue evaluatePredicateBloomFilter(PredicateLeaf predicate,
      final Object predObj, BloomFilter bloomFilter, boolean hasNull, boolean useUTCTimestamp) {
    switch (predicate.getOperator()) {
      case NULL_SAFE_EQUALS:
        // null safe equals does not return *_NULL variant. So set hasNull to false
        return checkInBloomFilter(bloomFilter, predObj, false, useUTCTimestamp);
      case EQUALS:
        return checkInBloomFilter(bloomFilter, predObj, hasNull, useUTCTimestamp);
      case IN:
        for (Object arg : predicate.getLiteralList()) {
          // if atleast one value in IN list exist in bloom filter, qualify the row group/stripe
          Object predObjItem = getBaseObjectForComparison(predicate.getType(), (Comparable) arg);
          TruthValue result =
              checkInBloomFilter(bloomFilter, predObjItem, hasNull, useUTCTimestamp);
          if (result == TruthValue.YES_NO_NULL || result == TruthValue.YES_NO) {
            return result;
          }
        }
        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
      default:
        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
    }
  }

  private static TruthValue checkInBloomFilter(BloomFilter bf,
                                               Object predObj,
                                               boolean hasNull,
                                               boolean useUTCTimestamp) {
    TruthValue result = hasNull ? TruthValue.NO_NULL : TruthValue.NO;

    if (predObj instanceof Long) {
      if (bf.testLong((Long) predObj)) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof Double) {
      if (bf.testDouble((Double) predObj)) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof String || predObj instanceof Text ||
        predObj instanceof HiveDecimalWritable ||
        predObj instanceof BigDecimal) {
      if (bf.testString(predObj.toString())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else if (predObj instanceof Timestamp) {
      if (useUTCTimestamp) {
        if (bf.testLong(((Timestamp) predObj).getTime())) {
          result = TruthValue.YES_NO_NULL;
        }
      } else {
        if (bf.testLong(SerializationUtils.convertToUtc(
            TimeZone.getDefault(), ((Timestamp) predObj).getTime()))) {
          result = TruthValue.YES_NO_NULL;
        }
      }
    } else if (predObj instanceof ChronoLocalDate) {
      if (bf.testLong(((ChronoLocalDate) predObj).toEpochDay())) {
        result = TruthValue.YES_NO_NULL;
      }
    } else {
      // if the predicate object is null and if hasNull says there are no nulls then return NO
      if (predObj == null && !hasNull) {
        result = TruthValue.NO;
      } else {
        result = TruthValue.YES_NO_NULL;
      }
    }

    if (result == TruthValue.YES_NO_NULL && !hasNull) {
      result = TruthValue.YES_NO;
    }

    LOG.debug("Bloom filter evaluation: {}", result);

    return result;
  }

  /**
   * An exception for when we can't cast things appropriately
   */
  static class SargCastException extends IllegalArgumentException {

    SargCastException(String string) {
      super(string);
    }
  }

  private static Comparable getBaseObjectForComparison(PredicateLeaf.Type type,
                                                       Comparable obj) {
    if (obj == null) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        if (obj instanceof Boolean) {
          return obj;
        } else {
          // will only be true if the string conversion yields "true", all other values are
          // considered false
          return Boolean.valueOf(obj.toString());
        }
      case DATE:
        if (obj instanceof ChronoLocalDate) {
          return obj;
        } else if (obj instanceof java.sql.Date) {
          return ((java.sql.Date) obj).toLocalDate();
        } else if (obj instanceof Date) {
          return LocalDateTime.ofInstant(((Date) obj).toInstant(),
              ZoneOffset.UTC).toLocalDate();
        } else if (obj instanceof String) {
          return LocalDate.parse((String) obj);
        } else if (obj instanceof Timestamp) {
          return ((Timestamp) obj).toLocalDateTime().toLocalDate();
        }
        // always string, but prevent the comparison to numbers (are they days/seconds/milliseconds?)
        break;
      case DECIMAL:
        if (obj instanceof Boolean) {
          return new HiveDecimalWritable((Boolean) obj ?
              HiveDecimal.ONE : HiveDecimal.ZERO);
        } else if (obj instanceof Integer) {
          return new HiveDecimalWritable((Integer) obj);
        } else if (obj instanceof Long) {
          return new HiveDecimalWritable(((Long) obj));
        } else if (obj instanceof Float || obj instanceof Double ||
            obj instanceof String) {
          return new HiveDecimalWritable(obj.toString());
        } else if (obj instanceof BigDecimal) {
          return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) obj));
        } else if (obj instanceof HiveDecimal) {
          return new HiveDecimalWritable((HiveDecimal) obj);
        } else if (obj instanceof HiveDecimalWritable) {
          return obj;
        } else if (obj instanceof Timestamp) {
          return new HiveDecimalWritable(Double.toString(
              TimestampUtils.getDouble((Timestamp) obj)));
        }
        break;
      case FLOAT:
        if (obj instanceof Number) {
          // widening conversion
          return ((Number) obj).doubleValue();
        } else if (obj instanceof HiveDecimal) {
          return ((HiveDecimal) obj).doubleValue();
        } else if (obj instanceof String) {
          return Double.valueOf(obj.toString());
        } else if (obj instanceof Timestamp) {
          return TimestampUtils.getDouble((Timestamp) obj);
        }
        break;
      case LONG:
        if (obj instanceof Number) {
          // widening conversion
          return ((Number) obj).longValue();
        } else if (obj instanceof HiveDecimal) {
          return ((HiveDecimal) obj).longValue();
        } else if (obj instanceof String) {
          return Long.valueOf(obj.toString());
        }
        break;
      case STRING:
        if (obj instanceof ChronoLocalDate) {
          ChronoLocalDate date = (ChronoLocalDate) obj;
          return date.format(DateTimeFormatter.ISO_LOCAL_DATE
              .withChronology(date.getChronology()));
        }
        return (obj.toString());
      case TIMESTAMP:
        if (obj instanceof Timestamp) {
          return obj;
        } else if (obj instanceof Integer) {
          return new Timestamp(((Number) obj).longValue());
        } else if (obj instanceof Float) {
          return TimestampUtils.doubleToTimestamp(((Float) obj).doubleValue());
        } else if (obj instanceof Double) {
          return TimestampUtils.doubleToTimestamp((Double) obj);
        } else if (obj instanceof HiveDecimal) {
          return TimestampUtils.decimalToTimestamp((HiveDecimal) obj);
        } else if (obj instanceof HiveDecimalWritable) {
          return TimestampUtils.decimalToTimestamp(((HiveDecimalWritable) obj).getHiveDecimal());
        } else if (obj instanceof Date) {
          return new Timestamp(((Date) obj).getTime());
        } else if (obj instanceof ChronoLocalDate) {
          return new Timestamp(((ChronoLocalDate) obj).atTime(LocalTime.MIDNIGHT)
              .toInstant(ZoneOffset.UTC).getEpochSecond() * 1000L);
        }
        // float/double conversion to timestamp is interpreted as seconds whereas integer conversion
        // to timestamp is interpreted as milliseconds by default. The integer to timestamp casting
        // is also config driven. The filter operator changes its promotion based on config:
        // "int.timestamp.conversion.in.seconds". Disable PPD for integer cases.
        break;
      default:
        break;
    }

    throw new SargCastException(String.format(
        "ORC SARGS could not convert from %s to %s", obj.getClass()
            .getSimpleName(), type));
  }

  public static class SargApplier {
    public static final boolean[] READ_ALL_RGS = null;
    public static final boolean[] READ_NO_RGS = new boolean[0];

    private final OrcFile.WriterVersion writerVersion;
    private final SearchArgument sarg;
    private final List<PredicateLeaf> sargLeaves;
    private final int[] filterColumns;
    private final long rowIndexStride;
    // same as the above array, but indices are set to true
    private final SchemaEvolution evolution;
    private final long[] exceptionCount;
    private final boolean useUTCTimestamp;
    private final boolean writerUsedProlepticGregorian;
    private final boolean convertToProlepticGregorian;

    /**
     * @deprecated Use the constructor having full parameters. This exists for backward compatibility.
     */
    public SargApplier(SearchArgument sarg,
                       long rowIndexStride,
                       SchemaEvolution evolution,
                       OrcFile.WriterVersion writerVersion,
                       boolean useUTCTimestamp) {
      this(sarg, rowIndexStride, evolution, writerVersion, useUTCTimestamp, false, false);
    }

    public SargApplier(SearchArgument sarg,
                       long rowIndexStride,
                       SchemaEvolution evolution,
                       OrcFile.WriterVersion writerVersion,
                       boolean useUTCTimestamp,
                       boolean writerUsedProlepticGregorian,
                       boolean convertToProlepticGregorian) {
      this.writerVersion = writerVersion;
      this.sarg = sarg;
      sargLeaves = sarg.getLeaves();
      this.writerUsedProlepticGregorian = writerUsedProlepticGregorian;
      this.convertToProlepticGregorian = convertToProlepticGregorian;
      filterColumns = mapSargColumnsToOrcInternalColIdx(sargLeaves,
                                                        evolution);
      this.rowIndexStride = rowIndexStride;
      this.evolution = evolution;
      exceptionCount = new long[sargLeaves.size()];
      this.useUTCTimestamp = useUTCTimestamp;
    }

    public void setRowIndexCols(boolean[] rowIndexCols) {
      // included will not be null, row options will fill the array with
      // trues if null
      for (int i : filterColumns) {
        // filter columns may have -1 as index which could be partition
        // column in SARG.
        if (i > 0) {
          rowIndexCols[i] = true;
        }
      }
    }

    /**
     * Pick the row groups that we need to load from the current stripe.
     *
     * @return an array with a boolean for each row group or null if all of the
     * row groups must be read.
     * @throws IOException
     */
    public boolean[] pickRowGroups(StripeInformation stripe,
                                   OrcProto.RowIndex[] indexes,
                                   OrcProto.Stream.Kind[] bloomFilterKinds,
                                   List<OrcProto.ColumnEncoding> encodings,
                                   OrcProto.BloomFilterIndex[] bloomFilterIndices,
                                   boolean returnNone) throws IOException {
      long rowsInStripe = stripe.getNumberOfRows();
      int groupsInStripe = (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
      boolean[] result = new boolean[groupsInStripe]; // TODO: avoid alloc?
      TruthValue[] leafValues = new TruthValue[sargLeaves.size()];
      boolean hasSelected = false;
      boolean hasSkipped = false;
      TruthValue[] exceptionAnswer = new TruthValue[leafValues.length];
      for (int rowGroup = 0; rowGroup < result.length; ++rowGroup) {
        for (int pred = 0; pred < leafValues.length; ++pred) {
          int columnIx = filterColumns[pred];
          if (columnIx == -1) {
            // the column is a virtual column
            leafValues[pred] = TruthValue.YES_NO_NULL;
          } else if (exceptionAnswer[pred] != null) {
            leafValues[pred] = exceptionAnswer[pred];
          } else {
            if (indexes[columnIx] == null) {
              LOG.warn("Index is not populated for " + columnIx);
              return READ_ALL_RGS;
            }
            OrcProto.RowIndexEntry entry = indexes[columnIx].getEntry(rowGroup);
            if (entry == null) {
              throw new AssertionError("RG is not populated for " + columnIx + " rg " + rowGroup);
            }
            OrcProto.ColumnStatistics stats = EMPTY_COLUMN_STATISTICS;
            if (entry.hasStatistics()) {
              stats = entry.getStatistics();
            }
            OrcProto.BloomFilter bf = null;
            OrcProto.Stream.Kind bfk = null;
            if (bloomFilterIndices != null && bloomFilterIndices[columnIx] != null) {
              bfk = bloomFilterKinds[columnIx];
              bf = bloomFilterIndices[columnIx].getBloomFilter(rowGroup);
            }
            if (evolution != null && evolution.isPPDSafeConversion(columnIx)) {
              PredicateLeaf predicate = sargLeaves.get(pred);
              try {
                leafValues[pred] = evaluatePredicateProto(stats,
                    predicate, bfk, encodings.get(columnIx), bf,
                    writerVersion, evolution.getFileSchema().
                    findSubtype(columnIx),
                    writerUsedProlepticGregorian, useUTCTimestamp);
              } catch (Exception e) {
                exceptionCount[pred] += 1;
                if (e instanceof SargCastException) {
                  LOG.info("Skipping ORC PPD - " + e.getMessage() + " on "
                      + predicate);
                } else {
                  final String reason = e.getClass().getSimpleName() +
                      " when evaluating predicate." +
                      " Skipping ORC PPD." +
                      " Stats: " + stats +
                      " Predicate: " + predicate;
                  LOG.warn(reason, e);
                }
                boolean hasNoNull = stats.hasHasNull() && !stats.getHasNull();
                if (predicate.getOperator().equals(PredicateLeaf.Operator.NULL_SAFE_EQUALS) ||
                    hasNoNull) {
                  exceptionAnswer[pred] = TruthValue.YES_NO;
                } else {
                  exceptionAnswer[pred] = TruthValue.YES_NO_NULL;
                }
                leafValues[pred] = exceptionAnswer[pred];
              }
            } else {
              leafValues[pred] = TruthValue.YES_NO_NULL;
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace("Stats = " + stats);
              LOG.trace("Setting " + sargLeaves.get(pred) + " to " + leafValues[pred]);
            }
          }
        }
        result[rowGroup] = sarg.evaluate(leafValues).isNeeded();
        hasSelected = hasSelected || result[rowGroup];
        hasSkipped = hasSkipped || (!result[rowGroup]);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Row group " + (rowIndexStride * rowGroup) + " to " +
              (rowIndexStride * (rowGroup + 1) - 1) + " is " +
              (result[rowGroup] ? "" : "not ") + "included.");
        }
      }

      return hasSkipped ? ((hasSelected || !returnNone) ? result : READ_NO_RGS) : READ_ALL_RGS;
    }

    /**
     * Get the count of exceptions for testing.
     * @return
     */
    long[] getExceptionCount() {
      return exceptionCount;
    }
  }

  /**
   * Pick the row groups that we need to load from the current stripe.
   *
   * @return an array with a boolean for each row group or null if all of the
   * row groups must be read.
   * @throws IOException
   */
  protected boolean[] pickRowGroups() throws IOException {
    // Read the Row Indicies if required
    if (rowIndexColsToRead != null) {
      readCurrentStripeRowIndex();
    }

    // In the absence of SArg all rows groups should be included
    if (sargApp == null) {
      return null;
    }
    return sargApp.pickRowGroups(stripes.get(currentStripe),
        indexes.getRowGroupIndex(),
        skipBloomFilters ? null : indexes.getBloomFilterKinds(),
        stripeFooter.getColumnsList(),
        skipBloomFilters ? null : indexes.getBloomFilterIndex(),
        false);
  }

  private void clearStreams() {
    planner.clearStreams();
  }

  /**
   * Read the current stripe into memory.
   *
   * @throws IOException
   */
  private void readStripe() throws IOException {
    StripeInformation stripe = beginReadStripe();
    planner.parseStripe(stripe, fileIncluded);
    includedRowGroups = pickRowGroups();

    // move forward to the first unskipped row
    if (includedRowGroups != null) {
      while (rowInStripe < rowCountInStripe &&
          !includedRowGroups[(int) (rowInStripe / rowIndexStride)]) {
        rowInStripe = Math.min(rowCountInStripe, rowInStripe + rowIndexStride);
      }
    }

    // if we haven't skipped the whole stripe, read the data
    if (rowInStripe < rowCountInStripe) {
      planner.readData(indexes, includedRowGroups, false, startReadPhase);
      reader.startStripe(planner, startReadPhase);
      needsFollowColumnsRead = true;
      // if we skipped the first row group, move the pointers forward
      if (rowInStripe != 0) {
        seekToRowEntry(reader, (int) (rowInStripe / rowIndexStride), startReadPhase);
      }
    }
  }

  private StripeInformation beginReadStripe() throws IOException {
    StripeInformation stripe = stripes.get(currentStripe);
    stripeFooter = readStripeFooter(stripe);
    clearStreams();
    // setup the position in the stripe
    rowCountInStripe = stripe.getNumberOfRows();
    rowInStripe = 0;
    followRowInStripe = 0;
    rowBaseInStripe = 0;
    for (int i = 0; i < currentStripe; ++i) {
      rowBaseInStripe += stripes.get(i).getNumberOfRows();
    }
    // reset all of the indexes
    OrcProto.RowIndex[] rowIndex = indexes.getRowGroupIndex();
    for (int i = 0; i < rowIndex.length; ++i) {
      rowIndex[i] = null;
    }
    return stripe;
  }

  /**
   * Read the next stripe until we find a row that we don't skip.
   *
   * @throws IOException
   */
  private void advanceStripe() throws IOException {
    rowInStripe = rowCountInStripe;
    while (rowInStripe >= rowCountInStripe &&
        currentStripe < stripes.size() - 1) {
      currentStripe += 1;
      readStripe();
    }
  }

  /**
   * Determine the RowGroup based on the supplied row id.
   * @param rowIdx Row for which the row group is being determined
   * @return Id of the RowGroup that the row belongs to
   */
  private int computeRGIdx(long rowIdx) {
    return rowIndexStride == 0 ? 0 : (int) (rowIdx / rowIndexStride);
  }

  /**
   * Skip over rows that we aren't selecting, so that the next row is
   * one that we will read.
   *
   * @param nextRow the row we want to go to
   * @throws IOException
   */
  private boolean advanceToNextRow(BatchReader reader, long nextRow, boolean canAdvanceStripe)
      throws IOException {
    long nextRowInStripe = nextRow - rowBaseInStripe;
    // check for row skipping
    if (rowIndexStride != 0 &&
        includedRowGroups != null &&
        nextRowInStripe < rowCountInStripe) {
      int rowGroup = computeRGIdx(nextRowInStripe);
      if (!includedRowGroups[rowGroup]) {
        while (rowGroup < includedRowGroups.length && !includedRowGroups[rowGroup]) {
          rowGroup += 1;
        }
        if (rowGroup >= includedRowGroups.length) {
          if (canAdvanceStripe) {
            advanceStripe();
          }
          return canAdvanceStripe;
        }
        nextRowInStripe = Math.min(rowCountInStripe, rowGroup * rowIndexStride);
      }
    }
    if (nextRowInStripe >= rowCountInStripe) {
      if (canAdvanceStripe) {
        advanceStripe();
      }
      return canAdvanceStripe;
    }
    if (nextRowInStripe != rowInStripe) {
      if (rowIndexStride != 0) {
        int rowGroup = (int) (nextRowInStripe / rowIndexStride);
        seekToRowEntry(reader, rowGroup, startReadPhase);
        reader.skipRows(nextRowInStripe - rowGroup * rowIndexStride, startReadPhase);
      } else {
        reader.skipRows(nextRowInStripe - rowInStripe, startReadPhase);
      }
      rowInStripe = nextRowInStripe;
    }
    return true;
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    try {
      int batchSize;
      // do...while is required to handle the case where the filter eliminates all rows in the
      // batch, we never return an empty batch unless the file is exhausted
      do {
        if (rowInStripe >= rowCountInStripe) {
          currentStripe += 1;
          if (currentStripe >= stripes.size()) {
            batch.size = 0;
            return false;
          }
          // Read stripe in Memory
          readStripe();
          followRowInStripe = rowInStripe;
        }

        batchSize = computeBatchSize(batch.getMaxSize());
        reader.setVectorColumnCount(batch.getDataColumnCount());
        reader.nextBatch(batch, batchSize, startReadPhase);
        if (startReadPhase == TypeReader.ReadPhase.LEADERS && batch.size > 0) {
          // At least 1 row has been selected and as a result we read the follow columns into the
          // row batch
          reader.nextBatch(batch,
                           batchSize,
                           prepareFollowReaders(rowInStripe, followRowInStripe));
          followRowInStripe = rowInStripe + batchSize;
        }
        rowInStripe += batchSize;
        advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
        // batch.size can be modified by filter so only batchSize can tell if we actually read rows
      } while (batchSize != 0 && batch.size == 0);

      if (noSelectedVector) {
        // In case selected vector is not supported we leave the size to be read size. In this case
        // the non filter columns might be read selectively, however the filter after the reader
        // should eliminate rows that don't match predicate conditions
        batch.size = batchSize;
        batch.selectedInUse = false;
      }

      return batchSize != 0;
    } catch (IOException e) {
      // Rethrow exception with file name in log message
      throw new IOException("Error reading file: " + path, e);
    }
  }

  /**
   * This method prepares the non-filter column readers for next batch. This involves the following
   * 1. Determine position
   * 2. Perform IO if required
   * 3. Position the non-filter readers
   *
   * This method is repositioning the non-filter columns and as such this method shall never have to
   * deal with navigating the stripe forward or skipping row groups, all of this should have already
   * taken place based on the filter columns.
   * @param toFollowRow The rowIdx identifies the required row position within the stripe for
   *                    follow read
   * @param fromFollowRow Indicates the current position of the follow read, exclusive
   * @return the read phase for reading non-filter columns, this shall be FOLLOWERS_AND_PARENTS in
   * case of a seek otherwise will be FOLLOWERS
   */
  private TypeReader.ReadPhase prepareFollowReaders(long toFollowRow, long fromFollowRow)
    throws IOException {
    // 1. Determine the required row group and skip rows needed from the RG start
    int needRG = computeRGIdx(toFollowRow);
    // The current row is not yet read so we -1 to compute the previously read row group
    int readRG = computeRGIdx(fromFollowRow - 1);
    long skipRows;
    if (needRG == readRG && toFollowRow >= fromFollowRow) {
      // In case we are skipping forward within the same row group, we compute skip rows from the
      // current position
      skipRows = toFollowRow - fromFollowRow;
    } else {
      // In all other cases including seeking backwards, we compute the skip rows from the start of
      // the required row group
      skipRows = toFollowRow - (needRG * rowIndexStride);
    }

    // 2. Plan the row group idx for the non-filter columns if this has not already taken place
    if (needsFollowColumnsRead) {
      needsFollowColumnsRead = false;
      planner.readFollowData(indexes, includedRowGroups, needRG, false);
      reader.startStripe(planner, TypeReader.ReadPhase.FOLLOWERS);
    }

    // 3. Position the non-filter readers to the required RG and skipRows
    TypeReader.ReadPhase result = TypeReader.ReadPhase.FOLLOWERS;
    if (needRG != readRG || toFollowRow < fromFollowRow) {
      // When having to change a row group or in case of back navigation, seek both the filter
      // parents and non-filter. This will re-position the parents present vector. This is needed
      // to determine the number of non-null values to skip on the non-filter columns.
      seekToRowEntry(reader, needRG, TypeReader.ReadPhase.FOLLOWERS_AND_PARENTS);
      // skip rows on both the filter parents and non-filter as both have been positioned in the
      // previous step
      reader.skipRows(skipRows, TypeReader.ReadPhase.FOLLOWERS_AND_PARENTS);
      result = TypeReader.ReadPhase.FOLLOWERS_AND_PARENTS;
    } else if (skipRows > 0) {
      // in case we are only skipping within the row group, position the filter parents back to the
      // position of the follow. This is required to determine the non-null values to skip on the
      // non-filter columns.
      seekToRowEntry(reader, readRG, TypeReader.ReadPhase.LEADER_PARENTS);
      reader.skipRows(fromFollowRow - (readRG * rowIndexStride),
          TypeReader.ReadPhase.LEADER_PARENTS);
      // Move both the filter parents and non-filter forward, this will compute the correct
      // non-null skips on follow children
      reader.skipRows(skipRows, TypeReader.ReadPhase.FOLLOWERS_AND_PARENTS);
      result = TypeReader.ReadPhase.FOLLOWERS_AND_PARENTS;
    }
    // Identifies the read level that should be performed for the read
    // FOLLOWERS_WITH_PARENTS indicates repositioning identifying both non-filter and filter parents
    // FOLLOWERS indicates read only of the non-filter level without the parents, which is used during
    // contiguous read. During a contiguous read no skips are needed and the non-null information of
    // the parent is available in the column vector for use during non-filter read
    return result;
  }

  private int computeBatchSize(long targetBatchSize) {
    final int batchSize;
    // In case of PPD, batch size should be aware of row group boundaries. If only a subset of row
    // groups are selected then marker position is set to the end of range (subset of row groups
    // within strip). Batch size computed out of marker position makes sure that batch size is
    // aware of row group boundary and will not cause overflow when reading rows
    // illustration of this case is here https://issues.apache.org/jira/browse/HIVE-6287
    if (rowIndexStride != 0 &&
        (includedRowGroups != null || startReadPhase != TypeReader.ReadPhase.ALL) &&
        rowInStripe < rowCountInStripe) {
      int startRowGroup = (int) (rowInStripe / rowIndexStride);
      if (includedRowGroups != null && !includedRowGroups[startRowGroup]) {
        while (startRowGroup < includedRowGroups.length && !includedRowGroups[startRowGroup]) {
          startRowGroup += 1;
        }
      }

      int endRowGroup = startRowGroup;
      // We force row group boundaries when dealing with filters. We adjust the end row group to
      // be the next row group even if more than one are possible selections.
      if (includedRowGroups != null && startReadPhase == TypeReader.ReadPhase.ALL) {
        while (endRowGroup < includedRowGroups.length && includedRowGroups[endRowGroup]) {
          endRowGroup += 1;
        }
      } else {
        endRowGroup += 1;
      }

      final long markerPosition = Math.min((endRowGroup * rowIndexStride), rowCountInStripe);
      batchSize = (int) Math.min(targetBatchSize, (markerPosition - rowInStripe));

      if (isLogDebugEnabled && batchSize < targetBatchSize) {
        LOG.debug("markerPosition: " + markerPosition + " batchSize: " + batchSize);
      }
    } else {
      batchSize = (int) Math.min(targetBatchSize, (rowCountInStripe - rowInStripe));
    }
    return batchSize;
  }

  @Override
  public void close() throws IOException {
    clearStreams();
    dataReader.close();
  }

  @Override
  public long getRowNumber() {
    return rowInStripe + rowBaseInStripe + firstRow;
  }

  /**
   * Return the fraction of rows that have been read from the selected.
   * section of the file
   *
   * @return fraction between 0.0 and 1.0 of rows consumed
   */
  @Override
  public float getProgress() {
    return ((float) rowBaseInStripe + rowInStripe) / totalRowCount;
  }

  private int findStripe(long rowNumber) {
    for (int i = 0; i < stripes.size(); i++) {
      StripeInformation stripe = stripes.get(i);
      if (stripe.getNumberOfRows() > rowNumber) {
        return i;
      }
      rowNumber -= stripe.getNumberOfRows();
    }
    throw new IllegalArgumentException("Seek after the end of reader range");
  }

  private void readCurrentStripeRowIndex() throws IOException {
    planner.readRowIndex(rowIndexColsToRead, indexes);
  }

  public OrcIndex readRowIndex(int stripeIndex,
                               boolean[] included,
                               boolean[] readCols) throws IOException {
    // Use the cached objects if the read request matches the cached request
    if (stripeIndex == currentStripe &&
        (readCols == null || Arrays.equals(readCols, rowIndexColsToRead))) {
      if (rowIndexColsToRead != null) {
        return indexes;
      } else {
        return planner.readRowIndex(readCols, indexes);
      }
    } else {
      StripePlanner copy = new StripePlanner(planner);
      if (included == null) {
        included = new boolean[schema.getMaximumId() + 1];
        Arrays.fill(included, true);
      }
      copy.parseStripe(stripes.get(stripeIndex), included);
      return copy.readRowIndex(readCols, null);
    }
  }

  private void seekToRowEntry(BatchReader reader, int rowEntry, TypeReader.ReadPhase readPhase)
      throws IOException {
    OrcProto.RowIndex[] rowIndices = indexes.getRowGroupIndex();
    PositionProvider[] index = new PositionProvider[rowIndices.length];
    for (int i = 0; i < index.length; ++i) {
      if (rowIndices[i] != null) {
        OrcProto.RowIndexEntry entry = rowIndices[i].getEntry(rowEntry);
        // This is effectively a test for pre-ORC-569 files.
        if (rowEntry == 0 && entry.getPositionsCount() == 0) {
          index[i] = new ZeroPositionProvider();
        } else {
          index[i] = new PositionProviderImpl(entry);
        }
      }
    }
    reader.seek(index, readPhase);
  }

  @Override
  public void seekToRow(long rowNumber) throws IOException {
    if (rowNumber < 0) {
      throw new IllegalArgumentException("Seek to a negative row number " +
          rowNumber);
    } else if (rowNumber < firstRow) {
      throw new IllegalArgumentException("Seek before reader range " +
          rowNumber);
    }
    // convert to our internal form (rows from the beginning of slice)
    rowNumber -= firstRow;

    // move to the right stripe
    int rightStripe = findStripe(rowNumber);
    if (rightStripe != currentStripe) {
      currentStripe = rightStripe;
      readStripe();
    }
    if (rowIndexColsToRead == null) {
      // Read the row indexes only if they were not already read as part of readStripe()
      readCurrentStripeRowIndex();
    }

    // if we aren't to the right row yet, advance in the stripe.
    advanceToNextRow(reader, rowNumber, true);
  }

  private static final String TRANSLATED_SARG_SEPARATOR = "_";
  public static String encodeTranslatedSargColumn(int rootColumn, Integer indexInSourceTable) {
    return rootColumn + TRANSLATED_SARG_SEPARATOR
        + ((indexInSourceTable == null) ? -1 : indexInSourceTable);
  }

  public static int[] mapTranslatedSargColumns(
      List<OrcProto.Type> types, List<PredicateLeaf> sargLeaves) {
    int[] result = new int[sargLeaves.size()];
    OrcProto.Type lastRoot = null; // Root will be the same for everyone as of now.
    String lastRootStr = null;
    for (int i = 0; i < result.length; ++i) {
      String[] rootAndIndex = sargLeaves.get(i).getColumnName().split(TRANSLATED_SARG_SEPARATOR);
      assert rootAndIndex.length == 2;
      String rootStr = rootAndIndex[0], indexStr = rootAndIndex[1];
      int index = Integer.parseInt(indexStr);
      // First, check if the column even maps to anything.
      if (index == -1) {
        result[i] = -1;
        continue;
      }
      assert index >= 0;
      // Then, find the root type if needed.
      if (!rootStr.equals(lastRootStr)) {
        lastRoot = types.get(Integer.parseInt(rootStr));
        lastRootStr = rootStr;
      }
      // Subtypes of the root types correspond, in order, to the columns in the table schema
      // (disregarding schema evolution that doesn't presently work). Get the index for the
      // corresponding subtype.
      result[i] = lastRoot.getSubtypes(index);
    }
    return result;
  }

  public CompressionCodec getCompressionCodec() {
    return dataReader.getCompressionOptions().getCodec();
  }

  public int getMaxDiskRangeChunkLimit() {
    return maxDiskRangeChunkLimit;
  }
}
