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

package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * The interface for reading ORC files.
 * <p>
 * One Reader can support multiple concurrent RecordReader.
 * @since 1.1.0
 */
public interface Reader extends Closeable {

  /**
   * Get the number of rows in the file.
   * @return the number of rows
   * @since 1.1.0
   */
  long getNumberOfRows();

  /**
   * Get the deserialized data size of the file
   * @return raw data size
   * @since 1.1.0
   */
  long getRawDataSize();

  /**
   * Get the deserialized data size of the specified columns
   * @param colNames the list of column names
   * @return raw data size of columns
   * @since 1.1.0
   */
  long getRawDataSizeOfColumns(List<String> colNames);

  /**
   * Get the deserialized data size of the specified columns ids
   * @param colIds - internal column id (check orcfiledump for column ids)
   * @return raw data size of columns
   * @since 1.1.0
   */
  long getRawDataSizeFromColIndices(List<Integer> colIds);

  /**
   * Get the user metadata keys.
   * @return the set of metadata keys
   * @since 1.1.0
   */
  List<String> getMetadataKeys();

  /**
   * Get a user metadata value.
   * @param key a key given by the user
   * @return the bytes associated with the given key
   * @since 1.1.0
   */
  ByteBuffer getMetadataValue(String key);

  /**
   * Did the user set the given metadata value.
   * @param key the key to check
   * @return true if the metadata value was set
   * @since 1.1.0
   */
  boolean hasMetadataValue(String key);

  /**
   * Get the compression kind.
   * @return the kind of compression in the file
   * @since 1.1.0
   */
  CompressionKind getCompressionKind();

  /**
   * Get the buffer size for the compression.
   * @return number of bytes to buffer for the compression codec.
   * @since 1.1.0
   */
  int getCompressionSize();

  /**
   * Get the number of rows per a entry in the row index.
   * @return the number of rows per an entry in the row index or 0 if there
   * is no row index.
   * @since 1.1.0
   */
  int getRowIndexStride();

  /**
   * Get the list of stripes.
   * @return the information about the stripes in order
   * @since 1.1.0
   */
  List<StripeInformation> getStripes();

  /**
   * Get the length of the file.
   * @return the number of bytes in the file
   * @since 1.1.0
   */
  long getContentLength();

  /**
   * Get the statistics about the columns in the file.
   * @return the information about the column
   * @since 1.1.0
   */
  ColumnStatistics[] getStatistics();

  /**
   * Get the type of rows in this ORC file.
   * @since 1.1.0
   */
  TypeDescription getSchema();

  /**
   * Get the list of types contained in the file. The root type is the first
   * type in the list.
   * @return the list of flattened types
   * @deprecated use getSchema instead
   * @since 1.1.0
   */
  List<OrcProto.Type> getTypes();

  /**
   * Get the file format version.
   * @since 1.1.0
   */
  OrcFile.Version getFileVersion();

  /**
   * Get the version of the writer of this file.
   * @since 1.1.0
   */
  OrcFile.WriterVersion getWriterVersion();

  /**
   * Get the implementation and version of the software that wrote the file.
   * It defaults to "ORC Java" for old files. For current files, we include the
   * version also.
   * @since 1.5.13
   * @return returns the writer implementation and hopefully the version of the
   *   software
   */
  String getSoftwareVersion();

  /**
   * Get the file tail (footer + postscript)
   *
   * @return - file tail
   * @since 1.1.0
   */
  OrcProto.FileTail getFileTail();

  /**
   * Get the list of encryption keys for column encryption.
   * @return the set of encryption keys
   * @since 1.6.0
   */
  EncryptionKey[] getColumnEncryptionKeys();

  /**
   * Get the data masks for the unencrypted variant of the data.
   * @return the lists of data masks
   * @since 1.6.0
   */
  DataMaskDescription[] getDataMasks();

  /**
   * Get the list of encryption variants for the data.
   * @since 1.6.0
   */
  EncryptionVariant[] getEncryptionVariants();

  /**
   * Get the stripe statistics for a given variant. The StripeStatistics will
   * have 1 entry for each column in the variant. This enables the user to
   * get the stripe statistics for each variant regardless of which keys are
   * available.
   * @param variant the encryption variant or null for unencrypted
   * @return a list of stripe statistics (one per a stripe)
   * @throws IOException if the required key is not available
   * @since 1.6.0
   */
  List<StripeStatistics> getVariantStripeStatistics(EncryptionVariant variant
                                                    ) throws IOException;

  /**
   * Options for creating a RecordReader.
   * @since 1.1.0
   */
  class Options implements Cloneable {
    private boolean[] include;
    private long offset = 0;
    private long length = Long.MAX_VALUE;
    private int positionalEvolutionLevel;
    private SearchArgument sarg = null;
    private String[] columnNames = null;
    private Boolean useZeroCopy = null;
    private Boolean skipCorruptRecords = null;
    private TypeDescription schema = null;
    private String[] preFilterColumns = null;
    Consumer<OrcFilterContext> skipRowCallback = null;
    private DataReader dataReader = null;
    private Boolean tolerateMissingSchema = null;
    private boolean forcePositionalEvolution;
    private boolean isSchemaEvolutionCaseAware =
        (boolean) OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.getDefaultValue();
    private boolean includeAcidColumns = true;
    private boolean allowSARGToFilter = false;
    private boolean useSelected = false;
    private boolean allowPluginFilters = false;
    private List<String> pluginAllowListFilters = null;
    private int minSeekSize = (int) OrcConf.ORC_MIN_DISK_SEEK_SIZE.getDefaultValue();
    private double minSeekSizeTolerance = (double) OrcConf.ORC_MIN_DISK_SEEK_SIZE_TOLERANCE
        .getDefaultValue();
    private int rowBatchSize = (int) OrcConf.ROW_BATCH_SIZE.getDefaultValue();

    /**
     * @since 1.1.0
     */
    public Options() {
      // PASS
    }

    /**
     * @since 1.1.0
     */
    public Options(Configuration conf) {
      useZeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(conf);
      skipCorruptRecords = OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf);
      tolerateMissingSchema = OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf);
      forcePositionalEvolution = OrcConf.FORCE_POSITIONAL_EVOLUTION.getBoolean(conf);
      positionalEvolutionLevel = OrcConf.FORCE_POSITIONAL_EVOLUTION_LEVEL.getInt(conf);
      isSchemaEvolutionCaseAware =
          OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.getBoolean(conf);
      allowSARGToFilter = OrcConf.ALLOW_SARG_TO_FILTER.getBoolean(conf);
      useSelected = OrcConf.READER_USE_SELECTED.getBoolean(conf);
      allowPluginFilters = OrcConf.ALLOW_PLUGIN_FILTER.getBoolean(conf);
      pluginAllowListFilters = OrcConf.PLUGIN_FILTER_ALLOWLIST.getStringAsList(conf);
      minSeekSize = OrcConf.ORC_MIN_DISK_SEEK_SIZE.getInt(conf);
      minSeekSizeTolerance = OrcConf.ORC_MIN_DISK_SEEK_SIZE_TOLERANCE.getDouble(conf);
      rowBatchSize = OrcConf.ROW_BATCH_SIZE.getInt(conf);
    }

    /**
     * Set the list of columns to read.
     * @param include a list of columns to read
     * @return this
     * @since 1.1.0
     */
    public Options include(boolean[] include) {
      this.include = include;
      return this;
    }

    /**
     * Set the range of bytes to read
     * @param offset the starting byte offset
     * @param length the number of bytes to read
     * @return this
     * @since 1.1.0
     */
    public Options range(long offset, long length) {
      this.offset = offset;
      this.length = length;
      return this;
    }

    /**
     * Set the schema on read type description.
     * @since 1.1.0
     */
    public Options schema(TypeDescription schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Set a row level filter.
     * This is an advanced feature that allows the caller to specify
     * a list of columns that are read first and then a filter that
     * is called to determine which rows if any should be read.
     *
     * User should expect the batches that come from the reader
     * to use the selected array set by their filter.
     *
     * Use cases for this are predicates that SearchArgs can't represent,
     * such as relationships between columns (eg. columnA == columnB).
     * @param filterColumnNames a comma separated list of the column names that
     *                      are read before the filter is applied. Only top
     *                      level columns in the reader's schema can be used
     *                      here and they must not be duplicated.
     * @param filterCallback a function callback to perform filtering during the call to
     *              RecordReader.nextBatch. This function should not reference
     *               any static fields nor modify the passed in ColumnVectors but
     *               should set the filter output using the selected array.
     *
     * @return this
     * @since 1.7.0
     */
    public Options setRowFilter(
        String[] filterColumnNames, Consumer<OrcFilterContext> filterCallback) {
      this.preFilterColumns = filterColumnNames;
      this.skipRowCallback =  filterCallback;
      return this;
    }

    /**
     * Set search argument for predicate push down.
     * @param sarg the search argument
     * @param columnNames the column names for
     * @return this
     * @since 1.1.0
     */
    public Options searchArgument(SearchArgument sarg, String[] columnNames) {
      this.sarg = sarg;
      this.columnNames = columnNames;
      return this;
    }

    /**
     * Set allowSARGToFilter.
     * @param allowSARGToFilter
     * @return this
     * @since 1.7.0
     */
    public Options allowSARGToFilter(boolean allowSARGToFilter) {
      this.allowSARGToFilter = allowSARGToFilter;
      return this;
    }

    /**
     * Get allowSARGToFilter value.
     * @return allowSARGToFilter
     * @since 1.7.0
     */
    public boolean isAllowSARGToFilter() {
      return allowSARGToFilter;
    }

    /**
     * Set whether to use zero copy from HDFS.
     * @param value the new zero copy flag
     * @return this
     * @since 1.1.0
     */
    public Options useZeroCopy(boolean value) {
      this.useZeroCopy = value;
      return this;
    }

    /**
     * Set dataReader.
     * @param value the new dataReader.
     * @return this
     * @since 1.1.0
     */
    public Options dataReader(DataReader value) {
      this.dataReader = value;
      return this;
    }

    /**
     * Set whether to skip corrupt records.
     * @param value the new skip corrupt records flag
     * @return this
     * @since 1.1.0
     */
    public Options skipCorruptRecords(boolean value) {
      this.skipCorruptRecords = value;
      return this;
    }

    /**
     * Set whether to make a best effort to tolerate schema evolution for files
     * which do not have an embedded schema because they were written with a'
     * pre-HIVE-4243 writer.
     * @param value the new tolerance flag
     * @return this
     * @since 1.2.0
     */
    public Options tolerateMissingSchema(boolean value) {
      this.tolerateMissingSchema = value;
      return this;
    }

    /**
     * Set whether to force schema evolution to be positional instead of
     * based on the column names.
     * @param value force positional evolution
     * @return this
     * @since 1.3.0
     */
    public Options forcePositionalEvolution(boolean value) {
      this.forcePositionalEvolution = value;
      return this;
    }

    /**
     * Set number of levels to force schema evolution to be positional instead of
     * based on the column names.
     * @param value number of levels of positional schema evolution
     * @return this
     * @since 1.5.11
     */
    public Options positionalEvolutionLevel(int value) {
      this.positionalEvolutionLevel = value;
      return this;
    }


    /**
     * Set boolean flag to determine if the comparison of field names in schema
     * evolution is case sensitive
     * @param value the flag for schema evolution is case sensitive or not.
     * @return this
     * @since 1.5.0
     */
    public Options isSchemaEvolutionCaseAware(boolean value) {
      this.isSchemaEvolutionCaseAware = value;
      return this;
    }

    /**
     * {@code true} if acid metadata columns should be decoded otherwise they will
     * be set to {@code null}.
     * @since 1.5.3
     */
    public Options includeAcidColumns(boolean includeAcidColumns) {
      this.includeAcidColumns = includeAcidColumns;
      return this;
    }

    /**
     * @since 1.1.0
     */
    public boolean[] getInclude() {
      return include;
    }

    /**
     * @since 1.1.0
     */
    public long getOffset() {
      return offset;
    }

    /**
     * @since 1.1.0
     */
    public long getLength() {
      return length;
    }

    /**
     * @since 1.1.0
     */
    public TypeDescription getSchema() {
      return schema;
    }

    /**
     * @since 1.1.0
     */
    public SearchArgument getSearchArgument() {
      return sarg;
    }

    /**
     * @since 1.7.0
     */
    public Consumer<OrcFilterContext> getFilterCallback() {
      return skipRowCallback;
    }

    /**
     * @since 1.7.0
     */
    public String[] getPreFilterColumnNames(){
      return preFilterColumns;
    }

    /**
     * @since 1.1.0
     */
    public String[] getColumnNames() {
      return columnNames;
    }

    /**
     * @since 1.1.0
     */
    public long getMaxOffset() {
      long result = offset + length;
      if (result < 0) {
        result = Long.MAX_VALUE;
      }
      return result;
    }

    /**
     * @since 1.1.0
     */
    public Boolean getUseZeroCopy() {
      return useZeroCopy;
    }

    /**
     * @since 1.1.0
     */
    public Boolean getSkipCorruptRecords() {
      return skipCorruptRecords;
    }

    /**
     * @since 1.1.0
     */
    public DataReader getDataReader() {
      return dataReader;
    }

    /**
     * @since 1.3.0
     */
    public boolean getForcePositionalEvolution() {
      return forcePositionalEvolution;
    }

    /**
     * @since 1.5.11
     */
    public int getPositionalEvolutionLevel() {
      return positionalEvolutionLevel;
    }

    /**
     * @since 1.5.0
     */
    public boolean getIsSchemaEvolutionCaseAware() {
      return isSchemaEvolutionCaseAware;
    }

    /**
     * @since 1.5.3
     */
    public boolean getIncludeAcidColumns() {
      return includeAcidColumns;
    }

    /**
     * @since 1.1.0
     */
    @Override
    public Options clone() {
      try {
        Options result = (Options) super.clone();
        if (dataReader != null) {
          result.dataReader = dataReader.clone();
        }
        return result;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    /**
     * @since 1.1.0
     */
    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("{include: ");
      if (include == null) {
        buffer.append("null");
      } else {
        buffer.append("[");
        for(int i=0; i < include.length; ++i) {
          if (i != 0) {
            buffer.append(", ");
          }
          buffer.append(include[i]);
        }
        buffer.append("]");
      }
      buffer.append(", offset: ");
      buffer.append(offset);
      buffer.append(", length: ");
      buffer.append(length);
      if (sarg != null) {
        buffer.append(", sarg: ");
        buffer.append(sarg);
      }
      if (schema != null) {
        buffer.append(", schema: ");
        schema.printToBuffer(buffer);
      }
      buffer.append(", includeAcidColumns: ").append(includeAcidColumns);
      buffer.append(", allowSARGToFilter: ").append(allowSARGToFilter);
      buffer.append(", useSelected: ").append(useSelected);
      buffer.append("}");
      return buffer.toString();
    }

    /**
     * @since 1.2.0
     */
    public boolean getTolerateMissingSchema() {
      return tolerateMissingSchema != null ? tolerateMissingSchema :
          (Boolean) OrcConf.TOLERATE_MISSING_SCHEMA.getDefaultValue();
    }

    /**
     * @since 1.7.0
     */
    public boolean useSelected() {
      return useSelected;
    }

    /**
     * @since 1.7.0
     */
    public Options useSelected(boolean newValue) {
      this.useSelected = newValue;
      return this;
    }

    public boolean allowPluginFilters() {
      return allowPluginFilters;
    }

    public Options allowPluginFilters(boolean allowPluginFilters) {
      this.allowPluginFilters = allowPluginFilters;
      return this;
    }

    public List<String> pluginAllowListFilters() {
      return pluginAllowListFilters;
    }

    public Options pluginAllowListFilters(String... allowLists) {
      this.pluginAllowListFilters = Arrays.asList(allowLists);
      return this;
    }

    /**
     * @since 1.8.0
     */
    public int minSeekSize() {
      return minSeekSize;
    }

    /**
     * @since 1.8.0
     */
    public Options minSeekSize(int minSeekSize) {
      this.minSeekSize = minSeekSize;
      return this;
    }

    /**
     * @since 1.8.0
     */
    public double minSeekSizeTolerance() {
      return minSeekSizeTolerance;
    }

    /**
     * @since 1.8.0
     */
    public Options minSeekSizeTolerance(double value) {
      this.minSeekSizeTolerance = value;
      return this;
    }

    /**
     * @since 1.9.0
     */
    public int getRowBatchSize() {
      return rowBatchSize;
    }

    /**
     * @since 1.9.0
     */
    public Options rowBatchSize(int value) {
      this.rowBatchSize = value;
      return this;
    }
  }

  /**
   * Create a default options object that can be customized for creating
   * a RecordReader.
   * @return a new default Options object
   * @since 1.2.0
   */
  Options options();

  /**
   * Create a RecordReader that reads everything with the default options.
   * @return a new RecordReader
   * @since 1.1.0
   */
  RecordReader rows() throws IOException;

  /**
   * Create a RecordReader that uses the options given.
   * This method can't be named rows, because many callers used rows(null)
   * before the rows() method was introduced.
   * @param options the options to read with
   * @return a new RecordReader
   * @since 1.1.0
   */
  RecordReader rows(Options options) throws IOException;

  /**
   * @return List of integers representing version of the file, in order from major to minor.
   * @since 1.1.0
   */
  List<Integer> getVersionList();

  /**
   * @return Gets the size of metadata, in bytes.
   * @since 1.1.0
   */
  int getMetadataSize();

  /**
   * @return Stripe statistics, in original protobuf form.
   * @deprecated Use {@link #getStripeStatistics()} instead.
   * @since 1.1.0
   */
  List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics();

  /**
   * Get the stripe statistics for all of the columns.
   * @return a list of the statistics for each stripe in the file
   * @since 1.2.0
   */
  List<StripeStatistics> getStripeStatistics() throws IOException;

  /**
   * Get the stripe statistics from the file.
   * @param include null for all columns or an array where the required columns
   *                are selected
   * @return a list of the statistics for each stripe in the file
   * @since 1.6.0
   */
  List<StripeStatistics> getStripeStatistics(boolean[] include) throws IOException;

  /**
   * @return File statistics, in original protobuf form.
   * @deprecated Use {@link #getStatistics()} instead.
   * @since 1.1.0
   */
  List<OrcProto.ColumnStatistics> getOrcProtoFileStatistics();

  /**
   * @return Serialized file metadata read from disk for the purposes of caching, etc.
   * @since 1.1.0
   */
  ByteBuffer getSerializedFileFooter();

  /**
   * Was the file written using the proleptic Gregorian calendar.
   * @since 1.5.9
   */
  boolean writerUsedProlepticGregorian();

  /**
   * Should the returned values use the proleptic Gregorian calendar?
   * @since 1.5.9
   */
  boolean getConvertToProlepticGregorian();
}
