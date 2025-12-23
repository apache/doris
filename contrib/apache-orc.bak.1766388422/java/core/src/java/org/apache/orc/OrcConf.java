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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Define the configuration properties that Orc understands.
 */
public enum OrcConf {
  STRIPE_SIZE("orc.stripe.size", "hive.exec.orc.default.stripe.size",
      64L * 1024 * 1024,
      "Define the default ORC stripe size, in bytes."),
  STRIPE_ROW_COUNT("orc.stripe.row.count", "orc.stripe.row.count",
      Integer.MAX_VALUE, "This value limit the row count in one stripe. \n" +
      "The number of stripe rows can be controlled at \n" +
      "(0, \"orc.stripe.row.count\" + max(batchSize, \"orc.rows.between.memory.checks\"))"),
  BLOCK_SIZE("orc.block.size", "hive.exec.orc.default.block.size",
      256L * 1024 * 1024,
      "Define the default file system block size for ORC files."),
  ENABLE_INDEXES("orc.create.index", "orc.create.index", true,
      "Should the ORC writer create indexes as part of the file."),
  ROW_INDEX_STRIDE("orc.row.index.stride",
      "hive.exec.orc.default.row.index.stride", 10000,
      "Define the default ORC index stride in number of rows. (Stride is the\n"+
          " number of rows an index entry represents.)"),
  BUFFER_SIZE("orc.compress.size", "hive.exec.orc.default.buffer.size",
      256 * 1024, "Define the default ORC buffer size, in bytes."),
  BASE_DELTA_RATIO("orc.base.delta.ratio", "hive.exec.orc.base.delta.ratio", 8,
      "The ratio of base writer and delta writer in terms of STRIPE_SIZE and BUFFER_SIZE."),
  BLOCK_PADDING("orc.block.padding", "hive.exec.orc.default.block.padding",
      true,
      "Define whether stripes should be padded to the HDFS block boundaries."),
  COMPRESS("orc.compress", "hive.exec.orc.default.compress", "ZLIB",
      "Define the default compression codec for ORC file"),
  WRITE_FORMAT("orc.write.format", "hive.exec.orc.write.format", "0.12",
      "Define the version of the file to write. Possible values are 0.11 and\n"+
          " 0.12. If this parameter is not defined, ORC will use the run\n" +
          " length encoding (RLE) introduced in Hive 0.12."),
  ENFORCE_COMPRESSION_BUFFER_SIZE("orc.buffer.size.enforce",
      "hive.exec.orc.buffer.size.enforce", false,
      "Defines whether to enforce ORC compression buffer size."),
  ENCODING_STRATEGY("orc.encoding.strategy", "hive.exec.orc.encoding.strategy",
      "SPEED",
      "Define the encoding strategy to use while writing data. Changing this\n"+
          "will only affect the light weight encoding for integers. This\n" +
          "flag will not change the compression level of higher level\n" +
          "compression codec (like ZLIB)."),
  COMPRESSION_STRATEGY("orc.compression.strategy",
      "hive.exec.orc.compression.strategy", "SPEED",
      "Define the compression strategy to use while writing data.\n" +
          "This changes the compression level of higher level compression\n" +
          "codec (like ZLIB)."),
  BLOCK_PADDING_TOLERANCE("orc.block.padding.tolerance",
      "hive.exec.orc.block.padding.tolerance", 0.05,
      "Define the tolerance for block padding as a decimal fraction of\n" +
          "stripe size (for example, the default value 0.05 is 5% of the\n" +
          "stripe size). For the defaults of 64Mb ORC stripe and 256Mb HDFS\n" +
          "blocks, the default block padding tolerance of 5% will\n" +
          "reserve a maximum of 3.2Mb for padding within the 256Mb block.\n" +
          "In that case, if the available size within the block is more than\n"+
          "3.2Mb, a new smaller stripe will be inserted to fit within that\n" +
          "space. This will make sure that no stripe written will block\n" +
          " boundaries and cause remote reads within a node local task."),
  BLOOM_FILTER_FPP("orc.bloom.filter.fpp", "orc.default.bloom.fpp", 0.01,
      "Define the default false positive probability for bloom filters."),
  USE_ZEROCOPY("orc.use.zerocopy", "hive.exec.orc.zerocopy", false,
      "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),
  SKIP_CORRUPT_DATA("orc.skip.corrupt.data", "hive.exec.orc.skip.corrupt.data",
      false,
      "If ORC reader encounters corrupt data, this value will be used to\n" +
          "determine whether to skip the corrupt data or throw exception.\n" +
          "The default behavior is to throw exception."),
  TOLERATE_MISSING_SCHEMA("orc.tolerate.missing.schema",
      "hive.exec.orc.tolerate.missing.schema",
      true,
      "Writers earlier than HIVE-4243 may have inaccurate schema metadata.\n"
          + "This setting will enable best effort schema evolution rather\n"
          + "than rejecting mismatched schemas"),
  MEMORY_POOL("orc.memory.pool", "hive.exec.orc.memory.pool", 0.5,
      "Maximum fraction of heap that can be used by ORC file writers"),
  DICTIONARY_KEY_SIZE_THRESHOLD("orc.dictionary.key.threshold",
      "hive.exec.orc.dictionary.key.size.threshold",
      0.8,
      "If the number of distinct keys in a dictionary is greater than this\n" +
          "fraction of the total number of non-null rows, turn off \n" +
          "dictionary encoding.  Use 1 to always use dictionary encoding."),
  ROW_INDEX_STRIDE_DICTIONARY_CHECK("orc.dictionary.early.check",
      "hive.orc.row.index.stride.dictionary.check",
      true,
      "If enabled dictionary check will happen after first row index stride\n" +
          "(default 10000 rows) else dictionary check will happen before\n" +
          "writing first stripe. In both cases, the decision to use\n" +
          "dictionary or not will be retained thereafter."),
  DICTIONARY_IMPL("orc.dictionary.implementation", "orc.dictionary.implementation",
      "rbtree",
      "the implementation for the dictionary used for string-type column encoding.\n" +
          "The choices are:\n"
          + " rbtree - use red-black tree as the implementation for the dictionary.\n"
          + " hash - use hash table as the implementation for the dictionary."),
  BLOOM_FILTER_COLUMNS("orc.bloom.filter.columns", "orc.bloom.filter.columns",
      "", "List of columns to create bloom filters for when writing."),
  BLOOM_FILTER_WRITE_VERSION("orc.bloom.filter.write.version",
      "orc.bloom.filter.write.version", OrcFile.BloomFilterVersion.UTF8.toString(),
      "Which version of the bloom filters should we write.\n" +
          "The choices are:\n" +
          "  original - writes two versions of the bloom filters for use by\n" +
          "             both old and new readers.\n" +
          "  utf8 - writes just the new bloom filters."),
  IGNORE_NON_UTF8_BLOOM_FILTERS("orc.bloom.filter.ignore.non-utf8",
      "orc.bloom.filter.ignore.non-utf8", false,
      "Should the reader ignore the obsolete non-UTF8 bloom filters."),
  MAX_FILE_LENGTH("orc.max.file.length", "orc.max.file.length", Long.MAX_VALUE,
      "The maximum size of the file to read for finding the file tail. This\n" +
          "is primarily used for streaming ingest to read intermediate\n" +
          "footers while the file is still open"),
  MAPRED_INPUT_SCHEMA("orc.mapred.input.schema", null, null,
      "The schema that the user desires to read. The values are\n" +
      "interpreted using TypeDescription.fromString."),
  MAPRED_SHUFFLE_KEY_SCHEMA("orc.mapred.map.output.key.schema", null, null,
      "The schema of the MapReduce shuffle key. The values are\n" +
          "interpreted using TypeDescription.fromString."),
  MAPRED_SHUFFLE_VALUE_SCHEMA("orc.mapred.map.output.value.schema", null, null,
      "The schema of the MapReduce shuffle value. The values are\n" +
          "interpreted using TypeDescription.fromString."),
  MAPRED_OUTPUT_SCHEMA("orc.mapred.output.schema", null, null,
      "The schema that the user desires to write. The values are\n" +
          "interpreted using TypeDescription.fromString."),
  INCLUDE_COLUMNS("orc.include.columns", "hive.io.file.readcolumn.ids", null,
      "The list of comma separated column ids that should be read with 0\n" +
          "being the first column, 1 being the next, and so on. ."),
  KRYO_SARG("orc.kryo.sarg", "orc.kryo.sarg", null,
      "The kryo and base64 encoded SearchArgument for predicate pushdown."),
  KRYO_SARG_BUFFER("orc.kryo.sarg.buffer", null, 8192,
      "The kryo buffer size for SearchArgument for predicate pushdown."),
  SARG_COLUMNS("orc.sarg.column.names", "orc.sarg.column.names", null,
      "The list of column names for the SearchArgument."),
  FORCE_POSITIONAL_EVOLUTION("orc.force.positional.evolution",
      "orc.force.positional.evolution", false,
      "Require schema evolution to match the top level columns using position\n" +
      "rather than column names. This provides backwards compatibility with\n" +
      "Hive 2.1."),
  FORCE_POSITIONAL_EVOLUTION_LEVEL("orc.force.positional.evolution.level",
      "orc.force.positional.evolution.level", 1,
      "Require schema evolution to match the the defined no. of level columns using position\n" +
          "rather than column names. This provides backwards compatibility with Hive 2.1."),
  ROWS_BETWEEN_CHECKS("orc.rows.between.memory.checks", "orc.rows.between.memory.checks", 5000,
    "How often should MemoryManager check the memory sizes? Measured in rows\n" +
      "added to all of the writers.  Valid range is [1,10000] and is primarily meant for" +
      "testing.  Setting this too low may negatively affect performance."
        + " Use orc.stripe.row.count instead if the value larger than orc.stripe.row.count."),
  OVERWRITE_OUTPUT_FILE("orc.overwrite.output.file", "orc.overwrite.output.file", false,
    "A boolean flag to enable overwriting of the output file if it already exists.\n"),
  IS_SCHEMA_EVOLUTION_CASE_SENSITIVE("orc.schema.evolution.case.sensitive",
      "orc.schema.evolution.case.sensitive", true,
      "A boolean flag to determine if the comparision of field names " +
      "in schema evolution is case sensitive .\n"),
  ALLOW_SARG_TO_FILTER("orc.sarg.to.filter", "orc.sarg.to.filter", false,
                       "A boolean flag to determine if a SArg is allowed to become a filter"),
  READER_USE_SELECTED("orc.filter.use.selected", "orc.filter.use.selected", false,
                        "A boolean flag to determine if the selected vector is supported by\n"
                        + "the reading application. If false, the output of the ORC reader "
                        + "must have the filter\n"
                        + "reapplied to avoid using unset values in the unselected rows.\n"
                        + "If unsure please leave this as false."),
  ALLOW_PLUGIN_FILTER("orc.filter.plugin",
                      "orc.filter.plugin",
                      false,
                      "Enables the use of plugin filters during read. The plugin filters "
                      + "are discovered against the service "
                      + "org.apache.orc.filter.PluginFilterService, if multiple filters are "
                      + "determined, they are combined using AND. The order of application is "
                      + "non-deterministic and the filter functionality should not depend on the "
                      + "order of application."),

  PLUGIN_FILTER_ALLOWLIST("orc.filter.plugin.allowlist",
                          "orc.filter.plugin.allowlist",
                          "*",
                          "A list of comma-separated class names. If specified it restricts "
                          + "the PluginFilters to just these classes as discovered by the "
                          + "PluginFilterService. The default of * allows all discovered classes "
                          + "and an empty string would not allow any plugins to be applied."),

  WRITE_VARIABLE_LENGTH_BLOCKS("orc.write.variable.length.blocks", null, false,
      "A boolean flag as to whether the ORC writer should write variable length\n"
      + "HDFS blocks."),
  DIRECT_ENCODING_COLUMNS("orc.column.encoding.direct", "orc.column.encoding.direct", "",
      "Comma-separated list of columns for which dictionary encoding is to be skipped."),
  // some JVM doesn't allow array creation of size Integer.MAX_VALUE, so chunk size is slightly less than max int
  ORC_MAX_DISK_RANGE_CHUNK_LIMIT("orc.max.disk.range.chunk.limit",
      "hive.exec.orc.max.disk.range.chunk.limit",
    Integer.MAX_VALUE - 1024, "When reading stripes >2GB, specify max limit for the chunk size."),
  ORC_MIN_DISK_SEEK_SIZE("orc.min.disk.seek.size",
                         "orc.min.disk.seek.size",
                         0,
                         "When determining contiguous reads, gaps within this size are "
                         + "read contiguously and not seeked. Default value of zero disables this "
                         + "optimization"),
  ORC_MIN_DISK_SEEK_SIZE_TOLERANCE("orc.min.disk.seek.size.tolerance",
                          "orc.min.disk.seek.size.tolerance", 0.00,
                          "Define the tolerance for for extra bytes read as a result of "
                          + "orc.min.disk.seek.size. If the "
                          + "(bytesRead - bytesNeeded) / bytesNeeded is greater than this "
                          + "threshold then extra work is performed to drop the extra bytes from "
                          + "memory after the read."),
  ENCRYPTION("orc.encrypt", "orc.encrypt", null, "The list of keys and columns to encrypt with"),
  DATA_MASK("orc.mask", "orc.mask", null, "The masks to apply to the encrypted columns"),
  KEY_PROVIDER("orc.key.provider", "orc.key.provider", "hadoop",
      "The kind of KeyProvider to use for encryption."),
  PROLEPTIC_GREGORIAN("orc.proleptic.gregorian", "orc.proleptic.gregorian", false,
      "Should we read and write dates & times using the proleptic Gregorian calendar\n" +
          "instead of the hybrid Julian Gregorian? Hive before 3.1 and Spark before 3.0\n" +
          "used hybrid."),
  PROLEPTIC_GREGORIAN_DEFAULT("orc.proleptic.gregorian.default",
      "orc.proleptic.gregorian.default", false,
      "This value controls whether pre-ORC 27 files are using the hybrid or proleptic\n" +
      "calendar. Only Hive 3.1 and the C++ library wrote using the proleptic, so hybrid\n" +
      "is the default."),
  ROW_BATCH_SIZE("orc.row.batch.size", "orc.row.batch.size", 1024,
      "The number of rows to include in a orc vectorized reader batch. " +
      "The value should be carefully chosen to minimize overhead and avoid OOMs in reading data."),
  ROW_BATCH_CHILD_LIMIT("orc.row.child.limit", "orc.row.child.limit",
      1024 * 32, "The maximum number of child elements to buffer before "+
      "the ORC row writer writes the batch to the file."
      )
  ;

  private final String attribute;
  private final String hiveConfName;
  private final Object defaultValue;
  private final String description;

  OrcConf(String attribute,
          String hiveConfName,
          Object defaultValue,
          String description) {
    this.attribute = attribute;
    this.hiveConfName = hiveConfName;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  public String getAttribute() {
    return attribute;
  }

  public String getHiveConfName() {
    return hiveConfName;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  private String lookupValue(Properties tbl, Configuration conf) {
    String result = null;
    if (tbl != null) {
      result = tbl.getProperty(attribute);
    }
    if (result == null && conf != null) {
      result = conf.get(attribute);
      if (result == null && hiveConfName != null) {
        result = conf.get(hiveConfName);
      }
    }
    return result;
  }

  public int getInt(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return ((Number) defaultValue).intValue();
  }

  public int getInt(Configuration conf) {
    return getInt(null, conf);
  }

  /**
   * @deprecated Use {@link #getInt(Configuration)} instead. This method was
   * incorrectly added and shouldn't be used anymore.
   */
  @Deprecated
  public void getInt(Configuration conf, int value) {
    // noop
  }

  public void setInt(Configuration conf, int value) {
    conf.setInt(attribute, value);
  }

  public long getLong(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Long.parseLong(value);
    }
    return ((Number) defaultValue).longValue();
  }

  public long getLong(Configuration conf) {
    return getLong(null, conf);
  }

  public void setLong(Configuration conf, long value) {
    conf.setLong(attribute, value);
  }

  public String getString(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    return value == null ? (String) defaultValue : value;
  }

  public String getString(Configuration conf) {
    return getString(null, conf);
  }

  public List<String> getStringAsList(Configuration conf) {
    String value = getString(null, conf);
    List<String> confList = new ArrayList<>();
    if (StringUtils.isEmpty(value)) {
      return confList;
    }
    for (String str: value.split(",")) {
      String trimStr = StringUtils.trim(str);
      if (StringUtils.isNotEmpty(trimStr)) {
        confList.add(trimStr);
      }
    }
    return confList;
  }

  public void setString(Configuration conf, String value) {
    conf.set(attribute, value);
  }

  public boolean getBoolean(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return (Boolean) defaultValue;
  }

  public boolean getBoolean(Configuration conf) {
    return getBoolean(null, conf);
  }

  public void setBoolean(Configuration conf, boolean value) {
    conf.setBoolean(attribute, value);
  }

  public double getDouble(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return ((Number) defaultValue).doubleValue();
  }

  public double getDouble(Configuration conf) {
    return getDouble(null, conf);
  }

  public void setDouble(Configuration conf, double value) {
    conf.setDouble(attribute, value);
  }
}
