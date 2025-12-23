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

import com.google.protobuf.ByteString;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataMask;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.orc.impl.writer.TreeWriter;
import org.apache.orc.impl.writer.WriterContext;
import org.apache.orc.impl.writer.WriterEncryptionKey;
import org.apache.orc.impl.writer.WriterEncryptionVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * An ORC file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 * <p>
 * This class is unsynchronized like most Stream objects, so from the creation
 * of an OrcFile and all access to a single instance has to be from a single
 * thread.
 * <p>
 * There are no known cases where these happen between different threads today.
 * <p>
 * Caveat: the MemoryManager is created during WriterOptions create, that has
 * to be confined to a single thread as well.
 *
 */
public class WriterImpl implements WriterInternal, MemoryManager.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(WriterImpl.class);

  private static final int MIN_ROW_INDEX_STRIDE = 1000;

  private final Path path;
  private final long stripeSize;
  private final long stripeRowCount;
  private final int rowIndexStride;
  private final TypeDescription schema;
  private final PhysicalWriter physicalWriter;
  private final OrcFile.WriterVersion writerVersion;
  private final StreamOptions unencryptedOptions;

  private long rowCount = 0;
  private long rowsInStripe = 0;
  private long rawDataSize = 0;
  private int rowsInIndex = 0;
  private long lastFlushOffset = 0;
  private int stripesAtLastFlush = -1;
  private final List<OrcProto.StripeInformation> stripes =
      new ArrayList<>();
  private final Map<String, ByteString> userMetadata =
      new TreeMap<>();
  private final TreeWriter treeWriter;
  private final boolean buildIndex;
  private final MemoryManager memoryManager;
  private long previousAllocation = -1;
  private long memoryLimit;
  private final long ROWS_PER_CHECK;
  private long rowsSinceCheck = 0;
  private final OrcFile.Version version;
  private final Configuration conf;
  private final OrcFile.WriterCallback callback;
  private final OrcFile.WriterContext callbackContext;
  private final OrcFile.EncodingStrategy encodingStrategy;
  private final OrcFile.CompressionStrategy compressionStrategy;
  private final boolean[] bloomFilterColumns;
  private final double bloomFilterFpp;
  private final OrcFile.BloomFilterVersion bloomFilterVersion;
  private final boolean writeTimeZone;
  private final boolean useUTCTimeZone;
  private final double dictionaryKeySizeThreshold;
  private final boolean[] directEncodingColumns;
  private final List<OrcProto.ColumnEncoding> unencryptedEncodings =
      new ArrayList<>();

  // the list of maskDescriptions, keys, and variants
  private SortedMap<String, MaskDescriptionImpl> maskDescriptions = new TreeMap<>();
  private SortedMap<String, WriterEncryptionKey> keys = new TreeMap<>();
  private final WriterEncryptionVariant[] encryption;
  // the mapping of columns to maskDescriptions
  private final MaskDescriptionImpl[] columnMaskDescriptions;
  // the mapping of columns to EncryptionVariants
  private final WriterEncryptionVariant[] columnEncryption;
  private KeyProvider keyProvider;
  // do we need to include the current encryption keys in the next stripe
  // information
  private boolean needKeyFlush;
  private final boolean useProlepticGregorian;
  private boolean isClose = false;

  public WriterImpl(FileSystem fs,
                    Path path,
                    OrcFile.WriterOptions opts) throws IOException {
    this.path = path;
    this.conf = opts.getConfiguration();
    // clone it so that we can annotate it with encryption
    this.schema = opts.getSchema().clone();
    int numColumns = schema.getMaximumId() + 1;
    if (!opts.isEnforceBufferSize()) {
      opts.bufferSize(getEstimatedBufferSize(opts.getStripeSize(), numColumns,
          opts.getBufferSize()));
    }

    // Annotate the schema with the column encryption
    schema.annotateEncryption(opts.getEncryption(), opts.getMasks());
    columnEncryption = new WriterEncryptionVariant[numColumns];
    columnMaskDescriptions = new MaskDescriptionImpl[numColumns];
    encryption = setupEncryption(opts.getKeyProvider(), schema,
        opts.getKeyOverrides());
    needKeyFlush = encryption.length > 0;

    this.directEncodingColumns = OrcUtils.includeColumns(
        opts.getDirectEncodingColumns(), opts.getSchema());
    dictionaryKeySizeThreshold =
        OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getDouble(conf);

    this.callback = opts.getCallback();
    if (callback != null) {
      callbackContext = () -> WriterImpl.this;
    } else {
      callbackContext = null;
    }

    this.useProlepticGregorian = opts.getProlepticGregorian();
    this.writeTimeZone = hasTimestamp(schema);
    this.useUTCTimeZone = opts.getUseUTCTimestamp();

    this.encodingStrategy = opts.getEncodingStrategy();
    this.compressionStrategy = opts.getCompressionStrategy();

    // ORC-1362: if isBuildIndex=false, then rowIndexStride will be set to 0.
    if (opts.getRowIndexStride() >= 0 && opts.isBuildIndex()) {
      this.rowIndexStride = opts.getRowIndexStride();
    } else {
      this.rowIndexStride = 0;
    }

    this.buildIndex = rowIndexStride > 0;

    if (buildIndex && rowIndexStride < MIN_ROW_INDEX_STRIDE) {
      throw new IllegalArgumentException("Row stride must be at least " +
          MIN_ROW_INDEX_STRIDE);
    }

    this.writerVersion = opts.getWriterVersion();
    this.version = opts.getVersion();
    if (version == OrcFile.Version.FUTURE) {
      throw new IllegalArgumentException("Can not write in a unknown version.");
    } else if (version == OrcFile.Version.UNSTABLE_PRE_2_0) {
      LOG.warn("ORC files written in " + version.getName() + " will not be" +
          " readable by other versions of the software. It is only for" +
          " developer testing.");
    }

    this.bloomFilterVersion = opts.getBloomFilterVersion();
    this.bloomFilterFpp = opts.getBloomFilterFpp();
    /* do not write bloom filters for ORC v11 */
    if (!buildIndex || version == OrcFile.Version.V_0_11) {
      this.bloomFilterColumns = new boolean[schema.getMaximumId() + 1];
    } else {
      this.bloomFilterColumns =
          OrcUtils.includeColumns(opts.getBloomFilterColumns(), schema);
    }

    // ensure that we are able to handle callbacks before we register ourselves
    ROWS_PER_CHECK = Math.min(opts.getStripeRowCountValue(),
        OrcConf.ROWS_BETWEEN_CHECKS.getLong(conf));
    this.stripeRowCount= opts.getStripeRowCountValue();
    this.stripeSize = opts.getStripeSize();
    memoryLimit = stripeSize;
    memoryManager = opts.getMemoryManager();
    memoryManager.addWriter(path, stripeSize, this);

    // Set up the physical writer
    this.physicalWriter = opts.getPhysicalWriter() == null ?
        new PhysicalFsWriter(fs, path, opts, encryption) :
        opts.getPhysicalWriter();
    physicalWriter.writeHeader();
    unencryptedOptions = physicalWriter.getStreamOptions();
    OutStream.assertBufferSizeValid(unencryptedOptions.getBufferSize());

    treeWriter = TreeWriter.Factory.create(schema, null, new StreamFactory());

    LOG.info("ORC writer created for path: {} with stripeSize: {} options: {}",
        path, stripeSize, unencryptedOptions);
  }

  //@VisibleForTesting
  public static int getEstimatedBufferSize(long stripeSize, int numColumns,
                                           int bs) {
    // The worst case is that there are 2 big streams per a column and
    // we want to guarantee that each stream gets ~10 buffers.
    // This keeps buffers small enough that we don't get really small stripe
    // sizes.
    int estBufferSize = (int) (stripeSize / (20L * numColumns));
    estBufferSize = getClosestBufferSize(estBufferSize);
    return Math.min(estBufferSize, bs);
  }

  @Override
  public void increaseCompressionSize(int newSize) {
    if (newSize > unencryptedOptions.getBufferSize()) {
      unencryptedOptions.bufferSize(newSize);
    }
  }

  /**
   * Given a buffer size, return the nearest superior power of 2. Min value is
   * 4Kib, Max value is 256Kib.
   *
   * @param size Proposed buffer size
   * @return the suggested buffer size
   */
  private static int getClosestBufferSize(int size) {
    final int kb4 = 4 * 1024;
    final int kb256 = 256 * 1024;
    final int pow2 = size == 1 ? 1 : Integer.highestOneBit(size - 1) * 2;
    return Math.min(kb256, Math.max(kb4, pow2));
  }

  public static CompressionCodec createCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        return new AircompressorCodec(kind, new LzoCompressor(),
            new LzoDecompressor());
      case LZ4:
        return new AircompressorCodec(kind, new Lz4Compressor(),
            new Lz4Decompressor());
      case ZSTD:
        return new AircompressorCodec(kind, new ZstdCompressor(),
            new ZstdDecompressor());
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }

  @Override
  public boolean checkMemory(double newScale) throws IOException {
    memoryLimit = Math.round(stripeSize * newScale);
    return checkMemory();
  }

  private boolean checkMemory() throws IOException {
    if (rowsSinceCheck >= ROWS_PER_CHECK) {
      rowsSinceCheck = 0;
      long size = treeWriter.estimateMemory();
      if (LOG.isDebugEnabled()) {
        LOG.debug("ORC writer " + physicalWriter + " size = " + size +
            " memoryLimit = " + memoryLimit + " rowsInStripe = " + rowsInStripe +
            " stripeRowCountLimit = " + stripeRowCount);
      }
      if (size > memoryLimit || rowsInStripe >= stripeRowCount) {
        flushStripe();
        return true;
      }
    }
    return false;
  }

  /**
   * Interface from the Writer to the TreeWriters. This limits the visibility
   * that the TreeWriters have into the Writer.
   */
  private class StreamFactory implements WriterContext {

    /**
     * Create a stream to store part of a column.
     * @param name the name for the stream
     * @return The output outStream that the section needs to be written to.
     */
    @Override
    public OutStream createStream(StreamName name) throws IOException {
      StreamOptions options = SerializationUtils.getCustomizedCodec(
          unencryptedOptions, compressionStrategy, name.getKind());
      WriterEncryptionVariant encryption =
          (WriterEncryptionVariant) name.getEncryption();
      if (encryption != null) {
        if (options == unencryptedOptions) {
          options = new StreamOptions(options);
        }
        options.withEncryption(encryption.getKeyDescription().getAlgorithm(),
            encryption.getFileFooterKey())
            .modifyIv(CryptoUtils.modifyIvForStream(name, 1));
      }
      return new OutStream(name, options, physicalWriter.createDataStream(name));
    }

    /**
     * Get the stride rate of the row index.
     */
    @Override
    public int getRowIndexStride() {
      return rowIndexStride;
    }

    /**
     * Should be building the row index.
     * @return true if we are building the index
     */
    @Override
    public boolean buildIndex() {
      return buildIndex;
    }

    /**
     * Is the ORC file compressed?
     * @return are the streams compressed
     */
    @Override
    public boolean isCompressed() {
      return unencryptedOptions.getCodec() != null;
    }

    /**
     * Get the encoding strategy to use.
     * @return encoding strategy
     */
    @Override
    public OrcFile.EncodingStrategy getEncodingStrategy() {
      return encodingStrategy;
    }

    /**
     * Get the bloom filter columns
     * @return bloom filter columns
     */
    @Override
    public boolean[] getBloomFilterColumns() {
      return bloomFilterColumns;
    }

    /**
     * Get bloom filter false positive percentage.
     * @return fpp
     */
    @Override
    public double getBloomFilterFPP() {
      return bloomFilterFpp;
    }

    /**
     * Get the writer's configuration.
     * @return configuration
     */
    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    /**
     * Get the version of the file to write.
     */
    @Override
    public OrcFile.Version getVersion() {
      return version;
    }

    /**
     * Get the PhysicalWriter.
     *
     * @return the file's physical writer.
     */
    @Override
    public PhysicalWriter getPhysicalWriter() {
      return physicalWriter;
    }

    @Override
    public OrcFile.BloomFilterVersion getBloomFilterVersion() {
      return bloomFilterVersion;
    }

    @Override
    public void writeIndex(StreamName name,
                           OrcProto.RowIndex.Builder index) throws IOException {
      physicalWriter.writeIndex(name, index);
    }

    @Override
    public void writeBloomFilter(StreamName name,
                                 OrcProto.BloomFilterIndex.Builder bloom
                                 ) throws IOException {
      physicalWriter.writeBloomFilter(name, bloom);
    }

    @Override
    public WriterEncryptionVariant getEncryption(int columnId) {
      return columnId < columnEncryption.length ?
                 columnEncryption[columnId] : null;
    }

    @Override
    public DataMask getUnencryptedMask(int columnId) {
      if (columnMaskDescriptions != null) {
        MaskDescriptionImpl descr = columnMaskDescriptions[columnId];
        if (descr != null) {
          return DataMask.Factory.build(descr, schema.findSubtype(columnId),
              (type) -> columnMaskDescriptions[type.getId()]);
        }
      }
      return null;
    }

    @Override
    public void setEncoding(int column, WriterEncryptionVariant encryption,
                            OrcProto.ColumnEncoding encoding) {
      if (encryption == null) {
        unencryptedEncodings.add(encoding);
      } else {
        encryption.addEncoding(encoding);
      }
    }

    @Override
    public void writeStatistics(StreamName name,
                                OrcProto.ColumnStatistics.Builder stats
                                ) throws IOException {
      physicalWriter.writeStatistics(name, stats);
    }

    @Override
    public boolean getUseUTCTimestamp() {
      return useUTCTimeZone;
    }

    @Override
    public double getDictionaryKeySizeThreshold(int columnId) {
      return directEncodingColumns[columnId] ? 0.0 : dictionaryKeySizeThreshold;
    }

    @Override
    public boolean getProlepticGregorian() {
      return useProlepticGregorian;
    }
  }


  private static void writeTypes(OrcProto.Footer.Builder builder,
                                 TypeDescription schema) {
    builder.addAllTypes(OrcUtils.getOrcTypes(schema));
  }

  private void createRowIndexEntry() throws IOException {
    treeWriter.createRowIndexEntry();
    rowsInIndex = 0;
  }

  /**
   * Write the encrypted keys into the StripeInformation along with the
   * stripe id, so that the readers can decrypt the data.
   * @param dirEntry the entry to modify
   */
  private void addEncryptedKeys(OrcProto.StripeInformation.Builder dirEntry) {
    for(WriterEncryptionVariant variant: encryption) {
      dirEntry.addEncryptedLocalKeys(ByteString.copyFrom(
          variant.getMaterial().getEncryptedKey()));
    }
    dirEntry.setEncryptStripeId(1 + stripes.size());
  }

  private void flushStripe() throws IOException {
    if (buildIndex && rowsInIndex != 0) {
      createRowIndexEntry();
    }
    if (rowsInStripe != 0) {
      if (callback != null) {
        callback.preStripeWrite(callbackContext);
      }
      // finalize the data for the stripe
      int requiredIndexEntries = rowIndexStride == 0 ? 0 :
          (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
      OrcProto.StripeFooter.Builder builder =
          OrcProto.StripeFooter.newBuilder();
      if (writeTimeZone) {
        if (useUTCTimeZone) {
          builder.setWriterTimezone("UTC");
        } else {
          builder.setWriterTimezone(TimeZone.getDefault().getID());
        }
      }
      treeWriter.flushStreams();
      treeWriter.writeStripe(requiredIndexEntries);
      // update the encodings
      builder.addAllColumns(unencryptedEncodings);
      unencryptedEncodings.clear();
      for (WriterEncryptionVariant writerEncryptionVariant : encryption) {
        OrcProto.StripeEncryptionVariant.Builder encrypt =
            OrcProto.StripeEncryptionVariant.newBuilder();
        encrypt.addAllEncoding(writerEncryptionVariant.getEncodings());
        writerEncryptionVariant.clearEncodings();
        builder.addEncryption(encrypt);
      }
      OrcProto.StripeInformation.Builder dirEntry =
          OrcProto.StripeInformation.newBuilder()
              .setNumberOfRows(rowsInStripe);
      if (encryption.length > 0 && needKeyFlush) {
        addEncryptedKeys(dirEntry);
        needKeyFlush = false;
      }
      physicalWriter.finalizeStripe(builder, dirEntry);

      stripes.add(dirEntry.build());
      rowCount += rowsInStripe;
      rowsInStripe = 0;
    }
  }

  private long computeRawDataSize() {
    return treeWriter.getRawDataSize();
  }

  private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
    switch (kind) {
      case NONE: return OrcProto.CompressionKind.NONE;
      case ZLIB: return OrcProto.CompressionKind.ZLIB;
      case SNAPPY: return OrcProto.CompressionKind.SNAPPY;
      case LZO: return OrcProto.CompressionKind.LZO;
      case LZ4: return OrcProto.CompressionKind.LZ4;
      case ZSTD: return OrcProto.CompressionKind.ZSTD;
      default:
        throw new IllegalArgumentException("Unknown compression " + kind);
    }
  }

  private void writeMetadata() throws IOException {
    // The physical writer now has the stripe statistics, so we pass a
    // new builder in here.
    physicalWriter.writeFileMetadata(OrcProto.Metadata.newBuilder());
  }

  private long writePostScript() throws IOException {
    OrcProto.PostScript.Builder builder =
        OrcProto.PostScript.newBuilder()
            .setMagic(OrcFile.MAGIC)
            .addVersion(version.getMajor())
            .addVersion(version.getMinor())
            .setWriterVersion(writerVersion.getId());
    CompressionCodec codec = unencryptedOptions.getCodec();
    if (codec == null) {
      builder.setCompression(OrcProto.CompressionKind.NONE);
    } else {
      builder.setCompression(writeCompressionKind(codec.getKind()))
             .setCompressionBlockSize(unencryptedOptions.getBufferSize());
    }
    return physicalWriter.writePostScript(builder);
  }

  private OrcProto.EncryptionKey.Builder writeEncryptionKey(WriterEncryptionKey key) {
    OrcProto.EncryptionKey.Builder result = OrcProto.EncryptionKey.newBuilder();
    HadoopShims.KeyMetadata meta = key.getMetadata();
    result.setKeyName(meta.getKeyName());
    result.setKeyVersion(meta.getVersion());
    result.setAlgorithm(OrcProto.EncryptionAlgorithm.valueOf(
        meta.getAlgorithm().getSerialization()));
    return result;
  }

  private OrcProto.EncryptionVariant.Builder
      writeEncryptionVariant(WriterEncryptionVariant variant) {
    OrcProto.EncryptionVariant.Builder result =
        OrcProto.EncryptionVariant.newBuilder();
    result.setRoot(variant.getRoot().getId());
    result.setKey(variant.getKeyDescription().getId());
    result.setEncryptedKey(ByteString.copyFrom(variant.getMaterial().getEncryptedKey()));
    return result;
  }

  private OrcProto.Encryption.Builder writeEncryptionFooter() {
    OrcProto.Encryption.Builder encrypt = OrcProto.Encryption.newBuilder();
    for(MaskDescriptionImpl mask: maskDescriptions.values()) {
      OrcProto.DataMask.Builder maskBuilder = OrcProto.DataMask.newBuilder();
      maskBuilder.setName(mask.getName());
      for(String param: mask.getParameters()) {
        maskBuilder.addMaskParameters(param);
      }
      for(TypeDescription column: mask.getColumns()) {
        maskBuilder.addColumns(column.getId());
      }
      encrypt.addMask(maskBuilder);
    }
    for(WriterEncryptionKey key: keys.values()) {
      encrypt.addKey(writeEncryptionKey(key));
    }
    for(WriterEncryptionVariant variant: encryption) {
      encrypt.addVariants(writeEncryptionVariant(variant));
    }
    encrypt.setKeyProvider(OrcProto.KeyProviderKind.valueOf(
        keyProvider.getKind().getValue()));
    return encrypt;
  }

  private long writeFooter() throws IOException {
    writeMetadata();
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setNumberOfRows(rowCount);
    builder.setRowIndexStride(rowIndexStride);
    rawDataSize = computeRawDataSize();
    // serialize the types
    writeTypes(builder, schema);
    builder.setCalendar(useProlepticGregorian
                              ? OrcProto.CalendarKind.PROLEPTIC_GREGORIAN
                              : OrcProto.CalendarKind.JULIAN_GREGORIAN);
    // add the stripe information
    for(OrcProto.StripeInformation stripe: stripes) {
      builder.addStripes(stripe);
    }
    // add the column statistics
    treeWriter.writeFileStatistics();
    // add all of the user metadata
    for(Map.Entry<String, ByteString> entry: userMetadata.entrySet()) {
      builder.addMetadata(OrcProto.UserMetadataItem.newBuilder()
          .setName(entry.getKey()).setValue(entry.getValue()));
    }
    if (encryption.length > 0) {
      builder.setEncryption(writeEncryptionFooter());
    }
    builder.setWriter(OrcFile.WriterImplementation.ORC_JAVA.getId());
    builder.setSoftwareVersion(OrcUtils.getOrcVersion());
    physicalWriter.writeFileFooter(builder);
    return writePostScript();
  }

  @Override
  public TypeDescription getSchema() {
    return schema;
  }

  @Override
  public void addUserMetadata(String name, ByteBuffer value) {
    userMetadata.put(name, ByteString.copyFrom(value));
  }

  @Override
  public void addRowBatch(VectorizedRowBatch batch) throws IOException {
    try {
      // If this is the first set of rows in this stripe, tell the tree writers
      // to prepare the stripe.
      if (batch.size != 0 && rowsInStripe == 0) {
        treeWriter.prepareStripe(stripes.size() + 1);
      }
      if (buildIndex) {
        // Batch the writes up to the rowIndexStride so that we can get the
        // right size indexes.
        int posn = 0;
        while (posn < batch.size) {
          int chunkSize = Math.min(batch.size - posn,
              rowIndexStride - rowsInIndex);
          if (batch.isSelectedInUse()) {
            // find the longest chunk that is continuously selected from posn
            for (int len = 1; len < chunkSize; ++len) {
              if (batch.selected[posn + len] - batch.selected[posn] != len) {
                chunkSize = len;
                break;
              }
            }
            treeWriter.writeRootBatch(batch, batch.selected[posn], chunkSize);
          } else {
            treeWriter.writeRootBatch(batch, posn, chunkSize);
          }
          posn += chunkSize;
          rowsInIndex += chunkSize;
          rowsInStripe += chunkSize;
          if (rowsInIndex >= rowIndexStride) {
            createRowIndexEntry();
          }
        }
      } else {
        if (batch.isSelectedInUse()) {
          int posn = 0;
          while (posn < batch.size) {
            int chunkSize = 1;
            while (posn + chunkSize < batch.size) {
              // find the longest chunk that is continuously selected from posn
              if (batch.selected[posn + chunkSize] - batch.selected[posn] != chunkSize) {
                break;
              }
              ++chunkSize;
            }
            treeWriter.writeRootBatch(batch, batch.selected[posn], chunkSize);
            posn += chunkSize;
          }
        } else {
          treeWriter.writeRootBatch(batch, 0, batch.size);
        }
        rowsInStripe += batch.size;
      }
      rowsSinceCheck += batch.size;
      previousAllocation = memoryManager.checkMemory(previousAllocation, this);
      checkMemory();
    } catch (Throwable t) {
      try {
        close();
      } catch (Throwable ignore) {
        // ignore
      }
      if (t instanceof IOException) {
        throw (IOException) t;
      } else {
        throw new IOException("Problem adding row to " + path, t);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!isClose) {
      try {
        if (callback != null) {
          callback.preFooterWrite(callbackContext);
        }
        // remove us from the memory manager so that we don't get any callbacks
        memoryManager.removeWriter(path);
        // actually close the file
        flushStripe();
        lastFlushOffset = writeFooter();
        physicalWriter.close();
      } finally {
        isClose = true;
      }
    }
  }

  /**
   * Raw data size will be compute when writing the file footer. Hence raw data
   * size value will be available only after closing the writer.
   */
  @Override
  public long getRawDataSize() {
    return rawDataSize;
  }

  /**
   * Row count gets updated when flushing the stripes. To get accurate row
   * count call this method after writer is closed.
   */
  @Override
  public long getNumberOfRows() {
    return rowCount;
  }

  @Override
  public long writeIntermediateFooter() throws IOException {
    // flush any buffered rows
    flushStripe();
    // write a footer
    if (stripesAtLastFlush != stripes.size()) {
      if (callback != null) {
        callback.preFooterWrite(callbackContext);
      }
      lastFlushOffset = writeFooter();
      stripesAtLastFlush = stripes.size();
      physicalWriter.flush();
    }
    return lastFlushOffset;
  }

  private static void checkArgument(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public void appendStripe(byte[] stripe, int offset, int length,
                           StripeInformation stripeInfo,
                           OrcProto.StripeStatistics stripeStatistics
                           ) throws IOException {
    appendStripe(stripe, offset, length, stripeInfo,
        new StripeStatistics[]{
            new StripeStatisticsImpl(schema, stripeStatistics.getColStatsList(),
                false, false)});
  }

  @Override
  public void appendStripe(byte[] stripe, int offset, int length,
                           StripeInformation stripeInfo,
                           StripeStatistics[] stripeStatistics
                           ) throws IOException {
    checkArgument(stripe != null, "Stripe must not be null");
    checkArgument(length <= stripe.length,
        "Specified length must not be greater specified array length");
    checkArgument(stripeInfo != null, "Stripe information must not be null");
    checkArgument(stripeStatistics != null,
        "Stripe statistics must not be null");

    // If we have buffered rows, flush them
    if (rowsInStripe > 0) {
      flushStripe();
    }
    rowsInStripe = stripeInfo.getNumberOfRows();
    // update stripe information
    OrcProto.StripeInformation.Builder dirEntry =
        OrcProto.StripeInformation.newBuilder()
                                  .setNumberOfRows(rowsInStripe)
                                  .setIndexLength(stripeInfo.getIndexLength())
                                  .setDataLength(stripeInfo.getDataLength())
                                  .setFooterLength(stripeInfo.getFooterLength());
    // If this is the first stripe of the original file, we need to copy the
    // encryption information.
    if (stripeInfo.hasEncryptionStripeId()) {
      dirEntry.setEncryptStripeId(stripeInfo.getEncryptionStripeId());
      for(byte[] key: stripeInfo.getEncryptedLocalKeys()) {
        dirEntry.addEncryptedLocalKeys(ByteString.copyFrom(key));
      }
    }
    physicalWriter.appendRawStripe(ByteBuffer.wrap(stripe, offset, length),
        dirEntry);

    // since we have already written the stripe, just update stripe statistics
    treeWriter.addStripeStatistics(stripeStatistics);

    stripes.add(dirEntry.build());

    // reset it after writing the stripe
    rowCount += rowsInStripe;
    rowsInStripe = 0;
    needKeyFlush = encryption.length > 0;
  }

  @Override
  public void appendUserMetadata(List<OrcProto.UserMetadataItem> userMetadata) {
    if (userMetadata != null) {
      for (OrcProto.UserMetadataItem item : userMetadata) {
        this.userMetadata.put(item.getName(), item.getValue());
      }
    }
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    // get the column statistics
    final ColumnStatistics[] result =
        new ColumnStatistics[schema.getMaximumId() + 1];
    // Get the file statistics, preferring the encrypted one.
    treeWriter.getCurrentStatistics(result);
    return result;
  }

  @Override
  public List<StripeInformation> getStripes() throws IOException {
    return Collections.unmodifiableList(OrcUtils.convertProtoStripesToStripes(stripes));
  }

  public CompressionCodec getCompressionCodec() {
    return unencryptedOptions.getCodec();
  }

  private static boolean hasTimestamp(TypeDescription schema) {
    if (schema.getCategory() == TypeDescription.Category.TIMESTAMP) {
      return true;
    }
    List<TypeDescription> children = schema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        if (hasTimestamp(child)) {
          return true;
        }
      }
    }
    return false;
  }

  private WriterEncryptionKey getKey(String keyName,
                                     KeyProvider provider) throws IOException {
    WriterEncryptionKey result = keys.get(keyName);
    if (result == null) {
      result = new WriterEncryptionKey(provider.getCurrentKeyVersion(keyName));
      keys.put(keyName, result);
    }
    return result;
  }

  private MaskDescriptionImpl getMask(String maskString) {
    // if it is already there, get the earlier object
    MaskDescriptionImpl result = maskDescriptions.get(maskString);
    if (result == null) {
      result = ParserUtils.buildMaskDescription(maskString);
      maskDescriptions.put(maskString, result);
    }
    return result;
  }

  private int visitTypeTree(TypeDescription schema,
                            boolean encrypted,
                            KeyProvider provider) throws IOException {
    int result = 0;
    String keyName = schema.getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE);
    String maskName = schema.getAttributeValue(TypeDescription.MASK_ATTRIBUTE);
    if (keyName != null) {
      if (provider == null) {
        throw new IllegalArgumentException("Encryption requires a KeyProvider.");
      }
      if (encrypted) {
        throw new IllegalArgumentException("Nested encryption type: " + schema);
      }
      encrypted = true;
      result += 1;
      WriterEncryptionKey key = getKey(keyName, provider);
      HadoopShims.KeyMetadata metadata = key.getMetadata();
      WriterEncryptionVariant variant = new WriterEncryptionVariant(key,
          schema, provider.createLocalKey(metadata));
      key.addRoot(variant);
    }
    if (encrypted && (keyName != null || maskName != null)) {
      MaskDescriptionImpl mask = getMask(maskName == null ? "nullify" : maskName);
      mask.addColumn(schema);
    }
    List<TypeDescription> children = schema.getChildren();
    if (children != null) {
      for(TypeDescription child: children) {
        result += visitTypeTree(child, encrypted, provider);
      }
    }
    return result;
  }

  /**
   * Iterate through the encryption options given by the user and set up
   * our data structures.
   * @param provider the KeyProvider to use to generate keys
   * @param schema the type tree that we search for annotations
   * @param keyOverrides user specified key overrides
   */
  private WriterEncryptionVariant[] setupEncryption(
      KeyProvider provider,
      TypeDescription schema,
      Map<String, HadoopShims.KeyMetadata> keyOverrides) throws IOException {
    keyProvider = provider != null ? provider :
                      CryptoUtils.getKeyProvider(conf, new SecureRandom());
    // Load the overrides into the cache so that we use the required key versions.
    for(HadoopShims.KeyMetadata key: keyOverrides.values()) {
      keys.put(key.getKeyName(), new WriterEncryptionKey(key));
    }
    int variantCount = visitTypeTree(schema, false, keyProvider);

    // Now that we have de-duped the keys and maskDescriptions, make the arrays
    int nextId = 0;
    if (variantCount > 0) {
      for (MaskDescriptionImpl mask : maskDescriptions.values()) {
        mask.setId(nextId++);
        for (TypeDescription column : mask.getColumns()) {
          this.columnMaskDescriptions[column.getId()] = mask;
        }
      }
    }
    nextId = 0;
    int nextVariantId = 0;
    WriterEncryptionVariant[] result = new WriterEncryptionVariant[variantCount];
    for(WriterEncryptionKey key: keys.values()) {
      key.setId(nextId++);
      key.sortRoots();
      for(WriterEncryptionVariant variant: key.getEncryptionRoots()) {
        result[nextVariantId] = variant;
        columnEncryption[variant.getRoot().getId()] = variant;
        variant.setId(nextVariantId++);
      }
    }
    return result;
  }

  @Override
  public long estimateMemory() {
    return this.treeWriter.estimateMemory();
  }
}
