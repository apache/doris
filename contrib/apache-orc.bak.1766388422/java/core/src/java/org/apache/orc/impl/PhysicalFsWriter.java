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
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.orc.impl.writer.WriterEncryptionKey;
import org.apache.orc.impl.writer.WriterEncryptionVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PhysicalFsWriter implements PhysicalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFsWriter.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;

  private FSDataOutputStream rawWriter;
  private final DirectStream rawStream;

  // the compressed metadata information outStream
  private OutStream compressStream;
  // a protobuf outStream around streamFactory
  private CodedOutputStream codedCompressStream;

  private Path path;
  private final HadoopShims shims;
  private final long blockSize;
  private final int maxPadding;
  private final StreamOptions compress;
  private final OrcFile.CompressionStrategy compressionStrategy;
  private final boolean addBlockPadding;
  private final boolean writeVariableLengthBlocks;
  private final VariantTracker unencrypted;

  private long headerLength;
  private long stripeStart;
  // The position of the last time we wrote a short block, which becomes the
  // natural blocks
  private long blockOffset;
  private int metadataLength;
  private int stripeStatisticsLength = 0;
  private int footerLength;
  private int stripeNumber = 0;

  private final Map<WriterEncryptionVariant, VariantTracker> variants = new TreeMap<>();

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts
                          ) throws IOException {
    this(fs, path, opts, new WriterEncryptionVariant[0]);
  }

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts,
                          WriterEncryptionVariant[] encryption
                          ) throws IOException {
    this(fs.create(path, opts.getOverwrite(), HDFS_BUFFER_SIZE,
            fs.getDefaultReplication(path), opts.getBlockSize()), opts, encryption);
    this.path = path;
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
            " compression: {}", path, opts.getStripeSize(), blockSize, compress);
  }

  public PhysicalFsWriter(FSDataOutputStream outputStream,
                          OrcFile.WriterOptions opts,
                          WriterEncryptionVariant[] encryption
                          ) throws IOException {
    this.rawWriter = outputStream;
    long defaultStripeSize = opts.getStripeSize();
    this.addBlockPadding = opts.getBlockPadding();
    if (opts.isEnforceBufferSize()) {
      this.compress = new StreamOptions(opts.getBufferSize());
    } else {
      this.compress = new StreamOptions(
          WriterImpl.getEstimatedBufferSize(defaultStripeSize,
          opts.getSchema().getMaximumId() + 1,
          opts.getBufferSize()));
    }
    CompressionCodec codec = OrcCodecPool.getCodec(opts.getCompress());
    if (codec != null){
      compress.withCodec(codec, codec.getDefaultOptions());
    }
    this.compressionStrategy = opts.getCompressionStrategy();
    this.maxPadding = (int) (opts.getPaddingTolerance() * defaultStripeSize);
    this.blockSize = opts.getBlockSize();
    blockOffset = 0;
    unencrypted = new VariantTracker(opts.getSchema(), compress);
    writeVariableLengthBlocks = opts.getWriteVariableLengthBlocks();
    shims = opts.getHadoopShims();
    rawStream = new DirectStream(rawWriter);
    compressStream = new OutStream("stripe footer", compress, rawStream);
    codedCompressStream = CodedOutputStream.newInstance(compressStream);
    for(WriterEncryptionVariant variant: encryption) {
      WriterEncryptionKey key = variant.getKeyDescription();
      StreamOptions encryptOptions =
          new StreamOptions(unencrypted.options)
              .withEncryption(key.getAlgorithm(), variant.getFileFooterKey());
      variants.put(variant, new VariantTracker(variant.getRoot(), encryptOptions));
    }
  }

  /**
   * Record the information about each column encryption variant.
   * The unencrypted data and each encrypted column root are variants.
   */
  protected static class VariantTracker {
    // the streams that make up the current stripe
    protected final Map<StreamName, BufferedStream> streams = new TreeMap<>();
    private final int rootColumn;
    private final int lastColumn;
    protected final StreamOptions options;
    // a list for each column covered by this variant
    // the elements in the list correspond to each stripe in the file
    protected final List<OrcProto.ColumnStatistics>[] stripeStats;
    protected final List<OrcProto.Stream> stripeStatsStreams = new ArrayList<>();
    protected final OrcProto.ColumnStatistics[] fileStats;

    VariantTracker(TypeDescription schema, StreamOptions options) {
      rootColumn = schema.getId();
      lastColumn = schema.getMaximumId();
      this.options = options;
      stripeStats = new List[schema.getMaximumId() - schema.getId() + 1];
      for(int i=0; i < stripeStats.length; ++i) {
        stripeStats[i] = new ArrayList<>();
      }
      fileStats = new OrcProto.ColumnStatistics[stripeStats.length];
    }

    public BufferedStream createStream(StreamName name) {
      BufferedStream result = new BufferedStream();
      streams.put(name, result);
      return result;
    }

    /**
     * Place the streams in the appropriate area while updating the sizes
     * with the number of bytes in the area.
     * @param area the area to write
     * @param sizes the sizes of the areas
     * @return the list of stream descriptions to add
     */
    public List<OrcProto.Stream> placeStreams(StreamName.Area area,
                                              SizeCounters sizes) {
      List<OrcProto.Stream> result = new ArrayList<>(streams.size());
      for(Map.Entry<StreamName, BufferedStream> stream: streams.entrySet()) {
        StreamName name = stream.getKey();
        BufferedStream bytes = stream.getValue();
        if (name.getArea() == area && !bytes.isSuppressed) {
          OrcProto.Stream.Builder builder = OrcProto.Stream.newBuilder();
          long size = bytes.getOutputSize();
          if (area == StreamName.Area.INDEX) {
            sizes.index += size;
          } else {
            sizes.data += size;
          }
          builder.setColumn(name.getColumn())
              .setKind(name.getKind())
              .setLength(size);
          result.add(builder.build());
        }
      }
      return result;
    }

    /**
     * Write the streams in the appropriate area.
     * @param area the area to write
     * @param raw the raw stream to write to
     */
    public void writeStreams(StreamName.Area area,
                             FSDataOutputStream raw) throws IOException {
      for(Map.Entry<StreamName, BufferedStream> stream: streams.entrySet()) {
        if (stream.getKey().getArea() == area) {
          stream.getValue().spillToDiskAndClear(raw);
        }
      }
    }

    /**
     * Computed the size of the given column on disk for this stripe.
     * It excludes the index streams.
     * @param column a column id
     * @return the total number of bytes
     */
    public long getFileBytes(int column) {
      long result = 0;
      if (column >= rootColumn && column <= lastColumn) {
        for(Map.Entry<StreamName, BufferedStream> entry: streams.entrySet()) {
          StreamName name = entry.getKey();
          if (name.getColumn() == column &&
              name.getArea() != StreamName.Area.INDEX) {
            result += entry.getValue().getOutputSize();
          }
        }
      }
      return result;
    }
  }

  VariantTracker getVariant(EncryptionVariant column) {
    if (column == null) {
      return unencrypted;
    }
    return variants.get(column);
  }

  /**
   * Get the number of bytes for a file in a given column
   * by finding all the streams (not suppressed)
   * for a given column and returning the sum of their sizes.
   * excludes index
   *
   * @param column column from which to get file size
   * @return number of bytes for the given column
   */
  @Override
  public long getFileBytes(int column, WriterEncryptionVariant variant) {
    return getVariant(variant).getFileBytes(column);
  }

  @Override
  public StreamOptions getStreamOptions() {
    return unencrypted.options;
  }

  private static final byte[] ZEROS = new byte[64*1024];

  private static void writeZeros(OutputStream output,
                                 long remaining) throws IOException {
    while (remaining > 0) {
      long size = Math.min(ZEROS.length, remaining);
      output.write(ZEROS, 0, (int) size);
      remaining -= size;
    }
  }

  /**
   * Do any required shortening of the HDFS block or padding to avoid stradling
   * HDFS blocks. This is called before writing the current stripe.
   * @param stripeSize the number of bytes in the current stripe
   */
  private void padStripe(long stripeSize) throws IOException {
    this.stripeStart = rawWriter.getPos();
    long previousBytesInBlock = (stripeStart - blockOffset) % blockSize;
    // We only have options if this isn't the first stripe in the block
    if (previousBytesInBlock > 0) {
      if (previousBytesInBlock + stripeSize >= blockSize) {
        // Try making a short block
        if (writeVariableLengthBlocks &&
            shims.endVariableLengthBlock(rawWriter)) {
          blockOffset = stripeStart;
        } else if (addBlockPadding) {
          // if we cross the block boundary, figure out what we should do
          long padding = blockSize - previousBytesInBlock;
          if (padding <= maxPadding) {
            writeZeros(rawWriter, padding);
            stripeStart += padding;
          }
        }
      }
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   */
  private static class DirectStream implements OutputReceiver {
    private final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      throw new UnsupportedOperationException("Can't suppress direct stream");
    }
  }

  private void writeStripeFooter(OrcProto.StripeFooter footer,
                                 SizeCounters sizes,
                                 OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    footer.writeTo(codedCompressStream);
    codedCompressStream.flush();
    compressStream.flush();
    dirEntry.setOffset(stripeStart);
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - sizes.total());
  }

  /**
   * Write the saved encrypted stripe statistic in a variant out to the file.
   * The streams that are written are added to the tracker.stripeStatsStreams.
   * @param output the file we are writing to
   * @param stripeNumber the number of stripes in the file
   * @param tracker the variant to write out
   */
  static void writeEncryptedStripeStatistics(DirectStream output,
                                             int stripeNumber,
                                             VariantTracker tracker
                                             ) throws IOException {
    StreamOptions options = new StreamOptions(tracker.options);
    tracker.stripeStatsStreams.clear();
    for(int col = tracker.rootColumn;
        col < tracker.rootColumn + tracker.stripeStats.length; ++col) {
      options.modifyIv(CryptoUtils.modifyIvForStream(col,
          OrcProto.Stream.Kind.STRIPE_STATISTICS, stripeNumber + 1));
      OutStream stream = new OutStream("stripe stats for " + col,
          options, output);
      OrcProto.ColumnarStripeStatistics stats =
          OrcProto.ColumnarStripeStatistics.newBuilder()
              .addAllColStats(tracker.stripeStats[col - tracker.rootColumn])
              .build();
      long start = output.output.getPos();
      stats.writeTo(stream);
      stream.flush();
      OrcProto.Stream description = OrcProto.Stream.newBuilder()
                                   .setColumn(col)
                                   .setKind(OrcProto.Stream.Kind.STRIPE_STATISTICS)
                                   .setLength(output.output.getPos() - start)
                                   .build();
      tracker.stripeStatsStreams.add(description);
    }
  }

  /**
   * Merge the saved unencrypted stripe statistics into the Metadata section
   * of the footer.
   * @param builder the Metadata section of the file
   * @param stripeCount the number of stripes in the file
   * @param stats the stripe statistics
   */
  static void setUnencryptedStripeStatistics(OrcProto.Metadata.Builder builder,
                                             int stripeCount,
                                             List<OrcProto.ColumnStatistics>[] stats) {
    // Make the unencrypted stripe stats into lists of StripeStatistics.
    builder.clearStripeStats();
    for(int s=0; s < stripeCount; ++s) {
      OrcProto.StripeStatistics.Builder stripeStats =
          OrcProto.StripeStatistics.newBuilder();
      for(List<OrcProto.ColumnStatistics> col: stats) {
        stripeStats.addColStats(col.get(s));
      }
      builder.addStripeStats(stripeStats.build());
    }
  }

  static void setEncryptionStatistics(OrcProto.Encryption.Builder encryption,
                                      int stripeNumber,
                                      Collection<VariantTracker> variants
                                      ) throws IOException {
    int v = 0;
    for(VariantTracker variant: variants) {
      OrcProto.EncryptionVariant.Builder variantBuilder =
          encryption.getVariantsBuilder(v++);

      // Add the stripe statistics streams to the variant description.
      variantBuilder.clearStripeStatistics();
      variantBuilder.addAllStripeStatistics(variant.stripeStatsStreams);

      // Serialize and encrypt the file statistics.
      OrcProto.FileStatistics.Builder file = OrcProto.FileStatistics.newBuilder();
      for(OrcProto.ColumnStatistics col: variant.fileStats) {
        file.addColumn(col);
      }
      StreamOptions options = new StreamOptions(variant.options);
      options.modifyIv(CryptoUtils.modifyIvForStream(variant.rootColumn,
          OrcProto.Stream.Kind.FILE_STATISTICS, stripeNumber + 1));
      BufferedStream buffer = new BufferedStream();
      OutStream stream = new OutStream("stats for " + variant, options, buffer);
      file.build().writeTo(stream);
      stream.flush();
      variantBuilder.setFileStatistics(buffer.getBytes());
    }
  }

  @Override
  public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
    long stripeStatisticsStart = rawWriter.getPos();
    for(VariantTracker variant: variants.values()) {
      writeEncryptedStripeStatistics(rawStream, stripeNumber, variant);
    }
    setUnencryptedStripeStatistics(builder, stripeNumber,
        unencrypted.stripeStats);
    long metadataStart = rawWriter.getPos();
    builder.build().writeTo(codedCompressStream);
    codedCompressStream.flush();
    compressStream.flush();
    this.stripeStatisticsLength = (int) (metadataStart - stripeStatisticsStart);
    this.metadataLength = (int) (rawWriter.getPos() - metadataStart);
  }

  static void addUnencryptedStatistics(OrcProto.Footer.Builder builder,
                                       OrcProto.ColumnStatistics[] stats) {
    for(OrcProto.ColumnStatistics stat: stats) {
      builder.addStatistics(stat);
    }
  }

  @Override
  public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
    if (variants.size() > 0) {
      OrcProto.Encryption.Builder encryption = builder.getEncryptionBuilder();
      setEncryptionStatistics(encryption, stripeNumber, variants.values());
    }
    addUnencryptedStatistics(builder, unencrypted.fileStats);
    long bodyLength = rawWriter.getPos() - metadataLength - stripeStatisticsLength;
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    long startPosn = rawWriter.getPos();
    OrcProto.Footer footer = builder.build();
    footer.writeTo(codedCompressStream);
    codedCompressStream.flush();
    compressStream.flush();
    this.footerLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
    builder.setFooterLength(footerLength);
    builder.setMetadataLength(metadataLength);
    if (variants.size() > 0) {
      builder.setStripeStatisticsLength(stripeStatisticsLength);
    }
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    rawWriter.writeByte((int)length);
    return rawWriter.getPos();
  }

  @Override
  public void close() throws IOException {
    // We don't use the codec directly but do give it out codec in getCompressionCodec;
    // that is used in tests, for boolean checks, and in StreamFactory. Some of the changes that
    // would get rid of this pattern require cross-project interface changes, so just return the
    // codec for now.
    CompressionCodec codec = compress.getCodec();
    if (codec != null) {
      OrcCodecPool.returnCodec(codec.getKind(), codec);
    }
    compress.withCodec(null, null);
    rawWriter.close();
    rawWriter = null;
  }

  @Override
  public void flush() throws IOException {
    rawWriter.hflush();
  }

  @Override
  public void appendRawStripe(ByteBuffer buffer,
      OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();
    int length = buffer.remaining();
    long availBlockSpace = blockSize - (start % blockSize);

    // see if stripe can fit in the current hdfs block, else pad the remaining
    // space in the block
    if (length < blockSize && length > availBlockSpace &&
        addBlockPadding) {
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
      LOG.info("Padding ORC by {} bytes while merging", availBlockSpace);
      start += availBlockSpace;
      while (availBlockSpace > 0) {
        int writeLen = (int) Math.min(availBlockSpace, pad.length);
        rawWriter.write(pad, 0, writeLen);
        availBlockSpace -= writeLen;
      }
    }
    rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        length);
    dirEntry.setOffset(start);
    stripeNumber += 1;
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   */
  static final class BufferedStream implements OutputReceiver {
    private boolean isSuppressed = false;
    private final List<ByteBuffer> output = new ArrayList<>();

    @Override
    public void output(ByteBuffer buffer) {
      if (!isSuppressed) {
        output.add(buffer);
      }
    }

    @Override
    public void suppress() {
      isSuppressed = true;
      output.clear();
    }

    /**
     * Write any saved buffers to the OutputStream if needed, and clears all the
     * buffers.
     * @return true if the stream was written
     */
    boolean spillToDiskAndClear(FSDataOutputStream raw) throws IOException {
      if (!isSuppressed) {
        for (ByteBuffer buffer: output) {
          raw.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
              buffer.remaining());
        }
        output.clear();
        return true;
      }
      isSuppressed = false;
      return false;
    }

    /**
     * Get the buffer as a protobuf ByteString and clears the BufferedStream.
     * @return the bytes
     */
    ByteString getBytes() {
      int len = output.size();
      if (len == 0) {
        return ByteString.EMPTY;
      } else {
        ByteString result = ByteString.copyFrom(output.get(0));
        for (int i=1; i < output.size(); ++i) {
          result = result.concat(ByteString.copyFrom(output.get(i)));
        }
        output.clear();
        return result;
      }
    }

    /**
     * Get the stream as a ByteBuffer and clear it.
     * @return a single ByteBuffer with the contents of the stream
     */
    ByteBuffer getByteBuffer() {
      ByteBuffer result;
      if (output.size() == 1) {
        result = output.get(0);
      } else {
        result = ByteBuffer.allocate((int) getOutputSize());
        for (ByteBuffer buffer : output) {
          result.put(buffer);
        }
        output.clear();
        result.flip();
      }
      return result;
    }

    /**
     * Get the number of bytes that will be written to the output.
     *
     * Assumes the stream writing into this receiver has already been flushed.
     * @return number of bytes
     */
    public long getOutputSize() {
      long result = 0;
      for (ByteBuffer buffer: output) {
        result += buffer.remaining();
      }
      return result;
    }
  }

  static class SizeCounters {
    long index = 0;
    long data = 0;

    long total() {
      return index + data;
    }
  }

  void buildStreamList(OrcProto.StripeFooter.Builder footerBuilder,
                       SizeCounters sizes
                       ) throws IOException {
    footerBuilder.addAllStreams(
        unencrypted.placeStreams(StreamName.Area.INDEX, sizes));
    final long unencryptedIndexSize = sizes.index;
    int v = 0;
    for (VariantTracker variant: variants.values()) {
      OrcProto.StripeEncryptionVariant.Builder builder =
          footerBuilder.getEncryptionBuilder(v++);
      builder.addAllStreams(
          variant.placeStreams(StreamName.Area.INDEX, sizes));
    }
    if (sizes.index != unencryptedIndexSize) {
      // add a placeholder that covers the hole where the encrypted indexes are
      footerBuilder.addStreams(OrcProto.Stream.newBuilder()
                                   .setKind(OrcProto.Stream.Kind.ENCRYPTED_INDEX)
                                   .setLength(sizes.index - unencryptedIndexSize));
    }
    footerBuilder.addAllStreams(
        unencrypted.placeStreams(StreamName.Area.DATA, sizes));
    final long unencryptedDataSize = sizes.data;
    v = 0;
    for (VariantTracker variant: variants.values()) {
      OrcProto.StripeEncryptionVariant.Builder builder =
          footerBuilder.getEncryptionBuilder(v++);
      builder.addAllStreams(
          variant.placeStreams(StreamName.Area.DATA, sizes));
    }
    if (sizes.data != unencryptedDataSize) {
      // add a placeholder that covers the hole where the encrypted indexes are
      footerBuilder.addStreams(OrcProto.Stream.newBuilder()
                                   .setKind(OrcProto.Stream.Kind.ENCRYPTED_DATA)
                                   .setLength(sizes.data - unencryptedDataSize));
    }
  }

  @Override
  public void finalizeStripe(OrcProto.StripeFooter.Builder footerBuilder,
                             OrcProto.StripeInformation.Builder dirEntry
                             ) throws IOException {
    SizeCounters sizes = new SizeCounters();
    buildStreamList(footerBuilder, sizes);

    OrcProto.StripeFooter footer = footerBuilder.build();

    // Do we need to pad the file so the stripe doesn't straddle a block boundary?
    padStripe(sizes.total() + footer.getSerializedSize());

    // write the unencrypted index streams
    unencrypted.writeStreams(StreamName.Area.INDEX, rawWriter);
    // write the encrypted index streams
    for (VariantTracker variant: variants.values()) {
      variant.writeStreams(StreamName.Area.INDEX, rawWriter);
    }

    // write the unencrypted data streams
    unencrypted.writeStreams(StreamName.Area.DATA, rawWriter);
    // write out the unencrypted data streams
    for (VariantTracker variant: variants.values()) {
      variant.writeStreams(StreamName.Area.DATA, rawWriter);
    }

    // Write out the footer.
    writeStripeFooter(footer, sizes, dirEntry);

    // fill in the data sizes
    dirEntry.setDataLength(sizes.data);
    dirEntry.setIndexLength(sizes.index);

    stripeNumber += 1;
  }

  @Override
  public void writeHeader() throws IOException {
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();
  }

  @Override
  public BufferedStream createDataStream(StreamName name) {
    VariantTracker variant = getVariant(name.getEncryption());
    BufferedStream result = variant.streams.get(name);
    if (result == null) {
      result = new BufferedStream();
      variant.streams.put(name, result);
    }
    return result;
  }

  private StreamOptions getOptions(OrcProto.Stream.Kind kind) {
    return SerializationUtils.getCustomizedCodec(compress, compressionStrategy,
        kind);
  }

  protected OutputStream createIndexStream(StreamName name) {
    BufferedStream buffer = createDataStream(name);
    VariantTracker tracker = getVariant(name.getEncryption());
    StreamOptions options =
        SerializationUtils.getCustomizedCodec(tracker.options,
            compressionStrategy, name.getKind());
    if (options.isEncrypted()) {
      if (options == tracker.options) {
        options = new StreamOptions(options);
      }
      options.modifyIv(CryptoUtils.modifyIvForStream(name, stripeNumber + 1));
    }
    return new OutStream(name.toString(), options, buffer);
  }

  @Override
  public void writeIndex(StreamName name,
                         OrcProto.RowIndex.Builder index
                         ) throws IOException {
    OutputStream stream = createIndexStream(name);
    index.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void writeBloomFilter(StreamName name,
                               OrcProto.BloomFilterIndex.Builder bloom
                               ) throws IOException {
    OutputStream stream = createIndexStream(name);
    bloom.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void writeStatistics(StreamName name,
                              OrcProto.ColumnStatistics.Builder statistics
                              ) {
    VariantTracker tracker = getVariant(name.getEncryption());
    if (name.getKind() == OrcProto.Stream.Kind.FILE_STATISTICS) {
      tracker.fileStats[name.getColumn() - tracker.rootColumn] =
          statistics.build();
    } else {
      tracker.stripeStats[name.getColumn() - tracker.rootColumn]
          .add(statistics.build());
    }
  }

  @Override
  public String toString() {
    if (path != null) {
      return path.toString();
    } else {
      return ByteString.EMPTY.toString();
    }
  }
}
