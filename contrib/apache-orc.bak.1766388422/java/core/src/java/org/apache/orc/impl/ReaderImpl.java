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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.io.Text;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataMaskDescription;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.EncryptionKey;
import org.apache.orc.EncryptionVariant;
import org.apache.orc.FileFormatException;
import org.apache.orc.FileMetadata;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.ReaderEncryptionVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class ReaderImpl implements Reader {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderImpl.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;
  public static final int DEFAULT_COMPRESSION_BLOCK_SIZE = 256 * 1024;

  private final long maxLength;
  protected final Path path;
  protected final OrcFile.ReaderOptions options;
  protected final org.apache.orc.CompressionKind compressionKind;
  protected FSDataInputStream file;
  protected int bufferSize;
  // the unencrypted stripe statistics or null if they haven't been read yet
  protected List<OrcProto.StripeStatistics> stripeStatistics;
  private final int metadataSize;
  protected final List<OrcProto.Type> types;
  private final TypeDescription schema;
  private final List<OrcProto.UserMetadataItem> userMetadata;
  private final List<OrcProto.ColumnStatistics> fileStats;
  private final List<StripeInformation> stripes;
  protected final int rowIndexStride;
  private final long contentLength, numberOfRows;
  private final ReaderEncryption encryption;

  private long deserializedSize = -1;
  protected final Configuration conf;
  protected final boolean useUTCTimestamp;
  private final List<Integer> versionList;
  private final OrcFile.WriterVersion writerVersion;
  private final String softwareVersion;

  protected final OrcTail tail;

  public static class StripeInformationImpl
      implements StripeInformation {
    private final long stripeId;
    private final long originalStripeId;
    private final byte[][] encryptedKeys;
    private final OrcProto.StripeInformation stripe;

    public StripeInformationImpl(OrcProto.StripeInformation stripe,
                                 long stripeId,
                                 long previousOriginalStripeId,
                                 byte[][] previousKeys) {
      this.stripe = stripe;
      this.stripeId = stripeId;
      if (stripe.hasEncryptStripeId()) {
        originalStripeId = stripe.getEncryptStripeId();
      } else {
        originalStripeId = previousOriginalStripeId + 1;
      }
      if (stripe.getEncryptedLocalKeysCount() != 0) {
        encryptedKeys = new byte[stripe.getEncryptedLocalKeysCount()][];
        for(int v=0; v < encryptedKeys.length; ++v) {
          encryptedKeys[v] = stripe.getEncryptedLocalKeys(v).toByteArray();
        }
      } else {
        encryptedKeys = previousKeys;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StripeInformationImpl that = (StripeInformationImpl) o;
      return stripeId == that.stripeId &&
             originalStripeId == that.originalStripeId &&
             Arrays.deepEquals(encryptedKeys, that.encryptedKeys) &&
             stripe.equals(that.stripe);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(stripeId, originalStripeId, stripe);
      result = 31 * result + Arrays.hashCode(encryptedKeys);
      return result;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getLength() {
      return stripe.getDataLength() + getIndexLength() + getFooterLength();
    }

    @Override
    public long getDataLength() {
      return stripe.getDataLength();
    }

    @Override
    public long getFooterLength() {
      return stripe.getFooterLength();
    }

    @Override
    public long getIndexLength() {
      return stripe.getIndexLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

    @Override
    public long getStripeId() {
      return stripeId;
    }

    @Override
    public boolean hasEncryptionStripeId() {
      return stripe.hasEncryptStripeId();
    }

    @Override
    public long getEncryptionStripeId() {
      return originalStripeId;
    }

    @Override
    public byte[][] getEncryptedLocalKeys() {
      return encryptedKeys;
    }

    @Override
    public String toString() {
      return "offset: " + getOffset() + " data: " +
                 getDataLength() + " rows: " + getNumberOfRows() + " tail: " +
                 getFooterLength() + " index: " + getIndexLength() +
                 (!hasEncryptionStripeId() || stripeId == originalStripeId - 1
                      ? "" : " encryption id: " + originalStripeId);
    }
  }

  @Override
  public long getNumberOfRows() {
    return numberOfRows;
  }

  @Override
  public List<String> getMetadataKeys() {
    List<String> result = new ArrayList<>();
    for(OrcProto.UserMetadataItem item: userMetadata) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: userMetadata) {
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  @Override
  public boolean hasMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: userMetadata) {
      if (item.hasName() && item.getName().equals(key)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public org.apache.orc.CompressionKind getCompressionKind() {
    return compressionKind;
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public List<StripeInformation> getStripes() {
    return stripes;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return OrcUtils.getOrcTypes(schema);
  }

  public static OrcFile.Version getFileVersion(List<Integer> versionList) {
    if (versionList == null || versionList.isEmpty()) {
      return OrcFile.Version.V_0_11;
    }
    for (OrcFile.Version version: OrcFile.Version.values()) {
      if (version.getMajor() == versionList.get(0) &&
          version.getMinor() == versionList.get(1)) {
        return version;
      }
    }
    return OrcFile.Version.FUTURE;
  }

  @Override
  public OrcFile.Version getFileVersion() {
    return getFileVersion(versionList);
  }

  @Override
  public OrcFile.WriterVersion getWriterVersion() {
    return writerVersion;
  }

  @Override
  public String getSoftwareVersion() {
    return softwareVersion;
  }

  @Override
  public OrcProto.FileTail getFileTail() {
    return tail.getFileTail();
  }

  @Override
  public EncryptionKey[] getColumnEncryptionKeys() {
    return encryption.getKeys();
  }

  @Override
  public DataMaskDescription[] getDataMasks() {
    return encryption.getMasks();
  }

  @Override
  public ReaderEncryptionVariant[] getEncryptionVariants() {
    return encryption.getVariants();
  }

  @Override
  public List<StripeStatistics> getVariantStripeStatistics(EncryptionVariant variant)
      throws IOException {
    if (variant == null) {
      if (stripeStatistics == null) {
        try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
          InStream.StreamOptions options = new InStream.StreamOptions();
          if (codec != null) {
            options.withCodec(codec).withBufferSize(bufferSize);
          }

          // deserialize the unencrypted stripe statistics
          stripeStatistics = deserializeStripeStats(tail.getTailBuffer(),
                tail.getMetadataOffset(), tail.getMetadataSize(), options);
        }
      }
      return convertFromProto(stripeStatistics);
    } else {
      try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
        InStream.StreamOptions compression = new InStream.StreamOptions();
        if (codec != null) {
          compression.withCodec(codec).withBufferSize(bufferSize);
        }
        return ((ReaderEncryptionVariant) variant).getStripeStatistics(null,
            compression, this);
      }
    }
  }

  /**
   * Internal access to our view of the encryption.
   * @return the encryption information for this reader.
   */
  public ReaderEncryption getEncryption() {
    return encryption;
  }

  @Override
  public int getRowIndexStride() {
    return rowIndexStride;
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result = deserializeStats(schema, fileStats);
    if (encryption.getKeys().length > 0) {
      try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
        InStream.StreamOptions compression = InStream.options();
        if (codec != null) {
          compression.withCodec(codec).withBufferSize(bufferSize);
        }
        for (int c = schema.getId(); c <= schema.getMaximumId(); ++c) {
          ReaderEncryptionVariant variant = encryption.getVariant(c);
          if (variant != null) {
            try {
              int base = variant.getRoot().getId();
              ColumnStatistics[] overrides = decryptFileStats(variant,
                  compression, tail.getFooter());
              for(int sub=0; sub < overrides.length; ++sub) {
                result[base + sub] = overrides[sub];
              }
            } catch (IOException e) {
              throw new RuntimeException("Can't decrypt file stats for " + path +
                                         " with " + variant.getKeyDescription());
            }
          }
        }
      }
    }
    return result;
  }

  private ColumnStatistics[] decryptFileStats(ReaderEncryptionVariant encryption,
                                              InStream.StreamOptions compression,
                                              OrcProto.Footer footer
                                              ) throws IOException {
    Key key = encryption.getFileFooterKey();
    if (key == null) {
      return null;
    } else {
      OrcProto.EncryptionVariant protoVariant =
          footer.getEncryption().getVariants(encryption.getVariantId());
      byte[] bytes = protoVariant.getFileStatistics().toByteArray();
      BufferChunk buffer = new BufferChunk(ByteBuffer.wrap(bytes), 0);
      EncryptionAlgorithm algorithm = encryption.getKeyDescription().getAlgorithm();
      byte[] iv = new byte[algorithm.getIvLength()];
      CryptoUtils.modifyIvForStream(encryption.getRoot().getId(),
          OrcProto.Stream.Kind.FILE_STATISTICS, footer.getStripesCount() + 1)
          .accept(iv);
      InStream.StreamOptions options = new InStream.StreamOptions(compression)
                                           .withEncryption(algorithm, key, iv);
      InStream in = InStream.create("encrypted file stats", buffer,
          0, bytes.length, options);
      OrcProto.FileStatistics decrypted = OrcProto.FileStatistics.parseFrom(in);
      ColumnStatistics[] result = new ColumnStatistics[decrypted.getColumnCount()];
      TypeDescription root = encryption.getRoot();
      for(int i= 0; i < result.length; ++i){
        result[i] = ColumnStatisticsImpl.deserialize(root.findSubtype(root.getId() + i),
            decrypted.getColumn(i), writerUsedProlepticGregorian(),
            getConvertToProlepticGregorian());
      }
      return result;
    }
  }

  public ColumnStatistics[] deserializeStats(
      TypeDescription schema,
      List<OrcProto.ColumnStatistics> fileStats) {
    ColumnStatistics[] result = new ColumnStatistics[fileStats.size()];
    for(int i=0; i < result.length; ++i) {
      TypeDescription subschema = schema == null ? null : schema.findSubtype(i);
      result[i] = ColumnStatisticsImpl.deserialize(subschema, fileStats.get(i),
          writerUsedProlepticGregorian(),
          getConvertToProlepticGregorian());
    }
    return result;
  }

  @Override
  public TypeDescription getSchema() {
    return schema;
  }

  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   * @param in the file being read
   * @param path the filename for error messages
   * @param psLen the postscript length
   * @param buffer the tail of the file
   */
  protected static void ensureOrcFooter(FSDataInputStream in,
                                        Path path,
                                        int psLen,
                                        ByteBuffer buffer) throws IOException {
    int magicLength = OrcFile.MAGIC.length();
    int fullLength = magicLength + 1;
    if (psLen < fullLength || buffer.remaining() < fullLength) {
      throw new FileFormatException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen);
    }
    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - fullLength;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      byte[] header = new byte[magicLength];
      in.readFully(0, header, 0, magicLength);
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0 , magicLength).equals(OrcFile.MAGIC)) {
        throw new FileFormatException("Malformed ORC file " + path +
            ". Invalid postscript.");
      }
    }
  }

  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   * @param psLen the postscript length
   * @param buffer the tail of the file
   * @deprecated Use {@link ReaderImpl#ensureOrcFooter(FSDataInputStream, Path, int, ByteBuffer)} instead.
   */
  protected static void ensureOrcFooter(ByteBuffer buffer, int psLen) throws IOException {
    int magicLength = OrcFile.MAGIC.length();
    int fullLength = magicLength + 1;
    if (psLen < fullLength || buffer.remaining() < fullLength) {
      throw new FileFormatException("Malformed ORC file. Invalid postscript length " + psLen);
    }

    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - fullLength;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
      // if it isn't there, this may be 0.11.0 version of the ORC file.
      // Read the first 3 bytes from the buffer to check for the header
      if (!Text.decode(buffer.array(), 0, magicLength).equals(OrcFile.MAGIC)) {
        throw new FileFormatException("Malformed ORC file. Invalid postscript length " + psLen);
      }
    }
  }

  /**
   * Build a version string out of an array.
   * @param version the version number as a list
   * @return the human readable form of the version string
   */
  private static String versionString(List<Integer> version) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < version.size(); ++i) {
      if (i != 0) {
        buffer.append('.');
      }
      buffer.append(version.get(i));
    }
    return buffer.toString();
  }

  /**
   * Check to see if this ORC file is from a future version and if so,
   * warn the user that we may not be able to read all of the column encodings.
   * @param path the data source path for error messages
   * @param postscript the parsed postscript
   */
  protected static void checkOrcVersion(Path path,
                                        OrcProto.PostScript postscript
                                        ) throws IOException {
    List<Integer> version = postscript.getVersionList();
    if (getFileVersion(version) == OrcFile.Version.FUTURE) {
      throw new IOException(path + " was written by a future ORC version " +
          versionString(version) + ". This file is not readable by this version of ORC.\n"+
          "Postscript: " + TextFormat.shortDebugString(postscript));
    }
  }

  /**
  * Constructor that let's the user specify additional options.
   * @param path pathname for file
   * @param options options for reading
   */
  public ReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
    this.path = path;
    this.options = options;
    this.conf = options.getConfiguration();
    this.maxLength = options.getMaxLength();
    this.useUTCTimestamp = options.getUseUTCTimestamp();
    FileMetadata fileMetadata = options.getFileMetadata();
    if (fileMetadata != null) {
      this.compressionKind = fileMetadata.getCompressionKind();
      this.bufferSize = fileMetadata.getCompressionBufferSize();
      this.metadataSize = fileMetadata.getMetadataSize();
      this.stripeStatistics = fileMetadata.getStripeStats();
      this.versionList = fileMetadata.getVersionList();
      OrcFile.WriterImplementation writer =
          OrcFile.WriterImplementation.from(fileMetadata.getWriterImplementation());
      this.writerVersion =
          OrcFile.WriterVersion.from(writer, fileMetadata.getWriterVersionNum());
      List<OrcProto.Type> types = fileMetadata.getTypes();
      OrcUtils.isValidTypeTree(types, 0);
      this.schema = OrcUtils.convertTypeFromProtobuf(types, 0);
      this.rowIndexStride = fileMetadata.getRowIndexStride();
      this.contentLength = fileMetadata.getContentLength();
      this.numberOfRows = fileMetadata.getNumberOfRows();
      this.fileStats = fileMetadata.getFileStats();
      this.stripes = fileMetadata.getStripes();
      this.tail = null;
      this.userMetadata = null; // not cached and not needed here
      // FileMetadata is obsolete and doesn't support encryption
      this.encryption = new ReaderEncryption();
      this.softwareVersion = null;
    } else {
      OrcTail orcTail = options.getOrcTail();
      if (orcTail == null) {
        tail = extractFileTail(getFileSystem(), path, options.getMaxLength());
        options.orcTail(tail);
      } else {
        checkOrcVersion(path, orcTail.getPostScript());
        tail = orcTail;
      }
      this.compressionKind = tail.getCompressionKind();
      this.bufferSize = tail.getCompressionBufferSize();
      this.metadataSize = tail.getMetadataSize();
      this.versionList = tail.getPostScript().getVersionList();
      this.schema = tail.getSchema();
      this.rowIndexStride = tail.getFooter().getRowIndexStride();
      this.contentLength = tail.getFooter().getContentLength();
      this.numberOfRows = tail.getFooter().getNumberOfRows();
      this.userMetadata = tail.getFooter().getMetadataList();
      this.fileStats = tail.getFooter().getStatisticsList();
      this.writerVersion = tail.getWriterVersion();
      this.stripes = tail.getStripes();
      this.stripeStatistics = null;
      OrcProto.Footer footer = tail.getFooter();
      this.encryption = new ReaderEncryption(footer, schema,
          tail.getStripeStatisticsOffset(), tail.getTailBuffer(), stripes,
          options.getKeyProvider(), conf);
      this.softwareVersion = OrcUtils.getSoftwareVersion(footer.getWriter(),
          footer.getSoftwareVersion());
    }
    this.types = OrcUtils.getOrcTypes(schema);
  }

  protected FileSystem getFileSystem() throws IOException {
    FileSystem fileSystem = options.getFilesystem();
    if (fileSystem == null) {
      fileSystem = path.getFileSystem(options.getConfiguration());
      options.filesystem(fileSystem);
    }
    return fileSystem;
  }

  protected Supplier<FileSystem> getFileSystemSupplier() {
    return () -> {
      try {
        return getFileSystem();
      } catch (IOException e) {
        throw new RuntimeException("Can't create filesystem", e);
      }
    };
  }

  /**
   * Get the WriterVersion based on the ORC file postscript.
   * @param writerVersion the integer writer version
   * @return the version of the software that produced the file
   */
  public static OrcFile.WriterVersion getWriterVersion(int writerVersion) {
    for(OrcFile.WriterVersion version: OrcFile.WriterVersion.values()) {
      if (version.getId() == writerVersion) {
        return version;
      }
    }
    return OrcFile.WriterVersion.FUTURE;
  }

  public static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
      int metadataSize, InStream.StreamOptions options) throws IOException {
    bb.position(metadataAbsPos);
    bb.limit(metadataAbsPos + metadataSize);
    return OrcProto.Metadata.parseFrom(InStream.createCodedInputStream(
        InStream.create("metadata", new BufferChunk(bb, 0), 0, metadataSize, options)));
  }

  private static OrcProto.PostScript extractPostScript(BufferChunk buffer,
                                                       Path path,
                                                       int psLen,
                                                       long psOffset
                                                       ) throws IOException {
    CodedInputStream in = InStream.createCodedInputStream(
        InStream.create("ps", buffer, psOffset, psLen));
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    checkOrcVersion(path, ps);

    // Check compression codec.
    switch (ps.getCompression()) {
      case NONE:
      case ZLIB:
      case SNAPPY:
      case LZO:
      case LZ4:
      case ZSTD:
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    return ps;
  }

  /**
   * Build a virtual OrcTail for empty files.
   * @return a new OrcTail
   */
  OrcTail buildEmptyTail() throws IOException {
    OrcProto.PostScript.Builder postscript = OrcProto.PostScript.newBuilder();
    OrcFile.Version version = OrcFile.Version.CURRENT;
    postscript.setMagic(OrcFile.MAGIC)
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(0)
        .addVersion(version.getMajor())
        .addVersion(version.getMinor())
        .setMetadataLength(0)
        .setWriterVersion(OrcFile.CURRENT_WRITER.getId());

    // Use a struct with no fields
    OrcProto.Type.Builder struct = OrcProto.Type.newBuilder();
    struct.setKind(OrcProto.Type.Kind.STRUCT);

    OrcProto.Footer.Builder footer = OrcProto.Footer.newBuilder();
    footer.setHeaderLength(0)
          .setContentLength(0)
          .addTypes(struct)
          .setNumberOfRows(0)
          .setRowIndexStride(0);

    OrcProto.FileTail.Builder result = OrcProto.FileTail.newBuilder();
    result.setFooter(footer);
    result.setPostscript(postscript);
    result.setFileLength(0);
    result.setPostscriptLength(0);
    return new OrcTail(result.build(), new BufferChunk(0, 0), -1, this);
  }

  private static void read(FSDataInputStream file,
                           BufferChunk chunks) throws IOException {
    while (chunks != null) {
      if (!chunks.hasData()) {
        int len = chunks.getLength();
        ByteBuffer bb = ByteBuffer.allocate(len);
        file.readFully(chunks.getOffset(), bb.array(), bb.arrayOffset(), len);
        chunks.setChunk(bb);
      }
      chunks = (BufferChunk) chunks.next;
    }
  }

  /**
   * @deprecated Use {@link ReaderImpl#extractFileTail(FileSystem, Path, long)} instead.
   * This is for backward compatibility.
   */
  public static OrcTail extractFileTail(ByteBuffer buffer)
      throws IOException {
    return extractFileTail(buffer, -1,-1);
  }

  /**
   * Read compression block size from the postscript if it is set; otherwise,
   * use the same 256k default the C++ implementation uses.
   */
  public static int getCompressionBlockSize(OrcProto.PostScript postScript) {
    if (postScript.hasCompressionBlockSize()) {
      return (int) postScript.getCompressionBlockSize();
    } else {
      return DEFAULT_COMPRESSION_BLOCK_SIZE;
    }
  }

  /**
   * @deprecated Use {@link ReaderImpl#extractFileTail(FileSystem, Path, long)} instead.
   * This is for backward compatibility.
   */
  public static OrcTail extractFileTail(ByteBuffer buffer, long fileLen, long modificationTime)
      throws IOException {
    OrcProto.PostScript ps;
    long readSize = buffer.limit();
    OrcProto.FileTail.Builder fileTailBuilder = OrcProto.FileTail.newBuilder();
    fileTailBuilder.setFileLength(fileLen != -1 ? fileLen : readSize);

    int psLen = buffer.get((int) (readSize - 1)) & 0xff;
    int psOffset = (int) (readSize - 1 - psLen);
    ensureOrcFooter(buffer, psLen);
    byte[] psBuffer = new byte[psLen];
    System.arraycopy(buffer.array(), psOffset, psBuffer, 0, psLen);

    ps = OrcProto.PostScript.parseFrom(psBuffer);
    int footerSize = (int) ps.getFooterLength();
    CompressionKind compressionKind =
        CompressionKind.valueOf(ps.getCompression().name());
    fileTailBuilder.setPostscriptLength(psLen).setPostscript(ps);

    InStream.StreamOptions compression = new InStream.StreamOptions();
    try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)){
      if (codec != null) {
        compression.withCodec(codec)
            .withBufferSize(getCompressionBlockSize(ps));
      }

      OrcProto.Footer footer =
          OrcProto.Footer.parseFrom(
              InStream.createCodedInputStream(
                  InStream.create("footer", new BufferChunk(buffer, 0),
                                  psOffset - footerSize, footerSize, compression)));
      fileTailBuilder.setPostscriptLength(psLen).setFooter(footer);
    }
    // clear does not clear the contents but sets position to 0 and limit = capacity
    buffer.clear();
    return new OrcTail(fileTailBuilder.build(),
        new BufferChunk(buffer.slice(), 0), modificationTime);
  }

  protected OrcTail extractFileTail(FileSystem fs, Path path,
      long maxFileLength) throws IOException {
    BufferChunk buffer;
    OrcProto.PostScript ps;
    OrcProto.FileTail.Builder fileTailBuilder = OrcProto.FileTail.newBuilder();
    long modificationTime;
    file = fs.open(path);
    try {
      // figure out the size of the file using the option or filesystem
      long size;
      if (maxFileLength == Long.MAX_VALUE) {
        FileStatus fileStatus = fs.getFileStatus(path);
        size = fileStatus.getLen();
        modificationTime = fileStatus.getModificationTime();
      } else {
        size = maxFileLength;
        modificationTime = -1;
      }
      if (size == 0) {
        // Hive often creates empty files (including ORC) and has an
        // optimization to create a 0 byte file as an empty ORC file.
        return buildEmptyTail();
      } else if (size <= OrcFile.MAGIC.length()) {
        // Anything smaller than MAGIC header cannot be valid (valid ORC files
        // are actually around 40 bytes, this is more conservative)
        throw new FileFormatException("Not a valid ORC file " + path
                                          + " (maxFileLength= " + maxFileLength + ")");
      }
      fileTailBuilder.setFileLength(size);

      //read last bytes into buffer to get PostScript
      int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
      buffer = new BufferChunk(size - readSize, readSize);
      read(file, buffer);

      //read the PostScript
      //get length of PostScript
      ByteBuffer bb = buffer.getData();
      int psLen = bb.get(readSize - 1) & 0xff;
      ensureOrcFooter(file, path, psLen, bb);
      long psOffset = size - 1 - psLen;
      ps = extractPostScript(buffer, path, psLen, psOffset);
      CompressionKind compressionKind =
          CompressionKind.valueOf(ps.getCompression().name());
      fileTailBuilder.setPostscriptLength(psLen).setPostscript(ps);

      int footerSize = (int) ps.getFooterLength();
      int metadataSize = (int) ps.getMetadataLength();
      int stripeStatSize = (int) ps.getStripeStatisticsLength();

      //check if extra bytes need to be read
      int tailSize = 1 + psLen + footerSize + metadataSize + stripeStatSize;
      int extra = Math.max(0, tailSize - readSize);
      if (extra > 0) {
        //more bytes need to be read, seek back to the right place and read extra bytes
        BufferChunk orig = buffer;
        buffer = new BufferChunk(size - tailSize, extra);
        buffer.next = orig;
        orig.prev = buffer;
        read(file, buffer);
      }

      InStream.StreamOptions compression = new InStream.StreamOptions();
      try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
        if (codec != null) {
          compression.withCodec(codec)
              .withBufferSize(getCompressionBlockSize(ps));
        }
        OrcProto.Footer footer =
            OrcProto.Footer.parseFrom(
                InStream.createCodedInputStream(
                    InStream.create("footer", buffer, psOffset - footerSize,
                        footerSize, compression)));
        fileTailBuilder.setFooter(footer);
      }
    } catch (Throwable thr) {
      try {
        close();
      } catch (IOException except) {
        LOG.info("Ignoring secondary exception in close of " + path, except);
      }
      throw thr instanceof IOException ? (IOException) thr :
                new IOException("Problem reading file footer " + path, thr);
    }

    return new OrcTail(fileTailBuilder.build(), buffer, modificationTime, this);
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return tail.getSerializedTail();
  }

  @Override
  public boolean writerUsedProlepticGregorian() {
    OrcProto.Footer footer = tail.getFooter();
    return footer.hasCalendar()
               ? footer.getCalendar() == OrcProto.CalendarKind.PROLEPTIC_GREGORIAN
               : OrcConf.PROLEPTIC_GREGORIAN_DEFAULT.getBoolean(conf);
  }

  @Override
  public boolean getConvertToProlepticGregorian() {
    return options.getConvertToProlepticGregorian();
  }

  @Override
  public Options options() {
    return new Options(conf);
  }

  @Override
  public RecordReader rows() throws IOException {
    return rows(options());
  }

  @Override
  public RecordReader rows(Options options) throws IOException {
    LOG.info("Reading ORC rows from " + path + " with " + options);
    return new RecordReaderImpl(this, options);
  }

  @Override
  public long getRawDataSize() {
    // if the deserializedSize is not computed, then compute it, else
    // return the already computed size. since we are reading from the footer
    // we don't have to compute deserialized size repeatedly
    if (deserializedSize == -1) {
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < fileStats.size(); ++i) {
        indices.add(i);
      }
      deserializedSize = getRawDataSizeFromColIndices(indices);
    }
    return deserializedSize;
  }

  @Override
  public long getRawDataSizeFromColIndices(List<Integer> colIndices) {
    boolean[] include = new boolean[schema.getMaximumId() + 1];
    for(Integer rootId: colIndices) {
      TypeDescription root = schema.findSubtype(rootId);
      for(int c = root.getId(); c <= root.getMaximumId(); ++c) {
        include[c] = true;
      }
    }
    return getRawDataSizeFromColIndices(include, schema, fileStats);
  }

  public static long getRawDataSizeFromColIndices(
      List<Integer> colIndices,
      List<OrcProto.Type> types,
      List<OrcProto.ColumnStatistics> stats)
      throws FileFormatException {
    TypeDescription schema = OrcUtils.convertTypeFromProtobuf(types, 0);
    boolean[] include = new boolean[schema.getMaximumId() + 1];
    for(Integer rootId: colIndices) {
      TypeDescription root = schema.findSubtype(rootId);
      for(int c = root.getId(); c <= root.getMaximumId(); ++c) {
        include[c] = true;
      }
    }
    return getRawDataSizeFromColIndices(include, schema, stats);
  }

  static long getRawDataSizeFromColIndices(boolean[] include,
                                           TypeDescription schema,
                                           List<OrcProto.ColumnStatistics> stats) {
    long result = 0;
    for (int c = schema.getId(); c <= schema.getMaximumId(); ++c) {
      if (include[c]) {
        result += getRawDataSizeOfColumn(schema.findSubtype(c), stats);
      }
    }
    return result;
  }

  private static long getRawDataSizeOfColumn(TypeDescription column,
      List<OrcProto.ColumnStatistics> stats) {
    OrcProto.ColumnStatistics colStat = stats.get(column.getId());
    long numVals = colStat.getNumberOfValues();

    switch (column.getCategory()) {
      case BINARY:
        // old orc format doesn't support binary statistics. checking for binary
        // statistics is not required as protocol buffers takes care of it.
        return colStat.getBinaryStatistics().getSum();
      case STRING:
      case CHAR:
      case VARCHAR:
        // old orc format doesn't support sum for string statistics. checking for
        // existence is not required as protocol buffers takes care of it.

        // ORC strings are deserialized to java strings. so use java data model's
        // string size
        numVals = numVals == 0 ? 1 : numVals;
        int avgStrLen = (int) (colStat.getStringStatistics().getSum() / numVals);
        return numVals * JavaDataModel.get().lengthForStringOfLength(avgStrLen);
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return numVals * JavaDataModel.get().lengthOfTimestamp();
      case DATE:
        return numVals * JavaDataModel.get().lengthOfDate();
      case DECIMAL:
        return numVals * JavaDataModel.get().lengthOfDecimal();
      case DOUBLE:
      case LONG:
        return numVals * JavaDataModel.get().primitive2();
      case FLOAT:
      case INT:
      case SHORT:
      case BOOLEAN:
      case BYTE:
      case STRUCT:
      case UNION:
      case MAP:
      case LIST:
        return numVals * JavaDataModel.get().primitive1();
      default:
        LOG.debug("Unknown primitive category: {}", column.getCategory());
        break;
    }

    return 0;
  }

  @Override
  public long getRawDataSizeOfColumns(List<String> colNames) {
    boolean[] include = new boolean[schema.getMaximumId() + 1];
    for(String name: colNames) {
      TypeDescription sub = schema.findSubtype(name);
      for(int c = sub.getId(); c <= sub.getMaximumId(); ++c) {
        include[c] = true;
      }
    }
    return getRawDataSizeFromColIndices(include, schema, fileStats);
  }

  @Override
  public List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() {
    if (stripeStatistics == null) {
      try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
        InStream.StreamOptions options = new InStream.StreamOptions();
        if (codec != null) {
          options.withCodec(codec).withBufferSize(bufferSize);
        }
        stripeStatistics = deserializeStripeStats(tail.getTailBuffer(),
            tail.getMetadataOffset(), tail.getMetadataSize(), options);
      } catch (IOException ioe) {
        throw new RuntimeException("Can't deserialize stripe stats", ioe);
      }
    }
    return stripeStatistics;
  }

  @Override
  public List<OrcProto.ColumnStatistics> getOrcProtoFileStatistics() {
    return fileStats;
  }

  private static List<OrcProto.StripeStatistics> deserializeStripeStats(
      BufferChunk tailBuffer,
      long offset,
      int length,
      InStream.StreamOptions options) throws IOException {
    try (InStream stream = InStream.create("stripe stats", tailBuffer, offset,
        length, options)) {
      OrcProto.Metadata meta = OrcProto.Metadata.parseFrom(
          InStream.createCodedInputStream(stream));
      return meta.getStripeStatsList();
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Failed to parse stripe statistics", e);
      return Collections.emptyList();
    }
  }

  private List<StripeStatistics> convertFromProto(List<OrcProto.StripeStatistics> list) {
    if (list == null) {
      return null;
    } else {
      List<StripeStatistics> result = new ArrayList<>(list.size());
      for (OrcProto.StripeStatistics ss : stripeStatistics) {
        result.add(new StripeStatisticsImpl(schema,
            new ArrayList<>(ss.getColStatsList()), writerUsedProlepticGregorian(),
            getConvertToProlepticGregorian()));
      }
      return result;
    }
  }

  @Override
  public List<StripeStatistics> getStripeStatistics() throws IOException {
    return getStripeStatistics(null);
  }

  @Override
  public List<StripeStatistics> getStripeStatistics(boolean[] included) throws IOException {
    List<StripeStatistics> result = convertFromProto(stripeStatistics);
    if (result == null || encryption.getVariants().length > 0) {
      try (CompressionCodec codec = OrcCodecPool.getCodec(compressionKind)) {
        InStream.StreamOptions options = new InStream.StreamOptions();
        if (codec != null) {
          options.withCodec(codec).withBufferSize(bufferSize);
        }

        result = getVariantStripeStatistics(null);

        if (encryption.getVariants().length > 0) {
          // process any encrypted overrides that we have the key for
          for (int c = schema.getId(); c <= schema.getMaximumId(); ++c) {
            // only decrypt the variants that we need
            if (included == null || included[c]) {
              ReaderEncryptionVariant variant = encryption.getVariant(c);
              if (variant != null) {
                TypeDescription variantType = variant.getRoot();
                List<StripeStatistics> colStats =
                    variant.getStripeStatistics(included, options, this);
                for(int sub = c; sub <= variantType.getMaximumId(); ++sub) {
                  if (included == null || included[sub]) {
                    for(int s = 0; s < colStats.size(); ++s) {
                      StripeStatisticsImpl resultElem = (StripeStatisticsImpl) result.get(s);
                      resultElem.updateColumn(sub,
                          colStats.get(s).getColumn(sub - variantType.getId()));
                    }
                  }
                }
                c = variantType.getMaximumId();
              }
            }
          }
        }
      }
    }
    return result;
  }

  @Override
  public List<Integer> getVersionList() {
    return versionList;
  }

  @Override
  public int getMetadataSize() {
    return metadataSize;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("ORC Reader(");
    buffer.append(path);
    if (maxLength != -1) {
      buffer.append(", ");
      buffer.append(maxLength);
    }
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public void close() throws IOException {
    if (file != null) {
      file.close();
    }
  }

  /**
   * Take the file from the reader.
   * This allows the first RecordReader to use the same file, but additional
   * RecordReaders will open new handles.
   * @return a file handle, if one is available
   */
  public FSDataInputStream takeFile() {
    FSDataInputStream result = file;
    file = null;
    return result;
  }
}
