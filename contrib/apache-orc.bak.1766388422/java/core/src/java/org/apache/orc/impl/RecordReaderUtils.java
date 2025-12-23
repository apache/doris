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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Stateless methods shared between RecordReaderImpl and EncodedReaderImpl.
 */
public class RecordReaderUtils {
  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  private static final Logger LOG = LoggerFactory.getLogger(RecordReaderUtils.class);

  private static class DefaultDataReader implements DataReader {
    private FSDataInputStream file;
    private ByteBufferAllocatorPool pool;
    private HadoopShims.ZeroCopyReaderShim zcr = null;
    private final Supplier<FileSystem> fileSystemSupplier;
    private final Path path;
    private final boolean useZeroCopy;
    private final int minSeekSize;
    private final double minSeekSizeTolerance;
    private InStream.StreamOptions options;
    private boolean isOpen = false;

    private DefaultDataReader(DataReaderProperties properties) {
      this.fileSystemSupplier = properties.getFileSystemSupplier();
      this.path = properties.getPath();
      this.file = properties.getFile();
      this.useZeroCopy = properties.getZeroCopy();
      this.options = properties.getCompression();
      this.minSeekSize = properties.getMinSeekSize();
      this.minSeekSizeTolerance = properties.getMinSeekSizeTolerance();
    }

    @Override
    public void open() throws IOException {
      if (file == null) {
        this.file = fileSystemSupplier.get().open(path);
      }
      if (useZeroCopy) {
        // ZCR only uses codec for boolean checks.
        pool = new ByteBufferAllocatorPool();
        zcr = RecordReaderUtils.createZeroCopyShim(file, options.getCodec(), pool);
      } else {
        zcr = null;
      }
      isOpen = true;
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      if (!isOpen) {
        open();
      }
      long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
      int tailLength = (int) stripe.getFooterLength();

      // read the footer
      ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
      file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
      return OrcProto.StripeFooter.parseFrom(
          InStream.createCodedInputStream(InStream.create("footer",
              new BufferChunk(tailBuf, 0), 0, tailLength, options)));
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList range,
                                        boolean doForceDirect
                                        ) throws IOException {
      RecordReaderUtils.readDiskRanges(file, zcr, range, doForceDirect, minSeekSize,
                                       minSeekSizeTolerance);
      return range;
    }

    @Override
    public void close() throws IOException {
      if (options.getCodec() != null) {
        OrcCodecPool.returnCodec(options.getCodec().getKind(), options.getCodec());
        options.withCodec(null);
      }
      if (pool != null) {
        pool.clear();
      }
      // close both zcr and file
      try (HadoopShims.ZeroCopyReaderShim myZcr = zcr) {
        if (file != null) {
          file.close();
          file = null;
        }
      }
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return zcr != null;
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      zcr.releaseBuffer(buffer);
    }

    @Override
    public DataReader clone() {
      if (this.file != null) {
        // We should really throw here, but that will cause failures in Hive.
        // While Hive uses clone, just log a warning.
        LOG.warn("Cloning an opened DataReader; the stream will be reused and closed twice");
      }
      try {
        DefaultDataReader clone = (DefaultDataReader) super.clone();
        if (options.getCodec() != null) {
          // Make sure we don't share the same codec between two readers.
          clone.options = options.clone();
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    @Override
    public InStream.StreamOptions getCompressionOptions() {
      return options;
    }
  }

  public static DataReader createDefaultDataReader(DataReaderProperties properties) {
    return new DefaultDataReader(properties);
  }

  /**
   * Does region A overlap region B? The end points are inclusive on both sides.
   * @param leftA A's left point
   * @param rightA A's right point
   * @param leftB B's left point
   * @param rightB B's right point
   * @return Does region A overlap region B?
   */
  static boolean overlap(long leftA, long rightA, long leftB, long rightB) {
    if (leftA <= leftB) {
      return rightA >= leftB;
    }
    return rightB >= leftA;
  }

  public static long estimateRgEndOffset(boolean isCompressed,
                                         int bufferSize,
                                         boolean isLast,
                                         long nextGroupOffset,
                                         long streamLength) {
    // Figure out the worst case last location
    long slop = WORST_UNCOMPRESSED_SLOP;
    // Stretch the slop by a factor to safely accommodate following compression blocks.
    // We need to calculate the maximum number of blocks(stretchFactor) by bufferSize accordingly.
    if (isCompressed) {
      int stretchFactor = 2 + (MAX_VALUES_LENGTH * MAX_BYTE_WIDTH - 1) / bufferSize;
      slop = stretchFactor * (OutStream.HEADER_SIZE + bufferSize);
    }
    return isLast ? streamLength : Math.min(streamLength, nextGroupOffset + slop);
  }

  private static final int BYTE_STREAM_POSITIONS = 1;
  private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
  private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
  private static final int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;

  /**
   * Get the offset in the index positions for the column that the given
   * stream starts.
   * @param columnEncoding the encoding of the column
   * @param columnType the type of the column
   * @param streamType the kind of the stream
   * @param isCompressed is the stream compressed?
   * @param hasNulls does the column have a PRESENT stream?
   * @return the number of positions that will be used for that stream
   */
  public static int getIndexPosition(OrcProto.ColumnEncoding.Kind columnEncoding,
                              TypeDescription.Category columnType,
                              OrcProto.Stream.Kind streamType,
                              boolean isCompressed,
                              boolean hasNulls) {
    if (streamType == OrcProto.Stream.Kind.PRESENT) {
      return 0;
    }
    int compressionValue = isCompressed ? 1 : 0;
    int base = hasNulls ? (BITFIELD_POSITIONS + compressionValue) : 0;
    switch (columnType) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
        return base;
      case CHAR:
      case VARCHAR:
      case STRING:
        if (columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          return base;
        } else {
          if (streamType == OrcProto.Stream.Kind.DATA) {
            return base;
          } else {
            return base + BYTE_STREAM_POSITIONS + compressionValue;
          }
        }
      case BINARY:
      case DECIMAL:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + RUN_LENGTH_INT_POSITIONS + compressionValue;
      default:
        throw new IllegalArgumentException("Unknown type " + columnType);
    }
  }

  // for uncompressed streams, what is the most overlap with the following set
  // of rows (long vint literal group).
  static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;
  // the maximum number of values that need to be consumed from the run
  static final int MAX_VALUES_LENGTH = RunLengthIntegerWriterV2.MAX_SCOPE;
  // the maximum byte width for each value
  static final int MAX_BYTE_WIDTH =
      SerializationUtils.decodeBitWidth(SerializationUtils.FixedBitSizes.SIXTYFOUR.ordinal()) / 8;

  /**
   * Is this stream part of a dictionary?
   * @return is this part of a dictionary?
   */
  public static boolean isDictionary(OrcProto.Stream.Kind kind,
                              OrcProto.ColumnEncoding encoding) {
    assert kind != OrcProto.Stream.Kind.DICTIONARY_COUNT;
    OrcProto.ColumnEncoding.Kind encodingKind = encoding.getKind();
    return kind == OrcProto.Stream.Kind.DICTIONARY_DATA ||
      (kind == OrcProto.Stream.Kind.LENGTH &&
       (encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
        encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2));
  }

  /**
   * Build a string representation of a list of disk ranges.
   * @param range ranges to stringify
   * @return the resulting string
   */
  public static String stringifyDiskRanges(DiskRangeList range) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    boolean isFirst = true;
    while (range != null) {
      if (!isFirst) {
        buffer.append(", {");
      } else {
        buffer.append("{");
      }
      isFirst = false;
      buffer.append(range);
      buffer.append("}");
      range = range.next;
    }
    buffer.append("]");
    return buffer.toString();
  }

  static long computeEnd(BufferChunk first, BufferChunk last) {
    long end = 0;
    for(BufferChunk ptr=first; ptr != last.next; ptr = (BufferChunk) ptr.next) {
      end = Math.max(ptr.getEnd(), end);
    }
    return end;
  }

  /**
   * Zero-copy read the data from the file based on a list of ranges in a
   * single read.
   *
   * As a side note, the HDFS zero copy API really sucks from a user's point of
   * view.
   *
   * @param file the file we're reading from
   * @param zcr the zero copy shim
   * @param first the first range to read
   * @param last the last range to read
   * @param allocateDirect if we need to allocate buffers, should we use direct
   * @throws IOException
   */
  static void zeroCopyReadRanges(FSDataInputStream file,
                                 HadoopShims.ZeroCopyReaderShim zcr,
                                 BufferChunk first,
                                 BufferChunk last,
                                 boolean allocateDirect) throws IOException {
    // read all of the bytes that we need
    final long offset = first.getOffset();
    int length = (int)(computeEnd(first, last) - offset);
    file.seek(offset);
    List<ByteBuffer> bytes = new ArrayList<>();
    while (length > 0) {
      ByteBuffer read= zcr.readBuffer(length, false);
      bytes.add(read);
      length -= read.remaining();
    }
    long currentOffset = offset;

    // iterate and fill each range
    BufferChunk current = first;
    Iterator<ByteBuffer> buffers = bytes.iterator();
    ByteBuffer currentBuffer = buffers.next();
    while (current != last.next) {

      // if we are past the start of the range, restart the iterator
      if (current.getOffset() < offset) {
        buffers = bytes.iterator();
        currentBuffer = buffers.next();
        currentOffset = offset;
      }

      // walk through the buffers to find the start of the buffer
      while (currentOffset + currentBuffer.remaining() <= current.getOffset()) {
        currentOffset += currentBuffer.remaining();
        // We assume that buffers.hasNext is true because we know we read
        // enough data to cover the last range.
        currentBuffer = buffers.next();
      }

      // did we get the current range in a single read?
      if (currentOffset + currentBuffer.remaining() >= current.getEnd()) {
        ByteBuffer copy = currentBuffer.duplicate();
        copy.position((int) (current.getOffset() - currentOffset));
        copy.limit(copy.position() + current.getLength());
        current.setChunk(copy);

      } else {
        // otherwise, build a single buffer that holds the entire range
        ByteBuffer result = allocateDirect
                              ? ByteBuffer.allocateDirect(current.getLength())
                              : ByteBuffer.allocate(current.getLength());
        // we know that the range spans buffers
        ByteBuffer copy = currentBuffer.duplicate();
        // skip over the front matter
        copy.position((int) (current.getOffset() - currentOffset));
        result.put(copy);
        // advance the buffer
        currentOffset += currentBuffer.remaining();
        currentBuffer = buffers.next();
        while (result.hasRemaining()) {
          if (result.remaining() > currentBuffer.remaining()) {
            result.put(currentBuffer.duplicate());
            currentOffset += currentBuffer.remaining();
            currentBuffer = buffers.next();
          } else {
            copy = currentBuffer.duplicate();
            copy.limit(result.remaining());
            result.put(copy);
          }
        }
        result.flip();
        current.setChunk(result);
      }
      current = (BufferChunk) current.next;
    }
  }

  /**
   * Read the data from the file based on a list of ranges in a single read.
   * @param file the file to read from
   * @param first the first range to read
   * @param last the last range to read
   * @param allocateDirect should we use direct buffers
   */
  static void readRanges(FSDataInputStream file,
                         BufferChunk first,
                         BufferChunk last,
                         boolean allocateDirect) throws IOException {
    // assume that the chunks are sorted by offset
    long offset = first.getOffset();
    int readSize = (int) (computeEnd(first, last) - offset);
    byte[] buffer = new byte[readSize];
    try {
      file.readFully(offset, buffer, 0, buffer.length);
    } catch(IOException e) {
      throw new IOException(String.format("Failed while reading %s %d:%d",
                                          file,
                                          offset,
                                          buffer.length),
                            e);
    }

    // get the data into a ByteBuffer
    ByteBuffer bytes;
    if (allocateDirect) {
      bytes = ByteBuffer.allocateDirect(readSize);
      bytes.put(buffer);
      bytes.flip();
    } else {
      bytes = ByteBuffer.wrap(buffer);
    }

    // populate each BufferChunks with the data
    BufferChunk current = first;
    while (current != last.next) {
      ByteBuffer currentBytes = current == last ? bytes : bytes.duplicate();
      currentBytes.position((int) (current.getOffset() - offset));
      currentBytes.limit((int) (current.getEnd() - offset));
      current.setChunk(currentBytes);
      current = (BufferChunk) current.next;
    }
  }

  /**
   * Find the list of ranges that should be read in a single read.
   * The read will stop when there is a gap, one of the ranges already has data,
   * or we have reached the maximum read size of 2^31.
   * @param first the first range to read
   * @return the last range to read
   */
  static BufferChunk findSingleRead(BufferChunk first) {
    return findSingleRead(first, 0);
  }

  /**
   * Find the list of ranges that should be read in a single read.
   * The read will stop when there is a gap, one of the ranges already has data,
   * or we have reached the maximum read size of 2^31.
   * @param first the first range to read
   * @param minSeekSize minimum size for seek instead of read
   * @return the last range to read
   */
  private static BufferChunk findSingleRead(BufferChunk first, long minSeekSize) {
    BufferChunk last = first;
    long currentEnd = first.getEnd();
    while (last.next != null &&
               !last.next.hasData() &&
               last.next.getOffset() <= (currentEnd + minSeekSize) &&
               last.next.getEnd() - first.getOffset() < Integer.MAX_VALUE) {
      last = (BufferChunk) last.next;
      currentEnd = Math.max(currentEnd, last.getEnd());
    }
    return last;
  }

  /**
   * Read the list of ranges from the file by updating each range in the list
   * with a buffer that has the bytes from the file.
   *
   * The ranges must be sorted, but may overlap or include holes.
   *
   * @param file the file to read
   * @param zcr the zero copy shim
   * @param list the disk ranges within the file to read
   * @param doForceDirect allocate direct buffers
   */
  static void readDiskRanges(FSDataInputStream file,
                             HadoopShims.ZeroCopyReaderShim zcr,
                             BufferChunkList list,
                             boolean doForceDirect) throws IOException {
    readDiskRanges(file, zcr, list, doForceDirect, 0, 0);
  }

  /**
   * Read the list of ranges from the file by updating each range in the list
   * with a buffer that has the bytes from the file.
   *
   * The ranges must be sorted, but may overlap or include holes.
   *
   * @param file the file to read
   * @param zcr the zero copy shim
   * @param list the disk ranges within the file to read
   * @param doForceDirect allocate direct buffers
   * @param minSeekSize the minimum gap to prefer seek vs read
   * @param minSeekSizeTolerance allowed tolerance for extra bytes in memory as a result of
   *                             minSeekSize
   */
  private static void readDiskRanges(FSDataInputStream file,
                             HadoopShims.ZeroCopyReaderShim zcr,
                             BufferChunkList list,
                             boolean doForceDirect,
                             int minSeekSize,
                             double minSeekSizeTolerance) throws IOException {
    BufferChunk current = list == null ? null : list.get();
    while (current != null) {
      while (current.hasData()) {
        current = (BufferChunk) current.next;
      }
      if (zcr != null) {
        BufferChunk last = findSingleRead(current);
        zeroCopyReadRanges(file, zcr, current, last, doForceDirect);
        current = (BufferChunk) last.next;
      } else {
        ChunkReader chunkReader = ChunkReader.create(current, minSeekSize);
        chunkReader.readRanges(file, doForceDirect, minSeekSizeTolerance);
        current = (BufferChunk) chunkReader.to.next;
      }
    }
  }

  static HadoopShims.ZeroCopyReaderShim createZeroCopyShim(FSDataInputStream file,
      CompressionCodec codec, ByteBufferAllocatorPool pool) throws IOException {
    if ((codec == null || ((codec instanceof DirectDecompressionCodec) &&
        ((DirectDecompressionCodec) codec).isAvailable()))) {
      /* codec is null or is available */
      return SHIMS.getZeroCopyReader(file, pool);
    }
    return null;
  }

  // this is an implementation copied from ElasticByteBufferPool in hadoop-2,
  // which lacks a clear()/clean() operation
  public static final class ByteBufferAllocatorPool implements HadoopShims.ByteBufferPoolShim {
    private static final class Key implements Comparable<Key> {
      private final int capacity;
      private final long insertionGeneration;

      Key(int capacity, long insertionGeneration) {
        this.capacity = capacity;
        this.insertionGeneration = insertionGeneration;
      }

      @Override
      public int compareTo(Key other) {
        final int c = Integer.compare(capacity, other.capacity);
        return (c != 0) ? c : Long.compare(insertionGeneration, other.insertionGeneration);
      }

      @Override
      public boolean equals(Object rhs) {
        if (rhs instanceof Key) {
          Key o = (Key) rhs;
          return 0 == compareTo(o);
        }
        return false;
      }

      @Override
      public int hashCode() {
        return new HashCodeBuilder().append(capacity).append(insertionGeneration)
            .toHashCode();
      }
    }

    private final TreeMap<Key, ByteBuffer> buffers = new TreeMap<>();

    private final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<>();

    private long currentGeneration = 0;

    private TreeMap<Key, ByteBuffer> getBufferTree(boolean direct) {
      return direct ? directBuffers : buffers;
    }

    public void clear() {
      buffers.clear();
      directBuffers.clear();
    }

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      TreeMap<Key, ByteBuffer> tree = getBufferTree(direct);
      Map.Entry<Key, ByteBuffer> entry = tree.ceilingEntry(new Key(length, 0));
      if (entry == null) {
        return direct ? ByteBuffer.allocateDirect(length) : ByteBuffer
            .allocate(length);
      }
      tree.remove(entry.getKey());
      return entry.getValue();
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      TreeMap<Key, ByteBuffer> tree = getBufferTree(buffer.isDirect());
      Key key;

      // Buffers are indexed by (capacity, generation).
      // If our key is not unique on the first try, try again
      do {
        key = new Key(buffer.capacity(), currentGeneration++);
      } while (tree.putIfAbsent(key, buffer) != null);
    }
  }

  static class ChunkReader {
    private final BufferChunk from;
    private final BufferChunk to;
    private final int readBytes;
    private final int reqBytes;

    private ChunkReader(BufferChunk from, BufferChunk to, int readSize, int reqBytes) {
      this.from = from;
      this.to = to;
      this.readBytes = readSize;
      this.reqBytes = reqBytes;
    }

    double getExtraBytesFraction() {
      return (readBytes - reqBytes) / ((double) reqBytes);
    }

    public int getReadBytes() {
      return readBytes;
    }

    public int getReqBytes() {
      return reqBytes;
    }

    public BufferChunk getFrom() {
      return from;
    }

    public BufferChunk getTo() {
      return to;
    }

    void populateChunks(ByteBuffer bytes, boolean allocateDirect, double extraByteTolerance) {
      if (getExtraBytesFraction() > extraByteTolerance) {
        LOG.debug("ExtraBytesFraction = {}, ExtraByteTolerance = {}, reducing memory size",
                  getExtraBytesFraction(),
                  extraByteTolerance);
        populateChunksReduceSize(bytes, allocateDirect);
      } else {
        LOG.debug("ExtraBytesFraction = {}, ExtraByteTolerance = {}, populating as is",
                  getExtraBytesFraction(),
                  extraByteTolerance);
        populateChunksAsIs(bytes);
      }
    }

    void populateChunksAsIs(ByteBuffer bytes) {
      // populate each BufferChunks with the data
      BufferChunk current = from;
      long offset = from.getOffset();
      while (current != to.next) {
        ByteBuffer currentBytes = current == to ? bytes : bytes.duplicate();
        currentBytes.position((int) (current.getOffset() - offset));
        currentBytes.limit((int) (current.getEnd() - offset));
        current.setChunk(currentBytes);
        current = (BufferChunk) current.next;
      }
    }

    void populateChunksReduceSize(ByteBuffer bytes, boolean allocateDirect) {
      ByteBuffer newBuffer;
      if (allocateDirect) {
        newBuffer = ByteBuffer.allocateDirect(reqBytes);
        newBuffer.position(reqBytes);
        newBuffer.flip();
      } else {
        byte[] newBytes = new byte[reqBytes];
        newBuffer = ByteBuffer.wrap(newBytes);
      }

      final long offset = from.getOffset();
      int copyStart = 0;
      int copyEnd;
      int copyLength;
      int skippedBytes = 0;
      int srcPosition;
      BufferChunk current = from;
      while (current != to.next) {
        // We can skip bytes as required, but no need to copy bytes that are already copied
        srcPosition = (int) (current.getOffset() - offset);
        skippedBytes += Math.max(0, srcPosition - copyStart);
        copyStart = Math.max(copyStart, srcPosition);
        copyEnd = (int) (current.getEnd() - offset);
        copyLength = copyStart < copyEnd ? copyEnd - copyStart : 0;
        newBuffer.put(bytes.array(), copyStart, copyLength);
        copyStart += copyLength;
        // Set up new ByteBuffer that wraps on the same backing array
        ByteBuffer currentBytes = current == to ? newBuffer : newBuffer.duplicate();
        currentBytes.position(srcPosition - skippedBytes);
        currentBytes.limit(currentBytes.position() + current.getLength());
        current.setChunk(currentBytes);
        current = (BufferChunk) current.next;
      }
    }

    /**
     * Read the data from the file based on a list of ranges in a single read.
     *
     * @param file           the file to read from
     * @param allocateDirect should we use direct buffers
     */
    void readRanges(FSDataInputStream file, boolean allocateDirect, double extraByteTolerance)
      throws IOException {
      // assume that the chunks are sorted by offset
      long offset = from.getOffset();
      int readSize = (int) (computeEnd(from, to) - offset);
      byte[] buffer = new byte[readSize];
      try {
        file.readFully(offset, buffer, 0, buffer.length);
      } catch (IOException e) {
        throw new IOException(String.format("Failed while reading %s %d:%d",
                                            file,
                                            offset,
                                            buffer.length),
                              e);
      }

      // get the data into a ByteBuffer
      ByteBuffer bytes;
      if (allocateDirect) {
        bytes = ByteBuffer.allocateDirect(readSize);
        bytes.put(buffer);
        bytes.flip();
      } else {
        bytes = ByteBuffer.wrap(buffer);
      }

      // populate each BufferChunks with the data
      populateChunks(bytes, allocateDirect, extraByteTolerance);
    }

    static ChunkReader create(BufferChunk from, BufferChunk to) {
      long f = Integer.MAX_VALUE;
      long e = Integer.MIN_VALUE;

      long cf = Integer.MAX_VALUE;
      long ef = Integer.MIN_VALUE;
      int reqBytes = 0;

      BufferChunk current = from;
      while (current != to.next) {
        f = Math.min(f, current.getOffset());
        e = Math.max(e, current.getEnd());
        if (ef == Integer.MIN_VALUE || current.getOffset() <= ef) {
          cf = Math.min(cf, current.getOffset());
          ef = Math.max(ef, current.getEnd());
        } else {
          reqBytes += ef - cf;
          cf = current.getOffset();
          ef = current.getEnd();
        }
        current = (BufferChunk) current.next;
      }
      reqBytes += ef - cf;
      return new ChunkReader(from, to, (int) (e - f), reqBytes);
    }

    static ChunkReader create(BufferChunk from, int minSeekSize) {
      BufferChunk to = findSingleRead(from, minSeekSize);
      return create(from, to);
    }
  }
}
