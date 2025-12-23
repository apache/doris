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
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.util.function.Consumer;

public abstract class InStream extends InputStream {

  private static final Logger LOG = LoggerFactory.getLogger(InStream.class);
  public static final int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20; // 1GB

  protected final Object name;
  protected final long offset;
  protected long length;
  protected DiskRangeList bytes;
  // position in the stream (0..length)
  protected long position;

  public InStream(Object name, long offset, long length) {
    this.name = name;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public String toString() {
    return name.toString();
  }

  @Override
  public abstract void close();

  /**
   * Set the current range
   * @param newRange the block that is current
   * @param isJump if this was a seek instead of a natural read
   */
  protected abstract void setCurrent(DiskRangeList newRange,
                                     boolean isJump);
  /**
   * Reset the input to a new set of data.
   * @param input the input data
   */
  protected void reset(DiskRangeList input) {
    bytes = input;
    while (input != null &&
               (input.getEnd() <= offset ||
                    input.getOffset() > offset + length)) {
      input = input.next;
    }
    if (input == null || input.getOffset() <= offset) {
      position = 0;
    } else {
      position = input.getOffset() - offset;
    }
    setCurrent(input, true);
  }

  /**
   * Reset the input to a new set of data with a different length.
   *
   * in some cases, after resetting an UncompressedStream, its actual length is longer than its initial length.
   * Prior to ORC-516, InStream.UncompressedStream class had the 'length' field and the length was modifiable in
   * the reset() method. It was used in SettableUncompressedStream class in setBuffers() method.
   * SettableUncompressedStream was passing 'diskRangeInfo.getTotalLength()' as the length to the reset() method.
   * SettableUncompressedStream had been removed from ORC code base, but it is required for Apache Hive and
   * Apache Hive manages its own copy of SettableUncompressedStream since upgrading its Apache ORC version to 1.6.7.
   * ORC-516 was the root cause of the regression reported in HIVE-27128 - EOFException when reading DATA stream.
   * This wrapper method allows to resolve HIVE-27128.
   *
   * @param input the input data
   * @param length new length of the stream
   */
  protected void reset(DiskRangeList input, long length) {
    this.length = length;
    reset(input);
  }

  public abstract void changeIv(Consumer<byte[]> modifier);

  static int getRangeNumber(DiskRangeList list, DiskRangeList current) {
    int result = 0;
    DiskRangeList range = list;
    while (range != null && range != current) {
      result += 1;
      range = range.next;
    }
    return result;
  }

  /**
   * Implements a stream over an uncompressed stream.
   */
  public static class UncompressedStream extends InStream {
    protected ByteBuffer decrypted;
    protected DiskRangeList currentRange;
    protected long currentOffset;

    /**
     * Create the stream without calling reset on it.
     * This is used for the subclass that needs to do more setup.
     * @param name name of the stream
     * @param length the number of bytes for the stream
     */
    public UncompressedStream(Object name, long offset, long length) {
      super(name, offset, length);
    }

    public UncompressedStream(Object name,
                              DiskRangeList input,
                              long offset,
                              long length) {
      super(name, offset, length);
      reset(input, length);
    }

    @Override
    public int read() {
      if (decrypted == null || decrypted.remaining() == 0) {
        if (position == length) {
          return -1;
        }
        setCurrent(currentRange.next, false);
      }
      position += 1;
      return 0xff & decrypted.get();
    }

    @Override
    protected void setCurrent(DiskRangeList newRange, boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        // copy the buffer so that we don't change the BufferChunk
        decrypted = newRange.getData().slice();
        currentOffset = newRange.getOffset();
        // Move the position in the ByteBuffer to match the currentOffset,
        // which is relative to the stream.
        int start = (int) (position + offset - currentOffset);
        decrypted.position(start);
        // make sure the end of the buffer doesn't go past our stream
        decrypted.limit(start + (int) Math.min(decrypted.remaining(),
            length - position));
      }
    }

    @Override
    public int read(byte[] data, int offset, int length) {
      if (decrypted == null || decrypted.remaining() == 0) {
        if (position == this.length) {
          return -1;
        }
        setCurrent(currentRange.next, false);
      }
      int actualLength = Math.min(length, decrypted.remaining());
      decrypted.get(data, offset, actualLength);
      position += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      if (decrypted != null && decrypted.remaining() > 0) {
        return decrypted.remaining();
      }
      return (int) (length - position);
    }

    @Override
    public void close() {
      currentRange = null;
      position = length;
      // explicit de-ref of bytes[]
      decrypted = null;
      bytes = null;
    }

    @Override
    public void changeIv(Consumer<byte[]> modifier) {
      // nothing to do
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
    }

    public void seek(long desired) throws IOException {
      if (desired == 0 && bytes == null) {
        return;
      }
      // compute the position of the desired point in file
      long positionFile = desired + offset;
      // If we are seeking inside of the current range, just reposition.
      if (currentRange != null && positionFile >= currentRange.getOffset() &&
          positionFile < currentRange.getEnd()) {
        decrypted.position((int) (positionFile - currentOffset));
        position = desired;
      } else {
        for (DiskRangeList curRange = bytes; curRange != null;
             curRange = curRange.next) {
          if (curRange.getOffset() <= positionFile &&
              (curRange.next == null ? positionFile <= curRange.getEnd() :
                   positionFile < curRange.getEnd())) {
            position = desired;
            setCurrent(curRange, true);
            return;
          }
        }
        throw new IllegalArgumentException("Seek in " + name + " to " +
            desired + " is outside of the data");
      }
    }

    @Override
    public String toString() {
      return "uncompressed stream " + name + " position: " + position +
          " length: " + length + " range: " + getRangeNumber(bytes, currentRange) +
          " offset: " + currentRange.getOffset() +
          " position: " + (decrypted == null ? 0 : decrypted.position()) +
          " limit: " + (decrypted == null ? 0 : decrypted.limit());
    }
  }

  private static ByteBuffer allocateBuffer(int size, boolean isDirect) {
    // TODO: use the same pool as the ORC readers
    if (isDirect) {
      return ByteBuffer.allocateDirect(size);
    } else {
      return ByteBuffer.allocate(size);
    }
  }

  /**
   * Manage the state of the decryption, including the ability to seek.
   */
  static class EncryptionState {
    private final Object name;
    private final Key key;
    private final byte[] iv;
    private final Cipher cipher;
    private final long offset;
    private ByteBuffer decrypted;

    EncryptionState(Object name, long offset, StreamOptions options) {
      this.name = name;
      this.offset = offset;
      EncryptionAlgorithm algorithm = options.getAlgorithm();
      key = options.getKey();
      iv = options.getIv();
      cipher = algorithm.createCipher();
    }

    void changeIv(Consumer<byte[]> modifier) {
      modifier.accept(iv);
      updateIv();
      OutStream.logKeyAndIv(name, key, iv);
    }

    private void updateIv() {
      try {
        cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
      } catch (InvalidKeyException e) {
        throw new IllegalArgumentException("Invalid key on " + name, e);
      } catch (InvalidAlgorithmParameterException e) {
        throw new IllegalArgumentException("Invalid iv on " + name, e);
      }
    }

    /**
     * We are seeking to a new range, so update the cipher to change the IV
     * to match. This code assumes that we only support encryption in CTR mode.
     * @param offset where we are seeking to in the stream
     */
    void changeIv(long offset) {
      int blockSize = cipher.getBlockSize();
      long encryptionBlocks = offset / blockSize;
      long extra = offset % blockSize;
      CryptoUtils.clearCounter(iv);
      if (encryptionBlocks != 0) {
        // Add the encryption blocks into the initial iv, to compensate for
        // skipping over decrypting those bytes.
        int posn = iv.length - 1;
        while (encryptionBlocks > 0) {
          long sum = (iv[posn] & 0xff) + encryptionBlocks;
          iv[posn--] = (byte) sum;
          encryptionBlocks =  sum / 0x100;
        }
      }
      updateIv();
      // If the range starts at an offset that doesn't match the encryption
      // block, we need to advance some bytes within an encryption block.
      if (extra > 0) {
        try {
          byte[] wasted = new byte[(int) extra];
          cipher.update(wasted, 0, wasted.length, wasted, 0);
        } catch (ShortBufferException e) {
          throw new IllegalArgumentException("Short buffer in " + name, e);
        }
      }
    }

    /**
     * Decrypt the given range into the decrypted buffer. It is assumed that
     * the cipher is correctly initialized by changeIv before this is called.
     * @param encrypted the bytes to decrypt
     * @return a reused ByteBuffer, which is used by each call to decrypt
     */
    ByteBuffer decrypt(ByteBuffer encrypted)  {
      int length = encrypted.remaining();
      if (decrypted == null || decrypted.capacity() < length) {
        decrypted = ByteBuffer.allocate(length);
      } else {
        decrypted.clear();
      }
      try {
        int output = cipher.update(encrypted, decrypted);
        if (output != length) {
          throw new IllegalArgumentException("Problem decrypting " + name +
              " at " + offset);
        }
      } catch (ShortBufferException e) {
        throw new IllegalArgumentException("Problem decrypting " + name +
            " at " + offset, e);
      }
      decrypted.flip();
      return decrypted;
    }

    void close() {
      decrypted = null;
    }
  }

  /**
   * Implements a stream over an encrypted, but uncompressed stream.
   */
  public static class EncryptedStream extends UncompressedStream {
    private final EncryptionState encrypt;

    public EncryptedStream(Object name, DiskRangeList input, long offset, long length,
                           StreamOptions options) {
      super(name, offset, length);
      encrypt = new EncryptionState(name, offset, options);
      reset(input);
    }

    @Override
    protected void setCurrent(DiskRangeList newRange, boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        // what is the position of the start of the newRange?
        currentOffset = newRange.getOffset();
        ByteBuffer encrypted = newRange.getData().slice();
        if (currentOffset < offset) {
          int ignoreBytes = (int) (offset - currentOffset);
          encrypted.position(ignoreBytes);
          currentOffset = offset;
        }
        if (isJump) {
          encrypt.changeIv(currentOffset - offset);
        }
        if (encrypted.remaining() > length + offset - currentOffset) {
          encrypted.limit((int) (length + offset - currentOffset));
        }
        decrypted = encrypt.decrypt(encrypted);
        decrypted.position((int) (position + offset - currentOffset));
      }
    }

    @Override
    public void close() {
      super.close();
      encrypt.close();
    }

    @Override
    public void changeIv(Consumer<byte[]> modifier) {
      encrypt.changeIv(modifier);
    }

    @Override
    public String toString() {
      return "encrypted " + super.toString();
    }
  }

  public static class CompressedStream extends InStream {
    private final int bufferSize;
    private ByteBuffer uncompressed;
    private final CompressionCodec codec;
    protected ByteBuffer compressed;
    protected DiskRangeList currentRange;
    private boolean isUncompressedOriginal;
    protected long currentCompressedStart = -1;

    /**
     * Create the stream without resetting the input stream.
     * This is used in subclasses so they can finish initializing before
     * reset is called.
     * @param name the name of the stream
     * @param length the total number of bytes in the stream
     * @param options the options used to read the stream
     */
    public CompressedStream(Object name,
                            long offset,
                            long length,
                            StreamOptions options) {
      super(name, offset, length);
      this.codec = options.codec;
      this.bufferSize = options.bufferSize;
    }

    /**
     * Create the stream and initialize the input for the stream.
     * @param name the name of the stream
     * @param input the input data
     * @param length the total length of the stream
     * @param options the options to read the data with
     */
    public CompressedStream(Object name,
                            DiskRangeList input,
                            long offset,
                            long length,
                            StreamOptions options) {
      super(name, offset, length);
      this.codec = options.codec;
      this.bufferSize = options.bufferSize;
      reset(input);
    }

    private void allocateForUncompressed(int size, boolean isDirect) {
      uncompressed = allocateBuffer(size, isDirect);
    }

    @Override
    protected void setCurrent(DiskRangeList newRange,
                              boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        compressed = newRange.getData().slice();
        int pos = (int) (position + offset - newRange.getOffset());
        compressed.position(pos);
        compressed.limit(pos + (int) Math.min(compressed.remaining(),
            length - position));
      }
    }

    private int readHeaderByte() {
      while (currentRange != null &&
                 (compressed == null || compressed.remaining() <= 0)) {
        setCurrent(currentRange.next, false);
      }
      if (compressed != null && compressed.remaining() > 0) {
        position += 1;
        return compressed.get() & 0xff;
      } else {
        throw new IllegalStateException("Can't read header at " + this);
      }
    }

    private void readHeader() throws IOException {
      currentCompressedStart = this.position;
      int b0 = readHeaderByte();
      int b1 = readHeaderByte();
      int b2 = readHeaderByte();
      boolean isOriginal = (b0 & 0x01) == 1;
      int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

      if (chunkLength > bufferSize) {
        throw new IllegalArgumentException("Buffer size too small. size = " +
            bufferSize + " needed = " + chunkLength + " in " + name);
      }
      ByteBuffer slice = this.slice(chunkLength);

      if (isOriginal) {
        uncompressed = slice;
        isUncompressedOriginal = true;
      } else {
        if (isUncompressedOriginal) {
          // Since the previous chunk was uncompressed, allocate the buffer and set original false
          allocateForUncompressed(bufferSize, slice.isDirect());
          isUncompressedOriginal = false;
        } else if (uncompressed == null) {
          // If the buffer was not allocated then allocate the same
          allocateForUncompressed(bufferSize, slice.isDirect());
        } else {
          // Since the buffer is already allocated just clear the same
          uncompressed.clear();
        }
        codec.decompress(slice, uncompressed);
      }
    }

    @Override
    public int read() throws IOException {
      if (!ensureUncompressed()) {
        return -1;
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (!ensureUncompressed()) {
        return -1;
      }
      int actualLength = Math.min(length, uncompressed.remaining());
      uncompressed.get(data, offset, actualLength);
      return actualLength;
    }

    private boolean ensureUncompressed() throws IOException {
      while (uncompressed == null || uncompressed.remaining() == 0) {
        if (position == this.length) {
          return false;
        }
        readHeader();
      }
      return true;
    }

    @Override
    public int available() throws IOException {
      if (!ensureUncompressed()) {
        return 0;
      }
      return uncompressed.remaining();
    }

    @Override
    public void close() {
      uncompressed = null;
      compressed = null;
      currentRange = null;
      position = length;
      bytes = null;
    }

    @Override
    public void changeIv(Consumer<byte[]> modifier) {
      // nothing to do
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      boolean seeked = seek(index.getNext());
      long uncompressedBytes = index.getNext();
      if (!seeked) {
        if (uncompressed != null) {
          // Only reposition uncompressed
          uncompressed.position((int) uncompressedBytes);
        }
        // uncompressed == null should not happen as !seeked would mean that a previous
        // readHeader has taken place
      } else {
        if (uncompressedBytes != 0) {
          // Decompress compressed as a seek has taken place and position uncompressed
          readHeader();
          uncompressed.position(uncompressed.position() +
                                (int) uncompressedBytes);
        } else if (uncompressed != null) {
          // mark the uncompressed buffer as done
          uncompressed.position(uncompressed.limit());
        }
      }
    }

    /* slices a read only contiguous buffer of chunkLength */
    private ByteBuffer slice(int chunkLength) throws IOException {
      int len = chunkLength;
      final DiskRangeList oldRange = currentRange;
      final long oldPosition = position;
      ByteBuffer slice;
      if (compressed.remaining() >= len) {
        slice = compressed.slice();
        // simple case
        slice.limit(len);
        position += len;
        compressed.position(compressed.position() + len);
        return slice;
      } else if (currentRange.next == null) {
        // nothing has been modified yet
        throw new IOException("EOF in " + this + " while trying to read " +
            chunkLength + " bytes");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(
            "Crossing into next BufferChunk because compressed only has %d bytes (needs %d)",
            compressed.remaining(), len));
      }

      // we need to consolidate 2 or more buffers into 1
      // first copy out compressed buffers
      ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
      position += compressed.remaining();
      len -= compressed.remaining();
      copy.put(compressed);

      while (currentRange.next != null) {
        setCurrent(currentRange.next, false);
        LOG.debug("Read slow-path, >1 cross block reads with {}", this);
        if (compressed.remaining() >= len) {
          slice = compressed.slice();
          slice.limit(len);
          copy.put(slice);
          position += len;
          compressed.position(compressed.position() + len);
          copy.flip();
          return copy;
        }
        position += compressed.remaining();
        len -= compressed.remaining();
        copy.put(compressed);
      }

      // restore offsets for exception clarity
      position = oldPosition;
      setCurrent(oldRange, true);
      throw new IOException("EOF in " + this + " while trying to read " +
          chunkLength + " bytes");
    }

    /**
     * Seek to the desired chunk based on the input position.
     *
     * @param desired position in the compressed stream
     * @return Indicates whether a seek was performed or not
     * @throws IOException when seeking outside the stream bounds
     */
    boolean seek(long desired) throws IOException {
      if (desired == 0 && bytes == null) {
        return false;
      }
      if (desired == currentCompressedStart) {
        // Header already at the required position
        return false;
      }
      long posn = desired + offset;
      for (DiskRangeList range = bytes; range != null; range = range.next) {
        if (range.getOffset() <= posn &&
            (range.next == null ? posn <= range.getEnd() :
              posn < range.getEnd())) {
          position = desired;
          setCurrent(range, true);
          return true;
        }
      }
      throw new IOException("Seek outside of data in " + this + " to " + desired);
    }

    private String rangeString() {
      StringBuilder builder = new StringBuilder();
      int i = 0;
      for (DiskRangeList range = bytes; range != null; range = range.next){
        if (i != 0) {
          builder.append("; ");
        }
        builder.append(" range ");
        builder.append(i);
        builder.append(" = ");
        builder.append(range.getOffset());
        builder.append(" to ");
        builder.append(range.getEnd());
        ++i;
      }
      return builder.toString();
    }

    @Override
    public String toString() {
      return "compressed stream " + name + " position: " + position +
          " length: " + length + " range: " + getRangeNumber(bytes, currentRange) +
          " offset: " + (compressed == null ? 0 : compressed.position()) +
          " limit: " + (compressed == null ? 0 : compressed.limit()) +
          rangeString() +
          (uncompressed == null ? "" :
              " uncompressed: " + uncompressed.position() + " to " +
                  uncompressed.limit());
    }
  }

  private static class EncryptedCompressedStream extends CompressedStream {
    private final EncryptionState encrypt;

    EncryptedCompressedStream(Object name,
                                     DiskRangeList input,
                                     long offset,
                                     long length,
                                     StreamOptions options) {
      super(name, offset, length, options);
      encrypt = new EncryptionState(name, offset, options);
      reset(input);
    }

    @Override
    protected void setCurrent(DiskRangeList newRange, boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        // what is the position of the start of the newRange?
        long rangeOffset = newRange.getOffset();
        int ignoreBytes = 0;
        ByteBuffer encrypted = newRange.getData().slice();
        if (rangeOffset < offset) {
          ignoreBytes = (int) (offset - rangeOffset);
          encrypted.position(ignoreBytes);
        }
        if (isJump) {
          encrypt.changeIv(ignoreBytes + rangeOffset - offset);
        }
        encrypted.limit(ignoreBytes +
                            (int) Math.min(encrypted.remaining(), length));
        compressed = encrypt.decrypt(encrypted);
        if (position + offset > rangeOffset + ignoreBytes) {
          compressed.position((int) (position + offset - rangeOffset - ignoreBytes));
        }
      }
    }

    @Override
    public void close() {
      super.close();
      encrypt.close();
    }

    @Override
    public void changeIv(Consumer<byte[]> modifier) {
      encrypt.changeIv(modifier);
    }

    @Override
    public String toString() {
      return "encrypted " + super.toString();
    }
  }

  public abstract void seek(PositionProvider index) throws IOException;

  public static class StreamOptions implements Cloneable {
    private CompressionCodec codec;
    private int bufferSize;
    private EncryptionAlgorithm algorithm;
    private Key key;
    private byte[] iv;

    public StreamOptions(StreamOptions other) {
      codec = other.codec;
      bufferSize = other.bufferSize;
      algorithm = other.algorithm;
      key = other.key;
      iv = other.iv == null ? null : other.iv.clone();
    }

    public StreamOptions() {
    }

    public StreamOptions withCodec(CompressionCodec value) {
      this.codec = value;
      return this;
    }

    public StreamOptions withBufferSize(int value) {
      bufferSize = value;
      return this;
    }

    public StreamOptions withEncryption(EncryptionAlgorithm algorithm,
                                        Key key,
                                        byte[] iv) {
      this.algorithm = algorithm;
      this.key = key;
      this.iv = iv;
      return this;
    }

    public boolean isCompressed() {
      return codec != null;
    }

    public CompressionCodec getCodec() {
      return codec;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public EncryptionAlgorithm getAlgorithm() {
      return algorithm;
    }

    public Key getKey() {
      return key;
    }

    public byte[] getIv() {
      return iv;
    }

    @Override
    public StreamOptions clone() {
      try {
        StreamOptions clone = (StreamOptions) super.clone();
        if (clone.codec != null) {
          // Make sure we don't share the same codec between two readers.
          clone.codec = OrcCodecPool.getCodec(codec.getKind());
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("compress: ");
      buffer.append(codec == null ? "none" : codec.getKind());
      buffer.append(", buffer size: ");
      buffer.append(bufferSize);
      if (key != null) {
        buffer.append(", encryption: ");
        buffer.append(algorithm);
      }
      return buffer.toString();
    }
  }

  public static StreamOptions options() {
    return new StreamOptions();
  }

  /**
   * Create an input stream from a list of disk ranges with data.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream; from disk or cache
   * @param offset the first byte offset of the stream
   * @param length the length in bytes of the stream
   * @param options the options to read with
   * @return an input stream
   */
  public static InStream create(Object name,
                                DiskRangeList input,
                                long offset,
                                long length,
                                StreamOptions options) {
    LOG.debug("Reading {} with {} from {} for {}", name, options, offset,
        length);
    if (options == null || options.codec == null) {
      if (options == null || options.key == null) {
        return new UncompressedStream(name, input, offset, length);
      } else {
        OutStream.logKeyAndIv(name, options.getKey(), options.getIv());
        return new EncryptedStream(name, input, offset, length, options);
      }
    } else if (options.key == null) {
      return new CompressedStream(name, input, offset, length, options);
    } else {
      OutStream.logKeyAndIv(name, options.getKey(), options.getIv());
      return new EncryptedCompressedStream(name, input, offset, length, options);
    }
  }

  /**
   * Create an input stream from a list of disk ranges with data.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream; from disk or cache
   * @param length the length in bytes of the stream
   * @return an input stream
   */
  public static InStream create(Object name,
                                DiskRangeList input,
                                long offset,
                                long length) {
    return create(name, input, offset, length, null);
  }

  /**
   * Creates coded input stream (used for protobuf message parsing) with higher
   * message size limit.
   *
   * @param inStream   the stream to wrap.
   * @return coded input stream
   */
  public static CodedInputStream createCodedInputStream(InStream inStream) {
    CodedInputStream codedInputStream = CodedInputStream.newInstance(inStream);
    codedInputStream.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
    return codedInputStream;
  }
}
