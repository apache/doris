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

import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.CompressionCodec;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.writer.StreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.util.function.Consumer;

/**
 * The output stream for writing to ORC files.
 * It handles both compression and encryption.
 */
public class OutStream extends PositionedOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OutStream.class);

  // This logger will log the local keys to be printed to the logs at debug.
  // Be *extremely* careful turning it on.
  static final Logger KEY_LOGGER = LoggerFactory.getLogger("org.apache.orc.keys");

  public static final int HEADER_SIZE = 3;
  private final Object name;
  private final PhysicalWriter.OutputReceiver receiver;

  /**
   * Stores the uncompressed bytes that have been serialized, but not
   * compressed yet. When this fills, we compress the entire buffer.
   */
  private ByteBuffer current = null;

  /**
   * Stores the compressed bytes until we have a full buffer and then outputs
   * them to the receiver. If no compression is being done, this (and overflow)
   * will always be null and the current buffer will be sent directly to the
   * receiver.
   */
  private ByteBuffer compressed = null;

  /**
   * Since the compressed buffer may start with contents from previous
   * compression blocks, we allocate an overflow buffer so that the
   * output of the codec can be split between the two buffers. After the
   * compressed buffer is sent to the receiver, the overflow buffer becomes
   * the new compressed buffer.
   */
  private ByteBuffer overflow = null;
  private final int bufferSize;
  private final CompressionCodec codec;
  private final CompressionCodec.Options options;
  private long compressedBytes = 0;
  private long uncompressedBytes = 0;
  private final Cipher cipher;
  private final Key key;
  private final byte[] iv;

  public OutStream(Object name,
                   StreamOptions options,
                   PhysicalWriter.OutputReceiver receiver) {
    this.name = name;
    this.bufferSize = options.getBufferSize();
    this.codec = options.getCodec();
    this.options = options.getCodecOptions();
    this.receiver = receiver;
    if (options.isEncrypted()) {
      this.cipher = options.getAlgorithm().createCipher();
      this.key = options.getKey();
      this.iv = options.getIv();
      resetState();
    } else {
      this.cipher = null;
      this.key = null;
      this.iv = null;
    }
    LOG.debug("Stream {} written to with {}", name, options);
    logKeyAndIv(name, key, iv);
  }

  static void logKeyAndIv(Object name, Key key, byte[] iv) {
    if (iv != null && KEY_LOGGER.isDebugEnabled()) {
      KEY_LOGGER.debug("Stream: {} Key: {} IV: {}", name,
          new BytesWritable(key.getEncoded()), new BytesWritable(iv));
    }
  }

  /**
   * Change the current Initialization Vector (IV) for the encryption.
   * @param modifier a function to modify the IV in place
   */
  @Override
  public void changeIv(Consumer<byte[]> modifier) {
    if (iv != null) {
      modifier.accept(iv);
      resetState();
      logKeyAndIv(name, key, iv);
    }
  }

  /**
   * Reset the cipher after changing the IV.
   */
  private void resetState() {
    try {
      cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
    } catch (InvalidKeyException e) {
      throw new IllegalStateException("ORC bad encryption key for " + this, e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalStateException("ORC bad encryption parameter for " + this, e);
    }
  }

  /**
   * When a buffer is done, we send it to the receiver to store.
   * If we are encrypting, encrypt the buffer before we pass it on.
   * @param buffer the buffer to store
   */
  void outputBuffer(ByteBuffer buffer) throws IOException {
    if (cipher != null) {
      ByteBuffer output = buffer.duplicate();
      int len = buffer.remaining();
      try {
        int encrypted = cipher.update(buffer, output);
        output.flip();
        receiver.output(output);
        if (encrypted != len) {
          throw new IllegalArgumentException("Encryption of incomplete buffer "
              + len + " -> " + encrypted + " in " + this);
        }
      } catch (ShortBufferException e) {
        throw new IOException("Short buffer in encryption in " + this, e);
      }
    } else {
      receiver.output(buffer);
    }
  }

  /**
   * Ensure that the cipher didn't save any data.
   * The next call should be to changeIv to restart the encryption on a new IV.
   */
  void finishEncryption() {
    try {
      byte[] finalBytes = cipher.doFinal();
      if (finalBytes != null && finalBytes.length != 0) {
        throw new IllegalStateException("We shouldn't have remaining bytes " + this);
      }
    } catch (IllegalBlockSizeException e) {
      throw new IllegalArgumentException("Bad block size", e);
    } catch (BadPaddingException e) {
      throw new IllegalArgumentException("Bad padding", e);
    }
  }

  /**
   * Write the length of the compressed bytes. Life is much easier if the
   * header is constant length, so just use 3 bytes. Considering most of the
   * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
   * be plenty. We also use the low bit for whether it is the original or
   * compressed bytes.
   * @param buffer the buffer to write the header to
   * @param position the position in the buffer to write at
   * @param val the size in the file
   * @param original is it uncompressed
   */
  private static void writeHeader(ByteBuffer buffer,
                                  int position,
                                  int val,
                                  boolean original) {
    buffer.put(position, (byte) ((val << 1) + (original ? 1 : 0)));
    buffer.put(position + 1, (byte) (val >> 7));
    buffer.put(position + 2, (byte) (val >> 15));
  }

  private void getNewInputBuffer() {
    if (codec == null) {
      current = ByteBuffer.allocate(bufferSize);
    } else {
      current = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      writeHeader(current, 0, bufferSize, true);
      current.position(HEADER_SIZE);
    }
  }

  /**
   * Throws exception if the bufferSize argument equals or exceeds 2^(3*8 - 1).
   * See {@link OutStream#writeHeader(ByteBuffer, int, int, boolean)}.
   * The bufferSize needs to be expressible in 3 bytes, and uses the least significant byte
   * to indicate original/compressed bytes.
   * @param bufferSize The ORC compression buffer size being checked.
   * @throws IllegalArgumentException If bufferSize value exceeds threshold.
   */
  public static void assertBufferSizeValid(int bufferSize) throws IllegalArgumentException {
    if (bufferSize >= (1 << 23)) {
      throw new IllegalArgumentException("Illegal value of ORC compression buffer size: " + bufferSize);
    }
  }

  /**
   * Allocate a new output buffer if we are compressing.
   */
  private ByteBuffer getNewOutputBuffer() {
    return ByteBuffer.allocate(bufferSize + HEADER_SIZE);
  }

  private void flip() {
    current.limit(current.position());
    current.position(codec == null ? 0 : HEADER_SIZE);
  }

  @Override
  public void write(int i) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    if (current.remaining() < 1) {
      spill();
    }
    uncompressedBytes += 1;
    current.put((byte) i);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    int remaining = Math.min(current.remaining(), length);
    current.put(bytes, offset, remaining);
    uncompressedBytes += remaining;
    length -= remaining;
    while (length != 0) {
      spill();
      offset += remaining;
      remaining = Math.min(current.remaining(), length);
      current.put(bytes, offset, remaining);
      uncompressedBytes += remaining;
      length -= remaining;
    }
  }

  private void spill() throws java.io.IOException {
    // if there isn't anything in the current buffer, don't spill
    if (current == null ||
        current.position() == (codec == null ? 0 : HEADER_SIZE)) {
      return;
    }
    flip();
    if (codec == null) {
      outputBuffer(current);
      getNewInputBuffer();
    } else {
      if (compressed == null) {
        compressed = getNewOutputBuffer();
      } else if (overflow == null) {
        overflow = getNewOutputBuffer();
      }
      int sizePosn = compressed.position();
      compressed.position(compressed.position() + HEADER_SIZE);
      if (codec.compress(current, compressed, overflow, options)) {
        uncompressedBytes = 0;
        // move position back to after the header
        current.position(HEADER_SIZE);
        current.limit(current.capacity());
        // find the total bytes in the chunk
        int totalBytes = compressed.position() - sizePosn - HEADER_SIZE;
        if (overflow != null) {
          totalBytes += overflow.position();
        }
        compressedBytes += totalBytes + HEADER_SIZE;
        writeHeader(compressed, sizePosn, totalBytes, false);
        // if we have less than the next header left, spill it.
        if (compressed.remaining() < HEADER_SIZE) {
          compressed.flip();
          outputBuffer(compressed);
          compressed = overflow;
          overflow = null;
        }
      } else {
        compressedBytes += uncompressedBytes + HEADER_SIZE;
        uncompressedBytes = 0;
        // we are using the original, but need to spill the current
        // compressed buffer first. So back up to where we started,
        // flip it and add it to done.
        if (sizePosn != 0) {
          compressed.position(sizePosn);
          compressed.flip();
          outputBuffer(compressed);
          compressed = null;
          // if we have an overflow, clear it and make it the new compress
          // buffer
          if (overflow != null) {
            overflow.clear();
            compressed = overflow;
            overflow = null;
          }
        } else {
          compressed.clear();
          if (overflow != null) {
            overflow.clear();
          }
        }

        // now add the current buffer into the done list and get a new one.
        current.position(0);
        // update the header with the current length
        writeHeader(current, 0, current.limit() - HEADER_SIZE, true);
        outputBuffer(current);
        getNewInputBuffer();
      }
    }
  }

  @Override
  public void getPosition(PositionRecorder recorder) {
    if (codec == null) {
      recorder.addPosition(uncompressedBytes);
    } else {
      recorder.addPosition(compressedBytes);
      recorder.addPosition(uncompressedBytes);
    }
  }

  @Override
  public void flush() throws IOException {
    spill();
    if (compressed != null && compressed.position() != 0) {
      compressed.flip();
      outputBuffer(compressed);
    }
    if (cipher != null) {
      finishEncryption();
    }
    compressed = null;
    uncompressedBytes = 0;
    compressedBytes = 0;
    overflow = null;
    current = null;
  }

  @Override
  public String toString() {
    return name.toString();
  }

  @Override
  public long getBufferSize() {
    if (codec == null) {
      return uncompressedBytes + (current == null ? 0 : current.remaining());
    } else {
      long result = 0;
      if (current != null) {
        result += current.capacity();
      }
      if (compressed != null) {
        result += compressed.capacity();
      }
      if (overflow != null) {
        result += overflow.capacity();
      }
      return result + compressedBytes;
    }
  }

  /**
   * Set suppress flag
   */
  public void suppress() {
    receiver.suppress();
  }
}

