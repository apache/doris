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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * This is copied from commons-io project to cut the dependency
 * from old Hadoop.
 */
public final class IOUtils {

  public static final int DEFAULT_BUFFER_SIZE = 8192;

  /**
   * Returns a new byte array of size {@link #DEFAULT_BUFFER_SIZE}.
   *
   * @return a new byte array of size {@link #DEFAULT_BUFFER_SIZE}.
   * @since 2.9.0
   */
  public static byte[] byteArray() {
    return byteArray(DEFAULT_BUFFER_SIZE);
  }

  /**
   * Returns a new byte array of the given size.
   *
   * TODO Consider guarding or warning against large allocations...
   *
   * @param size array size.
   * @return a new byte array of the given size.
   * @since 2.9.0
   */
  public static byte[] byteArray(final int size) {
    return new byte[size];
  }

  /**
   * Internal byte array buffer.
   */
  private static final ThreadLocal<byte[]> SKIP_BYTE_BUFFER =
      ThreadLocal.withInitial(IOUtils::byteArray);

  /**
   * Gets the thread local byte array.
   *
   * @return the thread local byte array.
   */
  static byte[] getByteArray() {
    return SKIP_BYTE_BUFFER.get();
  }

  /**
   * Skips the requested number of bytes or fail if there are not enough left.
   * <p>
   * This allows for the possibility that {@link InputStream#skip(long)} may
   * not skip as many bytes as requested (most likely because of reaching EOF).
   * <p>
   * Note that the implementation uses {@link #skip(InputStream, long)}.
   * This means that the method may be considerably less efficient than using the actual skip implementation,
   * this is done to guarantee that the correct number of characters are skipped.
   * </p>
   *
   * @param input stream to skip
   * @param toSkip the number of bytes to skip
   * @throws IOException              if there is a problem reading the file
   * @throws IllegalArgumentException if toSkip is negative
   * @throws EOFException             if the number of bytes skipped was incorrect
   * @see InputStream#skip(long)
   * @since 2.0
   */
  public static void skipFully(final InputStream input, final long toSkip)
      throws IOException {
    if (toSkip < 0) {
      throw new IllegalArgumentException("Bytes to skip must not be negative: " + toSkip);
    }
    final long skipped = skip(input, toSkip);
    if (skipped != toSkip) {
      throw new EOFException("Bytes to skip: " + toSkip + " actual: " + skipped);
    }
  }

  /**
   * Skips bytes from an input byte stream.
   * This implementation guarantees that it will read as many bytes
   * as possible before giving up; this may not always be the case for
   * skip() implementations in subclasses of {@link InputStream}.
   * <p>
   * Note that the implementation uses {@link InputStream#read(byte[], int, int)} rather
   * than delegating to {@link InputStream#skip(long)}.
   * This means that the method may be considerably less efficient than using the actual skip implementation,
   * this is done to guarantee that the correct number of bytes are skipped.
   * </p>
   *
   * @param input byte stream to skip
   * @param toSkip number of bytes to skip.
   * @return number of bytes actually skipped.
   * @throws IOException              if there is a problem reading the file
   * @throws IllegalArgumentException if toSkip is negative
   * @see <a href="https://issues.apache.org/jira/browse/IO-203">IO-203 - Add skipFully() method for InputStreams</a>
   * @since 2.0
   */
  public static long skip(final InputStream input, final long toSkip) throws IOException {
    if (toSkip < 0) {
      throw new IllegalArgumentException("Skip count must be non-negative, actual: " + toSkip);
    }
    /*
     * N.B. no need to synchronize access to SKIP_BYTE_BUFFER: - we don't care if the buffer is created multiple
     * times (the data is ignored) - we always use the same size buffer, so if it it is recreated it will still be
     * OK (if the buffer size were variable, we would need to synch. to ensure some other thread did not create a
     * smaller one)
     */
    long remain = toSkip;
    while (remain > 0) {
      // See https://issues.apache.org/jira/browse/IO-203 for why we use read() rather than delegating to skip()
      final byte[] byteArray = getByteArray();
      final long n = input.read(byteArray, 0, (int) Math.min(remain, byteArray.length));
      if (n < 0) { // EOF
        break;
      }
      remain -= n;
    }
    return toSkip - remain;
  }
}
