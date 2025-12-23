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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The API for compression codecs for ORC.
 * Closeable.close() returns this codec to the OrcCodecPool.
 */
public interface CompressionCodec extends Closeable {

  enum SpeedModifier {
    /* speed/compression tradeoffs */
    FASTEST,
    FAST,
    DEFAULT
  }

  enum DataKind {
    TEXT,
    BINARY
  }

  interface Options {
    /**
     * Make a copy before making changes.
     * @return a new copy
     */
    Options copy();

    /**
     * Set the speed for the compression.
     * @param newValue how aggressively to compress
     * @return this
     */
    Options setSpeed(SpeedModifier newValue);

    /**
     * Set the kind of data for the compression.
     * @param newValue what kind of data this is
     * @return this
     */
    Options setData(DataKind newValue);
  }

  /**
   * Get the default options for this codec.
   * @return the default options object
   */
  Options getDefaultOptions();

  /**
   * Compress the in buffer to the out buffer.
   * @param in the bytes to compress
   * @param out the compressed bytes
   * @param overflow put any additional bytes here
   * @param options the options to control compression
   * @return true if the output is smaller than input
   * @throws IOException
   */
  boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow,
                   Options options) throws IOException;

  /**
   * Decompress the in buffer to the out buffer.
   * @param in the bytes to decompress
   * @param out the decompressed bytes
   * @throws IOException
   */
  void decompress(ByteBuffer in, ByteBuffer out) throws IOException;

  /** Resets the codec, preparing it for reuse. */
  void reset();

  /** Closes the codec, releasing the resources. */
  void destroy();

  /**
   * Get the compression kind.
   */
  CompressionKind getKind();

  /**
   * Return the codec to the pool.
   */
  @Override
  void close();
}
