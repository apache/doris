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

import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZlibCodec implements CompressionCodec, DirectDecompressionCodec {
  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  // Note: shim path does not care about levels and strategies (only used for decompression).
  private HadoopShims.DirectDecompressor decompressShim = null;
  private Boolean direct = null;

  static class ZlibOptions implements Options {
    private int level;
    private int strategy;
    private final boolean FIXED;

    ZlibOptions(int level, int strategy, boolean fixed) {
      this.level = level;
      this.strategy = strategy;
      FIXED = fixed;
    }

    @Override
    public ZlibOptions copy() {
      return new ZlibOptions(level, strategy, false);
    }

    @Override
    public ZlibOptions setSpeed(SpeedModifier newValue) {
      if (FIXED) {
        throw new IllegalStateException("Attempt to modify the default options");
      }
      switch (newValue) {
        case FAST:
          // deflate_fast looking for 16 byte patterns
          level = Deflater.BEST_SPEED + 1;
          break;
        case DEFAULT:
          // deflate_slow looking for 128 byte patterns
          level = Deflater.DEFAULT_COMPRESSION;
          break;
        case FASTEST:
          // deflate_fast looking for 8 byte patterns
          level = Deflater.BEST_SPEED;
          break;
        default:
          break;
      }
      return this;
    }

    @Override
    public ZlibOptions setData(DataKind newValue) {
      if (FIXED) {
        throw new IllegalStateException("Attempt to modify the default options");
      }
      switch (newValue) {
        case BINARY:
          /* filtered == less LZ77, more huffman */
          strategy = Deflater.FILTERED;
          break;
        case TEXT:
          strategy = Deflater.DEFAULT_STRATEGY;
          break;
        default:
          break;
      }
      return this;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || getClass() != other.getClass()) {
        return false;
      } else if (this == other) {
        return true;
      } else {
        ZlibOptions otherOpts = (ZlibOptions) other;
        return level == otherOpts.level && strategy == otherOpts.strategy;
      }
    }

    @Override
    public int hashCode() {
      return level + strategy * 101;
    }
  }

  private static final ZlibOptions DEFAULT_OPTIONS =
      new ZlibOptions(Deflater.DEFAULT_COMPRESSION, Deflater.DEFAULT_STRATEGY, true);

  @Override
  public Options getDefaultOptions() {
    return DEFAULT_OPTIONS;
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow,
                          Options options) {
    ZlibOptions zlo = (ZlibOptions) options;
    int length = in.remaining();
    int outSize = 0;
    Deflater deflater = new Deflater(zlo.level, true);
    try {
      deflater.setStrategy(zlo.strategy);
      deflater.setInput(in.array(), in.arrayOffset() + in.position(), length);
      deflater.finish();
      int offset = out.arrayOffset() + out.position();
      while (!deflater.finished() && (length > outSize)) {
        int size = deflater.deflate(out.array(), offset, out.remaining());
        out.position(size + out.position());
        outSize += size;
        offset += size;
        // if we run out of space in the out buffer, use the overflow
        if (out.remaining() == 0) {
          if (overflow == null) {
            return false;
          }
          out = overflow;
          offset = out.arrayOffset() + out.position();
        }
      }
    } finally {
      deflater.end();
    }
    return length > outSize;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {

    if(in.isDirect() && out.isDirect()) {
      directDecompress(in, out);
      return;
    }

    Inflater inflater = new Inflater(true);
    try {
      inflater.setInput(in.array(), in.arrayOffset() + in.position(),
                        in.remaining());
      while (!(inflater.finished() || inflater.needsDictionary() ||
               inflater.needsInput())) {
        try {
          int count = inflater.inflate(out.array(),
                                       out.arrayOffset() + out.position(),
                                       out.remaining());
          out.position(count + out.position());
        } catch (DataFormatException dfe) {
          throw new IOException("Bad compression data", dfe);
        }
      }
      out.flip();
    } finally {
      inflater.end();
    }
    in.position(in.limit());
  }

  @Override
  public boolean isAvailable() {
    if (direct == null) {
      // see nowrap option in new Inflater(boolean) which disables zlib headers
      try {
        ensureShim();
        direct = (decompressShim != null);
      } catch (UnsatisfiedLinkError ule) {
        direct = false;
      }
    }
    return direct;
  }

  private void ensureShim() {
    if (decompressShim == null) {
      decompressShim = SHIMS.getDirectDecompressor(
          HadoopShims.DirectCompressionType.ZLIB_NOHEADER);
    }
  }

  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {
    ensureShim();
    decompressShim.decompress(in, out);
    out.flip(); // flip for read
  }

  @Override
  public void reset() {
    if (decompressShim != null) {
      decompressShim.reset();
    }
  }

  @Override
  public void destroy() {
    if (decompressShim != null) {
      decompressShim.end();
    }
  }

  @Override
  public CompressionKind getKind() {
    return CompressionKind.ZLIB;
  }

  @Override
  public void close() {
    OrcCodecPool.returnCodec(CompressionKind.ZLIB, this);
  }
}
