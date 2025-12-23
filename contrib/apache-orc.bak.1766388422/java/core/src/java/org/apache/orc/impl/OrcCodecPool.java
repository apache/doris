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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A clone of Hadoop codec pool for ORC; cause it has its own codecs...
 */
public final class OrcCodecPool {
  private static final Logger LOG = LoggerFactory.getLogger(OrcCodecPool.class);

  /**
   * A global decompressor pool used to save the expensive
   * construction/destruction of (possibly native) decompression codecs.
   */
  private static final ConcurrentHashMap<CompressionKind, List<CompressionCodec>> POOL =
      new ConcurrentHashMap<>();

  private static final int MAX_PER_KIND = 32;

  public static CompressionCodec getCodec(CompressionKind kind) {
    if (kind == CompressionKind.NONE) return null;
    CompressionCodec codec = null;
    List<CompressionCodec> codecList = POOL.get(kind);
    if (codecList != null) {
      synchronized (codecList) {
        if (!codecList.isEmpty()) {
          codec = codecList.remove(codecList.size() - 1);
        }
      }
    }
    if (codec == null) {
      codec = WriterImpl.createCodec(kind);
      LOG.debug("Got brand-new codec {}", kind);
    } else {
      LOG.debug("Got recycled codec");
    }
    return codec;
  }

  /**
   * Returns the codec to the pool or closes it, suppressing exceptions.
   * @param kind Compression kind.
   * @param codec Codec.
   */
  public static void returnCodec(CompressionKind kind, CompressionCodec codec) {
    if (codec == null) {
      return;
    }
    assert kind != CompressionKind.NONE;
    try {
      codec.reset();
      List<CompressionCodec> list = POOL.get(kind);
      if (list == null) {
        List<CompressionCodec> newList = new ArrayList<>();
        List<CompressionCodec> oldList = POOL.putIfAbsent(kind, newList);
        list = (oldList == null) ? newList : oldList;
      }
      synchronized (list) {
        if (list.size() < MAX_PER_KIND) {
          list.add(codec);
          return;
        }
      }
      // We didn't add the codec to the list.
      codec.destroy();
    } catch (Exception ex) {
      LOG.error("Ignoring codec cleanup error", ex);
    }
  }

  public static int getPoolSize(CompressionKind kind) {
    if (kind == CompressionKind.NONE) return 0;
    List<CompressionCodec> codecList = POOL.get(kind);
    if (codecList == null) return 0;
    synchronized (codecList) {
      return codecList.size();
    }
  }

  /**
   * Clear the codec pool. Mostly used for testing.
   */
  public static void clear() {
    POOL.clear();
  }

  private OrcCodecPool() {
    // prevent instantiation
  }
}
