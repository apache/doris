/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.function.Consumer;

/**
 * This class has routines to work with encryption within ORC files.
 */
public class CryptoUtils {

  private static final int COLUMN_ID_LENGTH = 3;
  private static final int KIND_LENGTH = 2;
  private static final int STRIPE_ID_LENGTH = 3;
  private static final int MIN_COUNT_BYTES = 8;

  static final int MAX_COLUMN = 0xffffff;
  static final int MAX_KIND = 0xffff;
  static final int MAX_STRIPE = 0xffffff;

  /**
   * Update the unique IV for each stream within a single key.
   * The top bytes are set with the column, stream kind, and stripe id and the
   * lower 8 bytes are always 0.
   * @param name the stream name
   * @param stripeId the stripe id
   */
  public static Consumer<byte[]> modifyIvForStream(StreamName name,
                                                   long stripeId) {
    return modifyIvForStream(name.getColumn(), name.getKind(), stripeId);
  }

  /**
   * Update the unique IV for each stream within a single key.
   * The top bytes are set with the column, stream kind, and stripe id and the
   * lower 8 bytes are always 0.
   * @param columnId the column id
   * @param kind the stream kind
   * @param stripeId the stripe id
   */
  public static Consumer<byte[]> modifyIvForStream(int columnId,
                                                   OrcProto.Stream.Kind kind,
                                                   long stripeId) {
    if (columnId < 0 || columnId > MAX_COLUMN) {
      throw new IllegalArgumentException("ORC encryption is limited to " +
                                             MAX_COLUMN + " columns. Value = " + columnId);
    }
    int k = kind.getNumber();
    if (k < 0 || k > MAX_KIND) {
      throw new IllegalArgumentException("ORC encryption is limited to " +
                                             MAX_KIND + " stream kinds. Value = " + k);
    }
    return (byte[] iv) -> {
      // the rest of the iv is used for counting within the stream
      if (iv.length - (COLUMN_ID_LENGTH + KIND_LENGTH + STRIPE_ID_LENGTH) < MIN_COUNT_BYTES) {
        throw new IllegalArgumentException("Not enough space in the iv for the count");
      }
      iv[0] = (byte) (columnId >> 16);
      iv[1] = (byte) (columnId >> 8);
      iv[2] = (byte) columnId;
      iv[COLUMN_ID_LENGTH] = (byte) (k >> 8);
      iv[COLUMN_ID_LENGTH + 1] = (byte) (k);
      modifyIvForStripe(stripeId).accept(iv);
    };
  }

  /**
   * Modify the IV for the given stripe id and make sure the low bytes are
   * set to 0.
   * @param stripeId the stripe id
   */
  public static Consumer<byte[]> modifyIvForStripe(long stripeId) {
    if (stripeId < 1 || stripeId > MAX_STRIPE) {
      throw new IllegalArgumentException("ORC encryption is limited to " +
                                             MAX_STRIPE + " stripes. Value = " +
                                             stripeId);
    }
    return (byte[] iv) -> {
      iv[COLUMN_ID_LENGTH + KIND_LENGTH] = (byte) (stripeId >> 16);
      iv[COLUMN_ID_LENGTH + KIND_LENGTH + 1] = (byte) (stripeId >> 8);
      iv[COLUMN_ID_LENGTH + KIND_LENGTH + 2] = (byte) stripeId;
      clearCounter(iv);
    };
  }

  /**
   * Clear the counter part of the IV.
   * @param iv the IV to modify
   */
  public static void clearCounter(byte[] iv) {
    for(int i= COLUMN_ID_LENGTH + KIND_LENGTH + STRIPE_ID_LENGTH; i < iv.length; ++i) {
      iv[i] = 0;
    }
  }

  /** A cache for the key providers */
  private static final Map<String, KeyProvider> keyProviderCache = new HashMap<>();

  /**
   * Create a KeyProvider.
   * It will cache the result, so that only one provider of each kind will be
   * created.
   *
   * @param random the random generator to use
   * @return the new KeyProvider
   */
  public static KeyProvider getKeyProvider(Configuration conf,
                                           Random random) throws IOException {
    String kind = OrcConf.KEY_PROVIDER.getString(conf);
    String cacheKey = kind + "." + random.getClass().getName();
    KeyProvider result = keyProviderCache.get(cacheKey);
    if (result == null) {
      ServiceLoader<KeyProvider.Factory> loader = ServiceLoader.load(KeyProvider.Factory.class);
      for (KeyProvider.Factory factory : loader) {
        result = factory.create(kind, conf, random);
        if (result != null) {
          keyProviderCache.put(cacheKey, result);
          break;
        }
      }
    }
    return result;
  }

  public static class HadoopKeyProviderFactory implements KeyProvider.Factory {

    @Override
    public KeyProvider create(String kind,
                              Configuration conf,
                              Random random) throws IOException {
      if ("hadoop".equals(kind)) {
        return HadoopShimsFactory.get().getHadoopKeyProvider(conf, random);
      } else if ("memory".equals(kind)) {
        return new InMemoryKeystore(random);
      }
      return null;
    }
  }
}
