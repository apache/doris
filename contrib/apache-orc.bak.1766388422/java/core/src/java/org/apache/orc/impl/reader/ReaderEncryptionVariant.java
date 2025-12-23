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

package org.apache.orc.impl.reader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.EncryptionKey;
import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.KeyProvider;
import org.apache.orc.impl.LocalKey;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.StripeStatisticsImpl;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Key;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Information about an encrypted column.
 */
public class ReaderEncryptionVariant implements EncryptionVariant {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReaderEncryptionVariant.class);
  private final KeyProvider provider;
  private final ReaderEncryptionKey key;
  private final TypeDescription column;
  private final int variantId;
  private final BufferChunk tailBuffer;
  private final List<OrcProto.Stream> stripeStats;
  private final LocalKey[] localKeys;
  private final LocalKey footerKey;
  private final int stripeCount;
  private final long stripeStatsOffset;

  /**
   * Create a reader's view of an encryption variant.
   * @param key the encryption key description
   * @param variantId the of of the variant (0..N-1)
   * @param proto the serialized description of the variant
   * @param schema the file schema
   * @param stripes the stripe information
   * @param stripeStatsOffset the offset of the stripe statistics
   * @param tailBuffer the serialized file tail
   * @param provider the key provider
   */
  ReaderEncryptionVariant(ReaderEncryptionKey key,
                          int variantId,
                          OrcProto.EncryptionVariant proto,
                          TypeDescription schema,
                          List<StripeInformation> stripes,
                          long stripeStatsOffset,
                          BufferChunk tailBuffer,
                          KeyProvider provider) {
    this.key = key;
    this.variantId = variantId;
    this.provider = provider;
    this.column = proto == null || !proto.hasRoot() ? schema :
                      schema.findSubtype(proto.getRoot());
    this.localKeys = new LocalKey[stripes.size()];
    HashMap<BytesWritable, LocalKey> cache = new HashMap<>();
    stripeCount = stripes.size();
    this.stripeStatsOffset = stripeStatsOffset;
    if (proto != null && proto.hasEncryptedKey()) {
      for (int s = 0; s < localKeys.length; ++s) {
        StripeInformation stripe = stripes.get(s);
        localKeys[s] = getCachedKey(cache, key.getAlgorithm(),
            stripe.getEncryptedLocalKeys()[variantId]);
      }
      footerKey = getCachedKey(cache, key.getAlgorithm(),
          proto.getEncryptedKey().toByteArray());
      key.addVariant(this);
      stripeStats = proto.getStripeStatisticsList();
      this.tailBuffer = tailBuffer;
    } else {
      footerKey = null;
      stripeStats = null;
      this.tailBuffer = null;
    }
  }

  @Override
  public ReaderEncryptionKey getKeyDescription() {
    return key;
  }

  @Override
  public TypeDescription getRoot() {
    return column;
  }

  @Override
  public int getVariantId() {
    return variantId;
  }

  /**
   * Deduplicate the local keys so that we only decrypt each local key once.
   * @param cache the cache to use
   * @param encrypted the encrypted key
   * @return the local key
   */
  private static LocalKey getCachedKey(Map<BytesWritable, LocalKey> cache,
                                       EncryptionAlgorithm algorithm,
                                       byte[] encrypted) {
    // wrap byte array in BytesWritable to get equality and hash
    BytesWritable wrap = new BytesWritable(encrypted);
    LocalKey result = cache.get(wrap);
    if (result == null) {
      result = new LocalKey(algorithm, null, encrypted);
      cache.put(wrap, result);
    }
    return result;
  }

  private Key getDecryptedKey(LocalKey localKey) throws IOException {
    Key result = localKey.getDecryptedKey();
    if (result == null) {
      switch (this.key.getState()) {
        case UNTRIED:
          try {
            result = provider.decryptLocalKey(key.getMetadata(),
                localKey.getEncryptedKey());
          } catch (IOException ioe) {
            LOG.info("Can't decrypt using key {}", key);
          }
          if (result != null) {
            localKey.setDecryptedKey(result);
            key.setSuccess();
          } else {
            key.setFailure();
          }
          break;
        case SUCCESS:
          result = provider.decryptLocalKey(key.getMetadata(),
              localKey.getEncryptedKey());
          if (result == null) {
            throw new IOException("Can't decrypt local key " + key);
          }
          localKey.setDecryptedKey(result);
          break;
        case FAILURE:
          return null;
      }
    }
    return result;
  }

  @Override
  public Key getFileFooterKey() throws IOException {
    return (key == null || provider == null) ? null : getDecryptedKey(footerKey);
  }

  @Override
  public Key getStripeKey(long stripe) throws IOException {
    return (key == null || provider == null) ? null : getDecryptedKey(localKeys[(int) stripe]);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    } else {
      return compareTo((EncryptionVariant) other) == 0;
    }
  }

  @Override
  public int hashCode() {
    return key.hashCode() * 127 + column.getId();
  }

  @Override
  public int compareTo(@NotNull EncryptionVariant other) {
    if (other == this) {
      return 0;
    }
    EncryptionKey otherKey = other.getKeyDescription();
    if (key == otherKey) {
      return Integer.compare(column.getId(), other.getRoot().getId());
    } else if (key == null) {
      return -1;
    } else if (otherKey == null) {
      return 1;
    } else {
      return key.compareTo(other.getKeyDescription());
    }
  }

  public long getStripeStatisticsLength() {
    long result = 0;
    for(OrcProto.Stream stream: stripeStats) {
      result += stream.getLength();
    }
    return result;
  }

  /**
   * Decrypt the raw data and return the list of the stripe statistics for this
   * variant.
   * @param columns true for the columns that should be included
   * @param compression the compression options
   * @return the stripe statistics for this variant.
   */
  public List<StripeStatistics> getStripeStatistics(boolean[] columns,
                                                    InStream.StreamOptions compression,
                                                    ReaderImpl reader
                                                    ) throws IOException {
    StripeStatisticsImpl[] result = new StripeStatisticsImpl[stripeCount];
    for(int s=0; s < result.length; ++s) {
      result[s] = new StripeStatisticsImpl(column, reader.writerUsedProlepticGregorian(),
          reader.getConvertToProlepticGregorian());
    }
    // create the objects
    long offset = stripeStatsOffset;
    Key fileKey = getFileFooterKey();
    if (fileKey == null) {
      throw new IOException("Can't get file footer key for " + key.getKeyName());
    }
    int root = column.getId();
    for(OrcProto.Stream stream: stripeStats){
      long length = stream.getLength();
      int column = stream.getColumn();
      OrcProto.Stream.Kind kind = stream.getKind();
      if (kind == OrcProto.Stream.Kind.STRIPE_STATISTICS &&
          (columns == null || columns[column])) {
        byte[] iv = new byte[key.getAlgorithm().getIvLength()];
        CryptoUtils.modifyIvForStream(column, kind, stripeCount + 1).accept(iv);
        InStream.StreamOptions options = new InStream.StreamOptions(compression)
            .withEncryption(key.getAlgorithm(), fileKey, iv);
        OrcProto.ColumnarStripeStatistics stat =
            OrcProto.ColumnarStripeStatistics.parseFrom(
                InStream.createCodedInputStream(
                    InStream.create(stream, tailBuffer, offset,
                        length, options)));
        for(int s=0; s < result.length; ++s) {
          result[s].updateColumn(column - root, stat.getColStats(s));
        }
      }
      offset += length;
    }
    return Arrays.asList(result);
  }
}
