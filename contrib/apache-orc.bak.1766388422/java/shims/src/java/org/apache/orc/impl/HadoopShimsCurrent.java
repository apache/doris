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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.orc.EncryptionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

/**
 * Shims for recent versions of Hadoop
 * <p>
 * Adds support for:
 * <ul>
 *   <li>Variable length HDFS blocks</li>
 * </ul>
 */
public class HadoopShimsCurrent implements HadoopShims {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopShimsCurrent.class);

  static DirectDecompressor getDecompressor(DirectCompressionType codec) {
    switch (codec) {
      case ZLIB:
        return new ZlibDirectDecompressWrapper(
            new ZlibDecompressor.ZlibDirectDecompressor());
      case ZLIB_NOHEADER:
        return new ZlibDirectDecompressWrapper(
            new ZlibDecompressor.ZlibDirectDecompressor(
                ZlibDecompressor.CompressionHeader.NO_HEADER, 0));
      case SNAPPY:
        return new SnappyDirectDecompressWrapper(
            new SnappyDecompressor.SnappyDirectDecompressor());
      default:
        return null;
    }
  }

  /**
   * Find the correct algorithm based on the key's metadata.
   *
   * @param meta the key's metadata
   * @return the correct algorithm
   */
  static EncryptionAlgorithm findAlgorithm(KeyProviderCryptoExtension.Metadata meta) {
    String cipher = meta.getCipher();
    if (cipher.startsWith("AES/")) {
      int bitLength = meta.getBitLength();
      if (bitLength == 128) {
        return EncryptionAlgorithm.AES_CTR_128;
      } else {
        if (bitLength != 256) {
          LOG.info("ORC column encryption does not support " + bitLength +
              " bit keys. Using 256 bits instead.");
        }
        return EncryptionAlgorithm.AES_CTR_256;
      }
    }
    throw new IllegalArgumentException("ORC column encryption only supports" +
        " AES and not " + cipher);
  }

  static String buildKeyVersionName(KeyMetadata key) {
    return key.getKeyName() + "@" + key.getVersion();
  }

  static KeyProvider createKeyProvider(Configuration conf,
                                       Random random) throws IOException {
    List<org.apache.hadoop.crypto.key.KeyProvider> result =
        KeyProviderFactory.getProviders(conf);
    if (result.size() == 0) {
      LOG.info("Can't get KeyProvider for ORC encryption from" +
          " hadoop.security.key.provider.path.");
      return new NullKeyProvider();
    } else {
      return new KeyProviderImpl(result.get(0), random);
    }
  }

  @Override
  public DirectDecompressor getDirectDecompressor(DirectCompressionType codec) {
    return getDecompressor(codec);
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
  ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  @Override
  public boolean endVariableLengthBlock(OutputStream output) throws IOException {
    if (output instanceof HdfsDataOutputStream) {
      HdfsDataOutputStream hdfs = (HdfsDataOutputStream) output;
      hdfs.hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.END_BLOCK));
      return true;
    }
    return false;
  }

  @Override
  public KeyProvider getHadoopKeyProvider(Configuration conf,
                                          Random random) throws IOException {
    return createKeyProvider(conf, random);
  }
}
