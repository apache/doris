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

import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.orc.EncryptionAlgorithm;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.util.List;
import java.util.Random;

/**
 * Shim implementation for ORC's KeyProvider API that uses Hadoop's
 * KeyProvider API and implementations. Most users use a Hadoop or Ranger
 * KMS and thus should use this default implementation.
 * <p>
 * The main two methods of ORC's KeyProvider are createLocalKey and
 * decryptLocalKey. These are very similar to Hadoop's
 * <pre>
 *   EncryptedKeyVersion generateEncryptedKey(String keyVersionName);
 *   KeyVersion decryptEncryptedKey(EncryptedKeyVersion encrypted)
 * </pre>
 * but there are some important differences.
 * <ul>
 * <li>Hadoop's generateEncryptedKey doesn't return the decrypted key, so it
 *     would require two round trips (generateEncryptedKey and then
 *     decryptEncryptedKey)to the KMS.</li>
 * <li>Hadoop's methods require storing both the IV and the encrypted key, so
 *     for AES256, it is 48 random bytes.</li>
 * </ul>
 * <p>
 * However, since the encryption in the KMS is using AES/CTR we know that the
 * flow is:
 *
 * <pre>
 *   tmpKey = aes(masterKey, iv);
 *   cypher = xor(tmpKey, plain);
 * </pre>
 * <p>
 * which means that encryption and decryption are symmetric. Therefore, if we
 * use the KMS' decryptEncryptedKey, and feed in a random iv and the right
 * number of 0's as the encrypted key, we get the right length of a tmpKey.
 * Since it is symmetric, we can use it for both encryption and decryption
 * and we only need to store the random iv. Since the iv is 16 bytes, it is
 * only a third the size of the other solution, and only requires one trip to
 * the KMS.
 * <p>
 * So the flow looks like:
 * <pre>
 *   encryptedKey = securely random 16 or 32 bytes
 *   iv = first 16 byte of encryptedKey
 *   --- on KMS ---
 *   tmpKey0 = aes(masterKey, iv)
 *   tmpKey1 = aes(masterKey, iv+1)
 *   decryptedKey0 = xor(tmpKey0, encryptedKey0)
 *   decryptedKey1 = xor(tmpKey1, encryptedKey1)
 * </pre>
 * <p>
 * In the long term, we should probably fix Hadoop's generateEncryptedKey
 * to either take the random key or pass it back.
 */
class KeyProviderImpl implements KeyProvider {
  private final org.apache.hadoop.crypto.key.KeyProvider provider;
  private final Random random;

  KeyProviderImpl(org.apache.hadoop.crypto.key.KeyProvider provider,
                  Random random) {
    this.provider = provider;
    this.random = random;
  }

  @Override
  public List<String> getKeyNames() throws IOException {
    return provider.getKeys();
  }

  @Override
  public HadoopShims.KeyMetadata getCurrentKeyVersion(String keyName) throws IOException {
    org.apache.hadoop.crypto.key.KeyProvider.Metadata meta =
        provider.getMetadata(keyName);
    return new HadoopShims.KeyMetadata(keyName, meta.getVersions() - 1,
        HadoopShimsCurrent.findAlgorithm(meta));
  }

  /**
   * The Ranger/Hadoop KMS mangles the IV by bit flipping it in a misguided
   * attempt to improve security. By bit flipping it here, we undo the
   * silliness so that we get
   *
   * @param input  the input array to copy from
   * @param output the output array to write to
   */
  private static void unmangleIv(byte[] input, byte[] output) {
    for (int i = 0; i < output.length && i < input.length; ++i) {
      output[i] = (byte) (0xff ^ input[i]);
    }
  }

  @Override
  public LocalKey createLocalKey(HadoopShims.KeyMetadata key) throws IOException {
    EncryptionAlgorithm algorithm = key.getAlgorithm();
    byte[] encryptedKey = new byte[algorithm.keyLength()];
    random.nextBytes(encryptedKey);
    byte[] iv = new byte[algorithm.getIvLength()];
    unmangleIv(encryptedKey, iv);
    EncryptedKeyVersion param = EncryptedKeyVersion.createForDecryption(
        key.getKeyName(), HadoopShimsCurrent.buildKeyVersionName(key), iv, encryptedKey);
    try {
      KeyProviderCryptoExtension.KeyVersion decryptedKey;
      if (provider instanceof KeyProviderCryptoExtension) {
        decryptedKey = ((KeyProviderCryptoExtension) provider).decryptEncryptedKey(param);
      } else if (provider instanceof CryptoExtension) {
        decryptedKey = ((CryptoExtension) provider).decryptEncryptedKey(param);
      } else {
        throw new UnsupportedOperationException(
            provider.getClass().getCanonicalName() + " is not supported.");
      }
      return new LocalKey(algorithm, decryptedKey.getMaterial(),
          encryptedKey);
    } catch (GeneralSecurityException e) {
      throw new IOException("Can't create local encryption key for " + key, e);
    }
  }

  @Override
  public Key decryptLocalKey(HadoopShims.KeyMetadata key,
                             byte[] encryptedKey) throws IOException {
    EncryptionAlgorithm algorithm = key.getAlgorithm();
    byte[] iv = new byte[algorithm.getIvLength()];
    unmangleIv(encryptedKey, iv);
    EncryptedKeyVersion param = EncryptedKeyVersion.createForDecryption(
        key.getKeyName(), HadoopShimsCurrent.buildKeyVersionName(key), iv, encryptedKey);
    try {
      KeyProviderCryptoExtension.KeyVersion decryptedKey;
      if (provider instanceof KeyProviderCryptoExtension) {
        decryptedKey = ((KeyProviderCryptoExtension) provider).decryptEncryptedKey(param);
      } else if (provider instanceof CryptoExtension) {
        decryptedKey = ((CryptoExtension) provider).decryptEncryptedKey(param);
      } else {
        throw new UnsupportedOperationException(
            provider.getClass().getCanonicalName() + " is not supported.");
      }
      return new SecretKeySpec(decryptedKey.getMaterial(),
          algorithm.getAlgorithm());
    } catch (GeneralSecurityException e) {
      return null;
    }
  }

  @Override
  public HadoopShims.KeyProviderKind getKind() {
    return HadoopShims.KeyProviderKind.HADOOP;
  }
}
