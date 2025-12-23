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

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.security.NoSuchAlgorithmException;

/**
 * The encryption algorithms supported by ORC.
 * <p>
 * This class can't reference any of the newer Hadoop classes.
 */
public enum EncryptionAlgorithm {
  AES_CTR_128("AES", "CTR/NoPadding", 16, 1),
  AES_CTR_256("AES", "CTR/NoPadding", 32, 2);

  private final String algorithm;
  private final String mode;
  private final int keyLength;
  private final int serialization;
  private final byte[] zero;

  EncryptionAlgorithm(String algorithm, String mode, int keyLength,
                      int serialization) {
    this.algorithm = algorithm;
    this.mode = mode;
    this.keyLength = keyLength;
    this.serialization = serialization;
    zero = new byte[keyLength];
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public int getIvLength() {
    return 16;
  }

  public Cipher createCipher() {
    try {
      return Cipher.getInstance(algorithm + "/" + mode);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Bad algorithm " + algorithm);
    } catch (NoSuchPaddingException e) {
      throw new IllegalArgumentException("Bad padding " + mode);
    }
  }

  public int keyLength() {
    return keyLength;
  }

  public byte[] getZeroKey() {
    return zero;
  }

  /**
   * Get the serialization code for this enumeration.
   * @return the serialization value
   */
  public int getSerialization() {
    return serialization;
  }

  /**
   * Get the serialization code for this enumeration.
   * @return the serialization value
   */
  public static EncryptionAlgorithm fromSerialization(int serialization) {
    for(EncryptionAlgorithm algorithm: values()) {
      if (algorithm.serialization == serialization) {
        return algorithm;
      }
    }
    throw new IllegalArgumentException("Unknown code in encryption algorithm " +
        serialization);
  }

  @Override
  public String toString() {
    return algorithm + (keyLength * 8);
  }
}
