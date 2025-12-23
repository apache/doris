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

import org.apache.orc.EncryptionAlgorithm;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

/**
 * Local keys are random keys that are generated for each file and column.
 * The file's metadata includes the encryptedKey and the reader needs to
 * use the KeyProvider to get the decryptedKey.
 */
public class LocalKey {
  private final byte[] encryptedKey;
  private Key decryptedKey;

  public LocalKey(EncryptionAlgorithm algorithm,
                  byte[] decryptedKey,
                  byte[] encryptedKey) {
    this.encryptedKey = encryptedKey;
    if (decryptedKey != null) {
      setDecryptedKey(new SecretKeySpec(decryptedKey, algorithm.getAlgorithm()));
    }
  }

  public void setDecryptedKey(Key key) {
    decryptedKey = key;
  }

  public Key getDecryptedKey() {
    return decryptedKey;
  }

  public byte[] getEncryptedKey() {
    return encryptedKey;
  }
}
