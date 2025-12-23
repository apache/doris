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

import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.Random;

/**
 * A source of crypto keys. This is usually backed by a Ranger KMS.
 */
public interface KeyProvider {

  /**
   * Get the list of key names from the key provider.
   * @return a list of key names
   */
  List<String> getKeyNames() throws IOException;

  /**
   * Get the current metadata for a given key. This is used when encrypting
   * new data.
   *
   * @param keyName the name of a key
   * @return metadata for the current version of the key
   * @throws IllegalArgumentException if the key is unknown
   */
  HadoopShims.KeyMetadata getCurrentKeyVersion(String keyName) throws IOException;

  /**
   * Create a local key for the given key version. This local key will be
   * randomly generated and encrypted with the given version of the master
   * key. The encryption and decryption is done with the local key and the
   * user process never has access to the master key, because it stays on the
   * Ranger KMS.
   *
   * @param key the master key version
   * @return the local key's material both encrypted and unencrypted
   */
  LocalKey createLocalKey(HadoopShims.KeyMetadata key) throws IOException;

  /**
   * Decrypt a local key for reading a file.
   *
   * @param key the master key version
   * @param encryptedKey the encrypted key
   * @return the decrypted local key's material or null if the key is not
   * available
   */
  Key decryptLocalKey(HadoopShims.KeyMetadata key, byte[] encryptedKey) throws IOException;

  /**
   * Get the kind of this provider.
   */
  HadoopShims.KeyProviderKind getKind();

  /**
   * A service loader factory interface.
   */
  interface Factory {
    KeyProvider create(String kind,
                       Configuration conf,
                       Random random) throws IOException;
  }
}
