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

/**
 * Information about a key used for column encryption in an ORC file.
 */
public interface EncryptionKey extends Comparable<EncryptionKey> {

  /**
   * The name of the key.
   * @return the name
   */
  String getKeyName();

  /**
   * The version of the key.
   * @return the version, which for most KeyProviders start at 0.
   */
  int getKeyVersion();

  /**
   * The encryption algorithm for this key.
   * @return the encryption algorithm
   */
  EncryptionAlgorithm getAlgorithm();

  /**
   * The columns that are encrypted with this key.
   * @return the list of columns
   */
  EncryptionVariant[] getEncryptionRoots();

  /**
   * Is the key available to this user?
   * @return true if the key is available
   */
  boolean isAvailable();
}
