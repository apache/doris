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

import java.io.IOException;
import java.security.Key;

/**
 * Information about a column encryption variant.
 * <p>
 * Column encryption is done by encoding multiple variants of the same column.
 * Each encrypted column ends up in two variants:
 * <ul>
 *   <li>Encrypted original</li>
 *   <li>Unencrypted masked</li>
 * </ul>
 */
public interface EncryptionVariant extends Comparable<EncryptionVariant> {

  /**
   * Get the key description for this column. This description is global to the
   * file and is passed to the KeyProvider along with various encrypted local
   * keys for the stripes or file footer so that it can decrypt them.
   * @return the encryption key description
   */
  EncryptionKey getKeyDescription();

  /**
   * Get the root column for this variant.
   * @return the root column type
   */
  TypeDescription getRoot();

  /**
   * Get the encryption variant id within the file.
   */
  int getVariantId();

  /**
   * Get the local key for the footer.
   * @return the local decrypted key or null if it isn't available
   */
  Key getFileFooterKey() throws IOException;

  /**
   * Get the local key for a stripe's data or footer.
   * @param stripe the stripe within the file (0 to N-1)
   * @return the local decrypted key or null if it isn't available
   */
  Key getStripeKey(long stripe) throws IOException;
}
