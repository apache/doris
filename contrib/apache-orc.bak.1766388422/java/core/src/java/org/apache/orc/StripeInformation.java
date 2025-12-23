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
 * Information about the stripes in an ORC file that is provided by the Reader.
 */
public interface StripeInformation {
  /**
   * Get the byte offset of the start of the stripe.
   * @return the bytes from the start of the file
   */
  long getOffset();

  /**
   * Get the total length of the stripe in bytes.
   * @return the number of bytes in the stripe
   */
  long getLength();

  /**
   * Get the length of the stripe's indexes.
   * @return the number of bytes in the index
   */
  long getIndexLength();

  /**
   * Get the length of the stripe's data.
   * @return the number of bytes in the stripe
   */
  long getDataLength();

  /**
   * Get the length of the stripe's tail section, which contains its index.
   * @return the number of bytes in the tail
   */
  long getFooterLength();

  /**
   * Get the number of rows in the stripe.
   * @return a count of the number of rows
   */
  long getNumberOfRows();

  /**
   * Get the index of this stripe in the current file.
   * @return 0 to number_of_stripes - 1
   */
  long getStripeId();

  /**
   * Does this stripe have an explicit encryption stripe id set?
   * @return true if this stripe was the first stripe of a merge
   */
  boolean hasEncryptionStripeId();

  /**
   * Get the original stripe id that was used when the stripe was originally
   * written. This is only different that getStripeId in merged files.
   * @return the original stripe id + 1
   */
  long getEncryptionStripeId();

  /**
   * Get the encrypted keys starting from this stripe until overridden by
   * a new set in a following stripe. The top level array is one for each
   * encryption variant. Each element is an encrypted key.
   * @return the array of encrypted keys
   */
  byte[][] getEncryptedLocalKeys();
}
