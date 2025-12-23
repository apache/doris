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

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.orc.EncryptionAlgorithm;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHadoopShimsPre2_7 {

  @Test
  public void testFindingUnknownEncryption() {
    assertThrows(IllegalArgumentException.class, () -> {
      KeyProvider.Metadata meta = new KMSClientProvider.KMSMetadata(
          "XXX/CTR/NoPadding", 128, "", new HashMap<String, String>(),
          new Date(0), 1);
      HadoopShimsCurrent.findAlgorithm(meta);
    });
  }

  @Test
  public void testFindingAesEncryption()  {
    KeyProvider.Metadata meta = new KMSClientProvider.KMSMetadata(
        "AES/CTR/NoPadding", 128, "", new HashMap<String, String>(),
        new Date(0), 1);
    assertEquals(EncryptionAlgorithm.AES_CTR_128,
        HadoopShimsCurrent.findAlgorithm(meta));
    meta = new KMSClientProvider.KMSMetadata(
        "AES/CTR/NoPadding", 256, "", new HashMap<String, String>(),
        new Date(0), 1);
    assertEquals(EncryptionAlgorithm.AES_CTR_256,
        HadoopShimsCurrent.findAlgorithm(meta));
    meta = new KMSClientProvider.KMSMetadata(
        "AES/CTR/NoPadding", 512, "", new HashMap<String, String>(),
        new Date(0), 1);
    assertEquals(EncryptionAlgorithm.AES_CTR_256,
        HadoopShimsCurrent.findAlgorithm(meta));
  }
}
