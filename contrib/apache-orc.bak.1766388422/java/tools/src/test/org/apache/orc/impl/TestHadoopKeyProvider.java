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
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHadoopKeyProvider {

  /**
   * Tests the path through the hadoop key provider code base.
   * This should be consistent with TestCryptoUtils.testMemoryKeyProvider.
   * @throws IOException
   */
  @Test
  public void testHadoopKeyProvider() throws IOException {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.key.provider.path", "test:///");
    // Hard code the random so that we know the bytes that will come out.
    KeyProvider provider = CryptoUtils.getKeyProvider(conf, new Random(24));
    List<String> keyNames = provider.getKeyNames();
    assertEquals(2, keyNames.size());
    assertTrue(keyNames.contains("pii"));
    assertTrue(keyNames.contains("secret"));
    HadoopShims.KeyMetadata piiKey = provider.getCurrentKeyVersion("pii");
    assertEquals(1, piiKey.getVersion());
    LocalKey localKey = provider.createLocalKey(piiKey);
    byte[] encrypted = localKey.getEncryptedKey();
    // make sure that we get exactly what we expect to test the encryption
    assertEquals("c7 ab 4f bb 38 f4 de ad d0 b3 59 e2 21 2a 95 32",
        new BytesWritable(encrypted).toString());
    // now check to make sure that we get the expected bytes back
    assertEquals("c7 a1 d0 41 7b 24 72 44 1a 58 c7 72 4a d4 be b3",
        new BytesWritable(localKey.getDecryptedKey().getEncoded()).toString());
    Key key = provider.decryptLocalKey(piiKey, encrypted);
    assertEquals(new BytesWritable(localKey.getDecryptedKey().getEncoded()).toString(),
        new BytesWritable(key.getEncoded()).toString());
  }
}
