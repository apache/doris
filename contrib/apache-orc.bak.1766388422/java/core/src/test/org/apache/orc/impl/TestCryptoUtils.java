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
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCryptoUtils {

  @Test
  public void testCreateStreamIv() throws Exception {
    EncryptionAlgorithm aes128 = EncryptionAlgorithm.AES_CTR_128;
    byte[] iv = new byte[aes128.getIvLength()];
    CryptoUtils.modifyIvForStream(0x234567, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
        0x123456).accept(iv);
    assertEquals(16, iv.length);
    assertEquals(0x23, iv[0]);
    assertEquals(0x45, iv[1]);
    assertEquals(0x67, iv[2]);
    assertEquals(0x0, iv[3]);
    assertEquals(0x8, iv[4]);
    assertEquals(0x12, iv[5]);
    assertEquals(0x34, iv[6]);
    assertEquals(0x56, iv[7]);
  }

  @Test
  public void testMemoryKeyProvider() throws IOException {
    Configuration conf = new Configuration();
    OrcConf.KEY_PROVIDER.setString(conf, "memory");
    // Hard code the random so that we know the bytes that will come out.
    InMemoryKeystore provider =
        (InMemoryKeystore) CryptoUtils.getKeyProvider(conf, new Random(24));
    byte[] piiKey = new byte[]{0,1,2,3,4,5,6,7,8,9,0xa,0xb,0xc,0xd,0xe,0xf};
    provider.addKey("pii", EncryptionAlgorithm.AES_CTR_128, piiKey);
    byte[] piiKey2 = new byte[]{0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,
        0x18,0x19,0x1a,0x1b,0x1c,0x1d,0x1e,0x1f};
    provider.addKey("pii", 1, EncryptionAlgorithm.AES_CTR_128, piiKey2);
    byte[] secretKey = new byte[]{0x20,0x21,0x22,0x23,0x24,0x25,0x26,0x27,
        0x28,0x29,0x2a,0x2b,0x2c,0x2d,0x2e,0x2f};
    provider.addKey("secret", EncryptionAlgorithm.AES_CTR_128, secretKey);

    List<String> keyNames = provider.getKeyNames();
    assertEquals(2, keyNames.size());
    assertTrue(keyNames.contains("pii"));
    assertTrue(keyNames.contains("secret"));
    HadoopShims.KeyMetadata meta = provider.getCurrentKeyVersion("pii");
    assertEquals(1, meta.getVersion());
    LocalKey localKey = provider.createLocalKey(meta);
    byte[] encrypted = localKey.getEncryptedKey();
    // make sure that we get exactly what we expect to test the encryption
    assertEquals("c7 ab 4f bb 38 f4 de ad d0 b3 59 e2 21 2a 95 32",
        new BytesWritable(encrypted).toString());
    // now check to make sure that we get the expected bytes back
    assertEquals("c7 a1 d0 41 7b 24 72 44 1a 58 c7 72 4a d4 be b3",
        new BytesWritable(localKey.getDecryptedKey().getEncoded()).toString());
    Key key = provider.decryptLocalKey(meta, encrypted);
    assertEquals(new BytesWritable(localKey.getDecryptedKey().getEncoded()).toString(),
        new BytesWritable(key.getEncoded()).toString());
  }

  @Test
  public void testInvalidKeyProvider() throws IOException {
    Configuration conf = new Configuration();
    OrcConf.KEY_PROVIDER.setString(conf, "");
    assertNull(CryptoUtils.getKeyProvider(conf, new Random()));
  }
}
