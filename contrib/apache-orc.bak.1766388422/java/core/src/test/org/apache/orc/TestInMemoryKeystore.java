/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.LocalKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test {@link InMemoryKeystore} class
 */
public class TestInMemoryKeystore {

  private InMemoryKeystore memoryKeystore;

  public TestInMemoryKeystore() {
    super();
  }

  @BeforeEach
  public void init() throws IOException {
    // For testing, use a fixed random number generator so that everything
    // is repeatable.
    Random random = new Random(2);
    memoryKeystore =
        new InMemoryKeystore(random)
            .addKey("key128", EncryptionAlgorithm.AES_CTR_128, "123".getBytes(StandardCharsets.UTF_8))
            .addKey("key256", EncryptionAlgorithm.AES_CTR_256, "secret123".getBytes(StandardCharsets.UTF_8))
            .addKey("key256short", EncryptionAlgorithm.AES_CTR_256, "5".getBytes(StandardCharsets.UTF_8));

  }

  private static String stringify(byte[] buffer) {
    return new BytesWritable(buffer).toString();
  }

  @Test
  public void testGetKeyNames() {

    assertTrue(memoryKeystore.getKeyNames().contains("key128"));
    assertTrue(memoryKeystore.getKeyNames().contains("key256"));
    assertTrue(memoryKeystore.getKeyNames().contains("key256short"));

  }

  @Test
  public void testGetCurrentKeyVersion() {

    final HadoopShims.KeyMetadata metadata = memoryKeystore
        .getCurrentKeyVersion("key256");

    assertEquals("key256", metadata.getKeyName());
    if (InMemoryKeystore.SUPPORTS_AES_256) {
      assertEquals(EncryptionAlgorithm.AES_CTR_256, metadata.getAlgorithm());
    } else {
      assertEquals(EncryptionAlgorithm.AES_CTR_128, metadata.getAlgorithm());
    }

    assertEquals(0, metadata.getVersion());

  }

  @Test
  public void testGetLocalKey() {

    HadoopShims.KeyMetadata metadata128 = memoryKeystore
        .getCurrentKeyVersion("key128");

    LocalKey key128 = memoryKeystore.createLocalKey(metadata128);
    // we are sure the key is the same because of the random generator.
    assertEquals("39 72 2c bb f8 b9 1a 4b 90 45 c5 e6 17 5f 10 01",
        stringify(key128.getEncryptedKey()));
    assertEquals("46 33 66 fd 79 57 66 9a ba 4a 28 df bf 16 f2 88",
        stringify(key128.getDecryptedKey().getEncoded()));
    // used online aes/cbc calculator to encrypt key
    assertEquals("AES", key128.getDecryptedKey().getAlgorithm());

    // now decrypt the key again
    Key decryptKey = memoryKeystore.decryptLocalKey(metadata128,
        key128.getEncryptedKey());
    assertEquals(stringify(key128.getDecryptedKey().getEncoded()),
        stringify(decryptKey.getEncoded()));

    HadoopShims.KeyMetadata metadata256 = memoryKeystore
        .getCurrentKeyVersion("key256");
    LocalKey key256 = memoryKeystore.createLocalKey(metadata256);
    // this is forced by the fixed Random in the keystore for this test
    if (InMemoryKeystore.SUPPORTS_AES_256) {
      assertEquals("ea c3 2f 7f cd 5e cc da 5c 6e 62 fc 4e 63 85 08 0f " +
                              "7b 6c db 79 e5 51 ec 9c 9c c7 fc bd 60 ee 73",
          stringify(key256.getEncryptedKey()));
       // used online aes/cbc calculator to encrypt key
      assertEquals("00 b0 1c 24 d9 03 bc 02 63 87 b3 f9 65 4e e7 a8 b8" +
                              " 58 eb a0 81 06 b3 61 cf f8 06 ba 30 d4 c5 36",
          stringify(key256.getDecryptedKey().getEncoded()));
    } else {
      assertEquals("ea c3 2f 7f cd 5e cc da 5c 6e 62 fc 4e 63 85 08",
          stringify(key256.getEncryptedKey()));
      assertEquals("6d 1c ff 55 a5 44 75 11 fb e6 8e 08 cd 2a 10 e8",
          stringify(key256.getDecryptedKey().getEncoded()));
    }
    assertEquals("AES", key256.getDecryptedKey().getAlgorithm());

    // now decrypt the key again
    decryptKey = memoryKeystore.decryptLocalKey(metadata256, key256.getEncryptedKey());
    assertEquals(stringify(key256.getDecryptedKey().getEncoded()),
        stringify(decryptKey.getEncoded()));
  }

  @Test
  public void testRollNewVersion() throws IOException {

    assertEquals(0,
        memoryKeystore.getCurrentKeyVersion("key128").getVersion());
    memoryKeystore.addKey("key128", 1, EncryptionAlgorithm.AES_CTR_128, "NewSecret".getBytes(StandardCharsets.UTF_8));
    assertEquals(1,
        memoryKeystore.getCurrentKeyVersion("key128").getVersion());
  }

  @Test
  public void testDuplicateKeyNames() {
    try {
      memoryKeystore.addKey("key128", 0, EncryptionAlgorithm.AES_CTR_128,
          "exception".getBytes(StandardCharsets.UTF_8));
      fail("Keys with same name cannot be added.");
    } catch (IOException e) {
      assertTrue(e.toString().contains("equal or higher version"));
    }

  }

  /**
   * This will test:
   * 1. Scenario where key with smaller version then existing should not be allowed
   * 2. Test multiple versions of the key
   * 3. Test get current version
   * 4. Ensure the different versions of the key have different material.
   */
  @Test
  public void testMultipleVersion() throws IOException {
    assertEquals(0,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());
    memoryKeystore.addKey("key256", 1, EncryptionAlgorithm.AES_CTR_256,
        "NewSecret".getBytes(StandardCharsets.UTF_8));
    assertEquals(1,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());

    try {
      memoryKeystore.addKey("key256", 1, EncryptionAlgorithm.AES_CTR_256,
          "BadSecret".getBytes(StandardCharsets.UTF_8));
      fail("Keys with smaller version should not be added.");
    } catch (final IOException e) {
      assertTrue(e.toString().contains("equal or higher version"));
    }

    memoryKeystore.addKey("key256", 2, EncryptionAlgorithm.AES_CTR_256,
        "NewerSecret".getBytes(StandardCharsets.UTF_8));
    assertEquals(2,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());

    // make sure that all 3 versions of key256 exist and have different secrets
    Key key0 = memoryKeystore.decryptLocalKey(
        new HadoopShims.KeyMetadata("key256", 0, EncryptionAlgorithm.AES_CTR_256),
        new byte[16]);
    Key key1 = memoryKeystore.decryptLocalKey(
        new HadoopShims.KeyMetadata("key256", 1, EncryptionAlgorithm.AES_CTR_256),
        new byte[16]);
    Key key2 = memoryKeystore.decryptLocalKey(
        new HadoopShims.KeyMetadata("key256", 2, EncryptionAlgorithm.AES_CTR_256),
        new byte[16]);
    assertNotEquals(new BytesWritable(key0.getEncoded()).toString(),
        new BytesWritable(key1.getEncoded()).toString());
    assertNotEquals(new BytesWritable(key1.getEncoded()).toString(),
        new BytesWritable(key2.getEncoded()).toString());
  }

}
