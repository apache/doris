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

import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Key;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestOutStream {

  @Test
  public void testFlush() throws Exception {
    PhysicalWriter.OutputReceiver receiver =
        Mockito.mock(PhysicalWriter.OutputReceiver.class);
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(128 * 1024)
        .withCodec(codec, codec.getDefaultOptions());
    try (OutStream stream = new OutStream("test", options, receiver)) {
      assertEquals(0L, stream.getBufferSize());
      stream.write(new byte[]{0, 1, 2});
      stream.flush();
      Mockito.verify(receiver).output(Mockito.any(ByteBuffer.class));
      assertEquals(0L, stream.getBufferSize());
    }
  }

  @Test
  public void testAssertBufferSizeValid() {
    try {
      OutStream.assertBufferSizeValid(1 + (1<<23));
      fail("Invalid buffer-size " + (1 + (1<<23)) + " should have been blocked.");
    }
    catch (IllegalArgumentException expected) {
      // Pass.
    }

    OutStream.assertBufferSizeValid((1<<23) -  1);
  }

  @Test
  public void testEncryption() throws Exception {
    TestInStream.OutputCollector receiver = new TestInStream.OutputCollector();
    EncryptionAlgorithm aes128 = EncryptionAlgorithm.AES_CTR_128;
    byte[] keyBytes = new byte[aes128.keyLength()];
    for(int i=0; i < keyBytes.length; ++i) {
      keyBytes[i] = (byte) i;
    }
    Key material = new SecretKeySpec(keyBytes, aes128.getAlgorithm());
    // test out stripe 18
    StreamOptions options = new StreamOptions(50)
        .withEncryption(aes128, material);
    options.modifyIv(CryptoUtils.modifyIvForStream(0x34,
        OrcProto.Stream.Kind.DATA, 18));
    try (OutStream stream = new OutStream("test", options, receiver)) {
      byte[] data = new byte[210];
      for (int i = 0; i < data.length; ++i) {
        data[i] = (byte) (i + 3);
      }

      // make 17 empty stripes for the stream
      for (int i = 0; i < 18; ++i) {
        stream.flush();
      }

      stream.write(data);
      stream.flush();
      byte[] output = receiver.buffer.get();

      // These are the outputs of aes256 with the key and incrementing ivs.
      // I included these hardcoded values to make sure that we are getting
      // AES128 encryption.
      //
      // I used http://extranet.cryptomathic.com/aescalc/index to compute these:
      // key: 000102030405060708090a0b0c0d0e0f
      // input: 00003400010000120000000000000000
      // ecb encrypt output: 822252A81CC7E7FE3E51F50E0E9B64B1
      int[] generated = new int[]{
          0x82, 0x22, 0x52, 0xA8, 0x1C, 0xC7, 0xE7, 0xFE, // block 0
          0x3E, 0x51, 0xF5, 0x0E, 0x0E, 0x9B, 0x64, 0xB1,

          0xF6, 0x4D, 0x36, 0xA9, 0xD9, 0xD7, 0x55, 0xDE, // block 1
          0xCB, 0xD5, 0x62, 0x0E, 0x6D, 0xA6, 0x6B, 0x16,

          0x00, 0x0B, 0xE8, 0xBA, 0x9D, 0xDE, 0x78, 0xEC, // block 2
          0x73, 0x05, 0xF6, 0x1E, 0x76, 0xD7, 0x9B, 0x7A,

          0x47, 0xE9, 0x61, 0x90, 0x65, 0x8B, 0x54, 0xAC, // block 3
          0xF2, 0x3F, 0x67, 0xAE, 0x25, 0x63, 0x1D, 0x4B,

          0x41, 0x48, 0xC4, 0x15, 0x5F, 0x2A, 0x7F, 0x91, // block 4
          0x9A, 0x87, 0xA1, 0x09, 0xFF, 0x68, 0x68, 0xCC,

          0xC0, 0x80, 0x52, 0xD4, 0xA5, 0x07, 0x4B, 0x79, // block 5
          0xC7, 0x08, 0x46, 0x46, 0x8C, 0x74, 0x2C, 0x0D,

          0x9F, 0x55, 0x7E, 0xA7, 0x17, 0x47, 0x91, 0xFD, // block 6
          0x01, 0xD4, 0x24, 0x1F, 0x76, 0xA1, 0xDC, 0xC3,

          0xEA, 0x13, 0x4C, 0x29, 0xCA, 0x68, 0x1E, 0x4F, // block 7
          0x0D, 0x19, 0xE5, 0x09, 0xF9, 0xC5, 0xF4, 0x15,

          0x9A, 0xAD, 0xC4, 0xA1, 0x0F, 0x28, 0xD4, 0x3D, // block 8
          0x59, 0xF0, 0x68, 0xD3, 0xC4, 0x98, 0x74, 0x68,

          0x37, 0xA4, 0xF4, 0x7C, 0x02, 0xCE, 0xC6, 0xCA, // block 9
          0xA1, 0xF8, 0xC3, 0x8C, 0x7B, 0x72, 0x38, 0xD1,

          0xAA, 0x52, 0x90, 0xDE, 0x28, 0xA1, 0x53, 0x6E, // block a
          0xA6, 0x5C, 0xC0, 0x89, 0xC4, 0x21, 0x76, 0xC0,

          0x1F, 0xED, 0x0A, 0xF9, 0xA2, 0xA7, 0xC1, 0x8D, // block b
          0xA0, 0x92, 0x44, 0x4F, 0x60, 0x51, 0x7F, 0xD8,

          0x6D, 0x16, 0xAF, 0x46, 0x1C, 0x27, 0x20, 0x1C, // block c
          0x01, 0xBD, 0xC5, 0x0B, 0x62, 0x3F, 0xEF, 0xEE,

          0x37, 0xae                                      // block d
      };
      assertEquals(generated.length, output.length);
      for (int i = 0; i < generated.length; ++i) {
        assertEquals((byte) (generated[i] ^ data[i]), output[i], "i = " + i);
      }

      receiver.buffer.clear();
      stream.changeIv(CryptoUtils.modifyIvForStripe(19));

      data = new byte[]{0x47, 0x77, 0x65, 0x6e};
      stream.write(data);
      stream.flush();
      output = receiver.buffer.get();
      generated = new int[]{0x16, 0x03, 0xE6, 0xC3};
      assertEquals(generated.length, output.length);
      for (int i = 0; i < generated.length; ++i) {
        assertEquals((byte) (generated[i] ^ data[i]), output[i], "i = " + i);
      }
    }
  }

  @Test
  public void testCompression256Encryption() throws Exception {
    // disable test if AES_256 is not available
    assumeTrue(InMemoryKeystore.SUPPORTS_AES_256);
    TestInStream.OutputCollector receiver = new TestInStream.OutputCollector();
    EncryptionAlgorithm aes256 = EncryptionAlgorithm.AES_CTR_256;
    byte[] keyBytes = new byte[aes256.keyLength()];
    for(int i=0; i < keyBytes.length; ++i) {
      keyBytes[i] = (byte) (i * 13);
    }
    Key material = new SecretKeySpec(keyBytes, aes256.getAlgorithm());
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(1024)
        .withCodec(codec, codec.getDefaultOptions())
        .withEncryption(aes256, material)
        .modifyIv(CryptoUtils.modifyIvForStream(0x1, OrcProto.Stream.Kind.DATA, 1));
    try (OutStream stream = new OutStream("test", options, receiver)) {
      for (int i = 0; i < 10000; ++i) {
        stream.write(("The Cheesy Poofs " + i + "\n")
                         .getBytes(StandardCharsets.UTF_8));
      }
      stream.flush();
    }
    // get the compressed, encrypted data
    byte[] encrypted = receiver.buffer.get();

    // decrypt it
    Cipher decrypt = aes256.createCipher();
    decrypt.init(Cipher.DECRYPT_MODE, material,
        new IvParameterSpec(options.getIv()));
    byte[] compressed = decrypt.doFinal(encrypted);

    // use InStream to decompress it
    BufferChunkList ranges = new BufferChunkList();
    ranges.add(new BufferChunk(ByteBuffer.wrap(compressed), 0));
    try (InStream decompressedStream = InStream.create("test", ranges.get(), 0,
             compressed.length,
             InStream.options().withCodec(new ZlibCodec()).withBufferSize(1024));
         BufferedReader reader
             = new BufferedReader(new InputStreamReader(decompressedStream,
             StandardCharsets.UTF_8))) {
      // check the contents of the decompressed stream
      for (int i = 0; i < 10000; ++i) {
        assertEquals("The Cheesy Poofs " + i, reader.readLine(), "i = " + i);
      }
      assertNull(reader.readLine());
    }
  }
}
