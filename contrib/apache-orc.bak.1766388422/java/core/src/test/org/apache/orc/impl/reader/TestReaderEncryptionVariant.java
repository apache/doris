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

package org.apache.orc.impl.reader;

import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;

public class TestReaderEncryptionVariant {

  @Test
  public void testInvalidKeyProvider() throws IOException {
    OrcProto.EncryptionAlgorithm algorithm = OrcProto.EncryptionAlgorithm.AES_CTR_256;
    ReaderEncryptionKey key =
        new ReaderEncryptionKey(OrcProto.EncryptionKey.newBuilder().setAlgorithm(algorithm).build());
    List<StripeInformation> strips = new ArrayList<>();
    ReaderEncryptionVariant readerEncryptionVariant =
        new ReaderEncryptionVariant(key, 0, null, null, strips, 0L, null, null);

    assertNull(readerEncryptionVariant.getFileFooterKey());
    assertNull(readerEncryptionVariant.getStripeKey(0L));
  }
}
