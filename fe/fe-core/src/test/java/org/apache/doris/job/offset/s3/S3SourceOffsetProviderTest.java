// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.job.offset.s3;

import org.apache.doris.filesystem.spi.ObjectStorageGlob;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class S3SourceOffsetProviderTest {

    @Test
    void hasMoreDataToConsumeUsesUtf8BinaryOrder() {
        String privateUseKey = "data/" + Character.toString(0xE000) + "/file.csv";
        String emojiKey = "data/" + Character.toString(0x1F600) + "/file.csv";
        Assertions.assertTrue(privateUseKey.compareTo(emojiKey) > 0);
        Assertions.assertTrue(ObjectStorageGlob.compareUtf8Binary(privateUseKey, emojiKey) < 0);

        S3Offset currentOffset = new S3Offset();
        currentOffset.setEndFile(privateUseKey);
        S3SourceOffsetProvider provider = new S3SourceOffsetProvider();
        provider.currentOffset = currentOffset;
        provider.maxEndFile = emojiKey;

        Assertions.assertTrue(provider.hasMoreDataToConsume());
    }
}
