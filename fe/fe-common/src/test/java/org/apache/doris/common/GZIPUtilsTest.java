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

package org.apache.doris.common;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;

public class GZIPUtilsTest {
    @Test
    public void testCompressFileRespectsMaxCompressedSize() throws IOException {
        File file = File.createTempFile("gzip_utils_bound_", ".bin");
        file.deleteOnExit();
        // Low-compressibility payload so compressed size stays close to input size.
        byte[] payload = new byte[64 * 1024];
        new Random(0).nextBytes(payload);
        Files.write(file.toPath(), payload);

        byte[] compressed = GZIPUtils.compress(file, Integer.MAX_VALUE);
        Assert.assertTrue(GZIPUtils.isGZIPCompressed(compressed));
        Assert.assertArrayEquals(payload, GZIPUtils.decompress(compressed));

        try {
            GZIPUtils.compress(file, 32);
            Assert.fail("expected IOException when compressed size exceeds limit");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("compressed size exceeds limit"));
        }
    }
}
