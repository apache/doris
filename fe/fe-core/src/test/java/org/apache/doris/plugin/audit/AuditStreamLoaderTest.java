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

package org.apache.doris.plugin.audit;

import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class AuditStreamLoaderTest {

    @Test
    public void testConnectionUsesGzipCompression() throws Exception {
        HttpURLConnection connection = Deencapsulation.invoke(AuditStreamLoader.class, "getConnection",
                "http://127.0.0.1:8030/api/db/table/_stream_load?", "label", "token");

        Assertions.assertEquals("gz", connection.getRequestProperty("compress_type"));
    }

    @Test
    public void testWriteCompressedBody() throws Exception {
        String payload = "audit row 中文\u001fselect 1\u001e";
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        Deencapsulation.invoke(AuditStreamLoader.class, "writeCompressedBody",
                compressed, new StringBuilder(payload));

        byte[] bytes = compressed.toByteArray();
        Assertions.assertEquals(0x1f, bytes[0] & 0xff);
        Assertions.assertEquals(0x8b, bytes[1] & 0xff);
        Assertions.assertEquals(payload, decompress(bytes));
    }

    private static String decompress(byte[] compressed) throws IOException {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressed));
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
