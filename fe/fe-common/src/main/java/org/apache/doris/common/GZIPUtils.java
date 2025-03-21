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

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPUtils {
    public static boolean isGZIPCompressed(byte[] data) {
        // From RFC 1952: 3.2. Members with a deflate compressed data stream (ID1 = 8, ID2 = 8)
        return data.length >= 2 && data[0] == (byte) 0x1F && data[1] == (byte) 0x8B;
    }

    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(bytesStream)) {
            gzipStream.write(data);
        }
        return bytesStream.toByteArray();
    }

    public static byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream bytesStream = new ByteArrayInputStream(data);
        try (GZIPInputStream gzipStream = new GZIPInputStream(bytesStream)) {
            return IOUtils.toByteArray(gzipStream);
        }
    }
}
