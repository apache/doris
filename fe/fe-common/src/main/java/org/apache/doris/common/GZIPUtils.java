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
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    public static byte[] compress(File file) throws IOException {
        return compress(file, Integer.MAX_VALUE);
    }

    /**
     * Stream-compress {@code file} with an 8KB read buffer.
     * Fails fast if the compressed output would exceed {@code maxCompressedSize}
     * so callers can avoid unbounded FE heap growth.
     */
    public static byte[] compress(File file, int maxCompressedSize) throws IOException {
        if (maxCompressedSize <= 0) {
            throw new IOException("maxCompressedSize must be positive, got " + maxCompressedSize);
        }
        ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
        try (FileInputStream fileInputStream = new FileInputStream(file);
                GZIPOutputStream gzipStream = new GZIPOutputStream(
                        new BoundedSizeOutputStream(bytesStream, maxCompressedSize))) {
            byte[] buffer = new byte[8192]; // 8KB buffer
            int bytesRead;
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                gzipStream.write(buffer, 0, bytesRead);
            }
        }
        return bytesStream.toByteArray();
    }

    public static byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream bytesStream = new ByteArrayInputStream(data);
        try (GZIPInputStream gzipStream = new GZIPInputStream(bytesStream)) {
            return IOUtils.toByteArray(gzipStream);
        }
    }

    public static InputStream lazyDecompress(byte[] data) throws IOException {
        return new GZIPInputStream(new ByteArrayInputStream(data));
    }

    /**
     * Counts written bytes and rejects writes that would exceed {@code maxSize}.
     * Used so gzip can fail during streaming instead of after a multi-GB buffer is filled.
     */
    private static final class BoundedSizeOutputStream extends FilterOutputStream {
        private final int maxSize;
        private int written;

        BoundedSizeOutputStream(OutputStream out, int maxSize) {
            super(out);
            this.maxSize = maxSize;
        }

        @Override
        public void write(int b) throws IOException {
            ensureCapacity(1);
            out.write(b);
            written++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ensureCapacity(len);
            out.write(b, off, len);
            written += len;
        }

        private void ensureCapacity(int len) throws IOException {
            if (len < 0 || (long) written + len > maxSize) {
                throw new IOException(
                        String.format("compressed size exceeds limit %d bytes", maxSize));
            }
        }
    }
}
