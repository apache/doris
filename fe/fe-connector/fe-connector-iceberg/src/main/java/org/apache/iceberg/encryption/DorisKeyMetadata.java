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

package org.apache.iceberg.encryption;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Minimal access to the SDK's package-private StandardKeyMetadata codec. */
public final class DorisKeyMetadata {
    private static final int MAX_METADATA_BYTES = 64 * 1024;

    private DorisKeyMetadata() {
    }

    /** Parse bounded v1 metadata with the SDK codec without advancing the caller's buffer. */
    public static NativeEncryptionKeyMetadata parse(ByteBuffer metadata) {
        ByteBuffer source = metadata.duplicate();
        if (!source.hasRemaining() || source.remaining() > MAX_METADATA_BYTES) {
            throw invalid();
        }
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        if (bytes[0] != 1) {
            throw new IllegalArgumentException("Iceberg encryption [UNSUPPORTED_KEY_METADATA_VERSION]");
        }
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(bytes, 1, bytes.length - 1);
            Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, null);
            long keyLength = decoder.readLong();
            if (keyLength != 16 && keyLength != 24 && keyLength != 32) {
                throw invalid();
            }
            skipBytes(decoder, input, keyLength);
            // Avro allocates from encoded lengths. A tiny corrupt buffer claiming a 2GB AAD
            // must fail before the SDK codec can allocate it; skipping never allocates that field.
            if (decoder.readIndex() != 1) {
                throw invalid();
            }
            skipBytes(decoder, input, decoder.readLong());
            int lengthBranch = decoder.readIndex();
            if (lengthBranch == 1) {
                if (decoder.readLong() < 0) {
                    throw invalid();
                }
            } else if (lengthBranch != 0) {
                throw invalid();
            }
            if (input.available() != 0) {
                throw invalid();
            }
            // Parse the validated snapshot, rather than a buffer shared with manifest-cache readers.
            return StandardKeyMetadata.parse(ByteBuffer.wrap(bytes));
        } catch (IOException | RuntimeException e) {
            // Codec exceptions can contain binary values. Do not retain their message or cause.
            throw invalid();
        }
    }

    private static void skipBytes(Decoder decoder, ByteArrayInputStream input, long length) throws IOException {
        if (length < 0 || length > input.available()) {
            throw invalid();
        }
        decoder.skipFixed((int) length);
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Iceberg encryption [INVALID_KEY_METADATA]");
    }
}
