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

package org.apache.doris.nereids.stats;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Writes the structural identity an hbo key is the sha256 of: every value is length prefixed and every
 * number is written in a fixed width, so the byte stream is injective over the tree by construction and a
 * value which carries a character of the printed grammar cannot stand for a field of its own.
 */
final class ShapeKeyWriter {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream(512);
    private final DataOutputStream data = new DataOutputStream(bytes);

    void tag(int value) {
        writeInt(value);
    }

    void flag(boolean value) {
        writeInt(value ? 1 : 0);
    }

    void count(int value) {
        writeInt(value);
    }

    void number(long value) {
        try {
            data.writeLong(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void text(String value) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        writeInt(raw.length);
        try {
            data.write(raw);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A raw value which already is a key: its length is written, not its content re-encoded. */
    void bytes(byte[] value) {
        writeInt(value.length);
        try {
            data.write(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    byte[] toByteArray() {
        return bytes.toByteArray();
    }

    private void writeInt(int value) {
        try {
            data.writeInt(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
