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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.thrift.TLanceIndexJobDispatch;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Wire payload bounds of one Lance index dispatch, mirroring the BE-side protocol
 * limits of the worker channel: the supervisor decodes a length-prefixed dispatch
 * frame of at most {@link #MAX_DISPATCH_BYTES} carrying at most
 * {@link #MAX_STORAGE_OPTIONS} storage options, each key bounded to
 * {@link #MAX_STORAGE_OPTION_KEY_BYTES} UTF-8 bytes and each value to
 * {@link #MAX_STORAGE_OPTION_VALUE_BYTES} UTF-8 bytes. The FE validates the same
 * bounds before the first byte of network I/O, so a violation is a
 * determined-never-sent failure that converges the job NOT_COMMITTED with the
 * internal NEVER_LAUNCHED proof, never UNKNOWN.
 *
 * <p>The constants live here (not in the dispatcher) so the wire contract test can
 * pin them against a maximal legal fixture. Validation messages name the violated
 * bound only: they never carry storage-option keys or values, so they are safe to
 * persist as the durable sanitized message of the rejection result.
 */
public final class LanceIndexDispatchBounds {
    /** The BE decodes at most this many storage-option entries per dispatch. */
    public static final int MAX_STORAGE_OPTIONS = 64;
    /** UTF-8 byte bound of one storage-option key. */
    public static final int MAX_STORAGE_OPTION_KEY_BYTES = 256;
    /** UTF-8 byte bound of one storage-option value. */
    public static final int MAX_STORAGE_OPTION_VALUE_BYTES = 4096;
    /** Bound of the whole TCompactProtocol-serialized dispatch frame. */
    public static final int MAX_DISPATCH_BYTES = 512 * 1024;

    private LanceIndexDispatchBounds() {
    }

    /**
     * Validates one built dispatch against every payload bound. Throws
     * {@link IllegalArgumentException} on the first violation, with a message that
     * names the bound and never carries storage-option keys or values.
     */
    public static void validatePayload(TLanceIndexJobDispatch dispatch) {
        Map<String, String> storageOptions = dispatch.getStorageOptions();
        if (storageOptions != null) {
            if (storageOptions.size() > MAX_STORAGE_OPTIONS) {
                throw new IllegalArgumentException("dispatch carries " + storageOptions.size()
                        + " storage options, past the bound of " + MAX_STORAGE_OPTIONS);
            }
            for (Map.Entry<String, String> option : storageOptions.entrySet()) {
                if (utf8Bytes(option.getKey()) > MAX_STORAGE_OPTION_KEY_BYTES) {
                    throw new IllegalArgumentException("a storage-option key exceeds "
                            + MAX_STORAGE_OPTION_KEY_BYTES + " UTF-8 bytes");
                }
                if (utf8Bytes(option.getValue()) > MAX_STORAGE_OPTION_VALUE_BYTES) {
                    throw new IllegalArgumentException("a storage-option value exceeds "
                            + MAX_STORAGE_OPTION_VALUE_BYTES + " UTF-8 bytes");
                }
            }
        }
        int serializedBytes = serializedSizeBytes(dispatch);
        if (serializedBytes > MAX_DISPATCH_BYTES) {
            throw new IllegalArgumentException("the serialized dispatch is " + serializedBytes
                    + " bytes, past the bound of " + MAX_DISPATCH_BYTES);
        }
    }

    /**
     * The size of one dispatch serialized with the same TCompactProtocol the dispatch
     * client speaks, which is what the BE frame-length check measures.
     */
    public static int serializedSizeBytes(TLanceIndexJobDispatch dispatch) {
        try {
            return new TSerializer(new TCompactProtocol.Factory()).serialize(dispatch).length;
        } catch (TException e) {
            throw new IllegalStateException("failed to serialize the lance index dispatch", e);
        }
    }

    private static int utf8Bytes(String value) {
        return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }
}
