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

package org.apache.doris.cache;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

public interface Cache extends Closeable {

    byte[] get(NamedKey key);

    void put(NamedKey key, byte[] value);

    Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys);

    CacheStats getStats();

    boolean isLocal();

    class NamedKey {
        public final String namespace;
        public final byte[] key;

        public NamedKey(String namespace, byte[] key) {
            Preconditions.checkNotNull(namespace, "Namespace must not be null");
            Preconditions.checkNotNull(key, "Key must not be null");
            this.namespace = namespace;
            this.key = key;
        }

        public byte[] toByteArray() {
            final byte[] nsBytes = namespace.getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(Integer.BYTES + nsBytes.length + key.length)
                    .putInt(nsBytes.length)
                    .put(nsBytes)
                    .put(key)
                    .array();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o instanceof NamedKey) {
                NamedKey namedKey = (NamedKey) o;
                return namespace.equals(namedKey.namespace) && Arrays.equals(key, namedKey.key);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return 31 * namespace.hashCode() + Arrays.hashCode(key);
        }

        @Override
        public String toString() {
            return namespace + "_" + new String(key, StandardCharsets.UTF_8);
        }
    }
}
