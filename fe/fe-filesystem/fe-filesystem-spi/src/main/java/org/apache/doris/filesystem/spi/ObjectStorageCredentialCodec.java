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

package org.apache.doris.filesystem.spi;

import java.io.IOException;
import java.util.Map;

/**
 * Provider-owned conversion between resolved credentials and the existing BE/Cloud wire envelopes.
 *
 * <p>Only JDK values cross the plugin boundary. Generated protocol classes and their runtimes remain
 * implementation details; plugins must not depend on fe-core. Implementations perform no I/O and
 * preserve absent fields versus explicitly empty fields in partial ALTER credentials.</p>
 */
public interface ObjectStorageCredentialCodec {
    /** Canonical object-store provider name used by the BE and Cloud protocols. */
    String provider();

    /** Encode a complete ObjectStoreCredentialPB envelope, not its provider-specific submessage. */
    byte[] toCloudCredential(Map<String, String> credential) throws IOException;

    /** Encode a complete TCredential envelope using Thrift's binary protocol. */
    byte[] toThriftCredential(Map<String, String> credential) throws IOException;

    /** Restore user-facing properties from an ObjectStoreCredentialPB envelope. */
    Map<String, String> toProperties(byte[] cloudCredential) throws IOException;
}
