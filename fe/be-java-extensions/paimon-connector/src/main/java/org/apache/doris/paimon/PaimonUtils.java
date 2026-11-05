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

package org.apache.doris.paimon;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class PaimonUtils {
    private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();
    private static final Base64.Decoder STD_DECODER = Base64.getDecoder();

    public static List<String> getFieldNames(RowType rowType) {
        return rowType.getFields().stream()
                .map(DataField::name)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    public static <T> T deserialize(String encodedStr) {
        try {
            byte[] decoded;
            try {
                decoded = URL_DECODER.decode(encodedStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (IllegalArgumentException e) {
                // Fallback to standard Base64 for splits encoded by native Paimon serialization.
                decoded = STD_DECODER.decode(encodedStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            return InstantiationUtil.deserializeObject(decoded, PaimonUtils.class.getClassLoader());
        } catch (Throwable e) {
            // A BE-first rolling upgrade against a legacy Paimon 1.4 warehouse can
            // send objects the 2.1 connector can no longer decode: the old FE
            // serializes the table as a PrivilegedFileStoreTable (its privilege
            // classes are gone), and the old classes are absent from this
            // classloader. Surface the upgrade cause instead of the bare
            // ClassNotFoundException, so the fix (upgrade the FE or migrate the
            // catalog's legacy privilege metadata) is actionable from the error.
            Throwable cause = e;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof ClassNotFoundException) {
                throw new RuntimeException(
                        "Paimon object cannot be decoded by the 2.1 connector: "
                                + cause.getMessage()
                                + ". A BE-first rolling upgrade against a legacy Paimon warehouse can send "
                                + "serialized objects (e.g. PrivilegedFileStoreTable) whose classes were "
                                + "removed in Paimon 2.1; upgrade the FE to the matching version or migrate "
                                + "the catalog's legacy privilege metadata.",
                        cause);
            }
            throw new RuntimeException(e);
        }
    }
}
