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

public class PaimonScannerUtils {
    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    public static <T> T decodeStringToObject(String encodedStr) {
        final byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try {
            return InstantiationUtil.deserializeObject(bytes, PaimonScannerUtils.class.getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> fieldNames(RowType rowType) {
        return rowType.getFields().stream()
                .map(DataField::name)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }
}
