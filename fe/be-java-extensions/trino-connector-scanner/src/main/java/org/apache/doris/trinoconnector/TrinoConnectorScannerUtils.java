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

package org.apache.doris.trinoconnector;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import java.util.List;

public class TrinoConnectorScannerUtils {
    public static <T> T decodeStringToObject(String encodedStr, Class<T> type,
            ObjectMapperProvider objectMapperProvider) {
        try {
            JsonCodec<T> jsonCodec = new JsonCodecFactory(objectMapperProvider).jsonCodec(type);
            return jsonCodec.fromJson(encodedStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> decodeStringToList(String encodedStr, Class<T> type,
            ObjectMapperProvider objectMapperProvider) {
        try {
            JsonCodec<List<T>> listJsonCodec = new JsonCodecFactory(objectMapperProvider).listJsonCodec(type);
            return listJsonCodec.fromJson(encodedStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

