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

import java.lang.reflect.Field;

public final class InvertedIndexStorageFormatValidator {
    private InvertedIndexStorageFormatValidator() {
    }

    public static void rejectRuntimeV1(String confVal) throws ConfigException {
        String normalizedConfVal = confVal.trim();
        if ("V1".equalsIgnoreCase(normalizedConfVal)) {
            throw new ConfigException("Inverted index V1 is deprecated and no longer allowed"
                    + " for new index creation. Please use inverted index V2.");
        }
    }

    public static void rejectStartupV1(String confVal) throws ConfigException {
        String normalizedConfVal = confVal.trim();
        if ("V1".equalsIgnoreCase(normalizedConfVal)) {
            throw new ConfigException(
                    "inverted_index_storage_format=V1 is no longer supported. "
                    + "Please update fe.conf (or fe_custom.conf): set inverted_index_storage_format=V2.");
        }
    }

    public static class RuntimeConfigHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            rejectRuntimeV1(confVal);
            super.handle(field, confVal.trim());
        }
    }
}
