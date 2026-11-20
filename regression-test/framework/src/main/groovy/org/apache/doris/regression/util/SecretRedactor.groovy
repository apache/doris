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

package org.apache.doris.regression.util

import java.util.concurrent.ConcurrentHashMap

/** Redact configured secrets in diagnostics only; never modify a statement sent to Doris. */
class SecretRedactor {
    private static final Set<String> secrets = ConcurrentHashMap.newKeySet()

    static void register(Collection values) {
        values.findAll { it != null && !it.toString().isEmpty() }.each { value ->
            String secret = value.toString()
            secrets.add(secret)
            secrets.add(secret.replace("\\", "\\\\").replace('"', '\\"'))
        }
    }

    static String redact(String text) {
        if (text == null || secrets.isEmpty()) {
            return text
        }
        // Longer tokens first so a contained key cannot prevent complete redaction.
        secrets.toList().sort { a, b -> b.length() <=> a.length() }.each { secret ->
            text = text.replace(secret, "[REDACTED]")
        }
        return text
    }
}
