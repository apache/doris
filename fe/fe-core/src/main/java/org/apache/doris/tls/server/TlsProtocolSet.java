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

package org.apache.doris.tls.server;

import org.apache.doris.common.Config;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

public final class TlsProtocolSet {
    public enum Protocol {
        BRPC,
        BDBJE,
        THRIFT,
        MYSQL,
        HTTP,
        ARROWFLIGHT
    }

    private static String cachedRaw = null;
    private static Set<String> cachedExcluded = Collections.emptySet();

    private TlsProtocolSet() {
    }

    public static synchronized boolean isProtocolIncluded(Protocol protocol) {
        return !excludedProtocols().contains(protocol.name().toLowerCase(Locale.ROOT));
    }

    public static boolean isHttpTlsActive() {
        return Config.enable_tls && isProtocolIncluded(Protocol.HTTP);
    }

    private static Set<String> excludedProtocols() {
        String raw = Config.tls_excluded_protocols == null ? "" : Config.tls_excluded_protocols;
        if (raw.equals(cachedRaw)) {
            return cachedExcluded;
        }

        LinkedHashSet<String> values = new LinkedHashSet<>();
        Arrays.stream(raw.split(","))
                .map(String::trim)
                .map(s -> s.toLowerCase(Locale.ROOT))
                .filter(s -> !s.isEmpty())
                .forEach(values::add);
        cachedRaw = raw;
        cachedExcluded = Collections.unmodifiableSet(values);
        return cachedExcluded;
    }
}
