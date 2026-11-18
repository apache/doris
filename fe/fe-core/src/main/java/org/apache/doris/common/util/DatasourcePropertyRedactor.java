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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/** Value-aware masking shared by catalog display, audit property rendering, and lineage metadata. */
final class DatasourcePropertyRedactor {
    private static final String MASK = DatasourcePrintableMap.PASSWORD_MASK;
    // The bare URI alias is shared with REST catalogs, so its connection-string status depends on the value.
    private static final Pattern CONNECTION_KEY = Pattern.compile(
            "(?i)(?:connection[._-]?string|(?:^|[._-])jdbc[._-]?(?:url|uri))$");
    private static final Pattern NATIVE_CREDENTIAL = Pattern.compile("(?i)(?:^|[._-])(?:token|credentials?)$");
    private static final Pattern HADOOP_CREDENTIAL = Pattern.compile("(?i)(?:^|\\.)fs\\.(?:"
            + "s3a?\\.(?:bucket\\..+\\.)?(?:access\\.key|secret\\.key|session\\.token|"
            + "(?:server-side-)?encryption\\.key|proxy\\.password)|"
            + "azure\\.(?:account\\.key|sas(?:\\.fixed\\.token)?|"
            + "account\\.oauth2\\.(?:client\\.secret|refresh\\.token)|"
            + "encryption\\.encoded\\.client-provided\\.key)(?:\\..*)?|adl\\.oauth2\\.(?:credential|refresh\\.token)|"
            + "oss\\.(?:accessKeyId|accessKeySecret|securityToken)|cosn\\.(?:userInfo\\.)?(?:secretId|secretKey)|"
            + "obs\\.(?:access\\.key|secret\\.key|(?:session|security)\\.token))$");
    private static final Pattern ADDRESS_KEY = Pattern.compile(
            "(?i)(?:^|[._-])(?:uris?|urls?|hosts?|endpoints?|addresses|address)$");
    private static final Pattern URL_VALUE = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://");
    private static final String HOST = "(?:[a-zA-Z0-9_][a-zA-Z0-9._-]*|"
            + "\\[[a-fA-F0-9:.]+(?:%[a-zA-Z0-9_.~-]+)?\\])(?::[0-9]+)?";
    private static final Pattern HOST_PATTERN = Pattern.compile(HOST);
    private static final Set<String> URI_SCHEMES = Set.of("http", "https", "thrift", "hdfs", "s3", "s3a", "s3n",
            "oss", "cos", "cosn", "obs", "gs", "abfs", "abfss", "wasb", "wasbs");
    private static final Pattern QUERY_NAME = Pattern.compile("[a-zA-Z0-9_.~-]+");
    private static final Pattern QUERY_CREDENTIAL = Pattern.compile(
            "(?i)password|passwd|pwd|secret|token|credential|access[._-]?key|api[._-]?key|"
                    + "account[._-]?key|private[._-]?key|auth[._-]?key|"
                    + "app(?:lication)?[._-]?key|authorization|^auth$|^key$");
    private static final Pattern SIGNATURE = Pattern.compile("(?i)signature|(?:^|[._-])(?:sig|sas)(?:$|[._-])");
    private static final Pattern ENCODED_ESCAPE = Pattern.compile("%[a-fA-F0-9]{2}");
    private static volatile NamePattern namePattern = new NamePattern(Config.datasource_sensitive_property_pattern);

    private DatasourcePropertyRedactor() {
    }

    static boolean isSensitiveName(String key) {
        return CONNECTION_KEY.matcher(key).find()
                || NATIVE_CREDENTIAL.matcher(key).find()
                || HADOOP_CREDENTIAL.matcher(key).find() || configuredNamePattern().matcher(key).find();
    }

    private static Pattern configuredNamePattern() {
        String configured = Config.datasource_sensitive_property_pattern;
        NamePattern snapshot = namePattern;
        if (!snapshot.source.equals(configured)) {
            // Publish source and compiled pattern together; a concurrent update cannot pair different versions.
            snapshot = new NamePattern(configured);
            namePattern = snapshot;
        }
        return snapshot.pattern;
    }

    static String redact(Object key, Object value) {
        if (DatasourcePrintableMap.isSensitiveKey(key)) {
            return MASK;
        }
        String text = String.valueOf(value);
        String address = text.trim();
        // JDBC drivers accept incompatible credential syntaxes, including forms with no URI authority.
        if (address.regionMatches(true, 0, "jdbc:", 0, 5)) {
            return MASK;
        }
        if ((key instanceof String && ADDRESS_KEY.matcher((String) key).find())
                || URL_VALUE.matcher(address).find()) {
            return redactAddress(address);
        }
        return text;
    }

    private static String redactAddress(String address) {
        if (address.isEmpty()) {
            return address;
        }
        try {
            if (!address.contains("://") && isPlainHosts(address)) {
                return address;
            }
            // URI accepts a later URL as path text after the first host; SDKs may instead split the list.
            if (address.indexOf(',') >= 0) {
                return MASK;
            }
            URI uri = new URI(address);
            if (uri.getScheme() == null || !URI_SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT))
                    || uri.isOpaque() || uri.getHost() == null || uri.getPort() > 65535) {
                return MASK;
            }
            String path = uri.getPath();
            // Matrix parameters and nested encodings have no common credential grammar across SDKs.
            if (path != null && (path.contains(";") || path.contains(",") || path.contains("://")
                    || ENCODED_ESCAPE.matcher(path).find())) {
                return MASK;
            }
            String query = redactQuery(uri.getRawQuery());
            if (MASK.equals(query)) {
                return MASK;
            }
            String authority = uri.getRawAuthority();
            authority = authority.substring(authority.lastIndexOf('@') + 1);
            return uri.getScheme() + "://" + authority + uri.getRawPath()
                    + (query == null ? "" : "?" + query);
        } catch (URISyntaxException | IllegalArgumentException e) {
            // Parser errors may themselves contain credentials. Never echo the value or exception to a log.
            return MASK;
        }
    }

    private static boolean isPlainHosts(String address) {
        // Match each host separately: a repeated regex group can exhaust the stack on long host lists.
        for (String item : address.split(",", -1)) {
            String host = item.trim();
            if (!HOST_PATTERN.matcher(host).matches()) {
                return false;
            }
            int colon = host.lastIndexOf(':');
            if (colon > host.lastIndexOf(']') && Long.parseLong(host.substring(colon + 1)) > 65535) {
                return false;
            }
        }
        return true;
    }

    private static String redactQuery(String rawQuery) {
        if (rawQuery == null) {
            return null;
        }
        List<String> parameters = new ArrayList<>();
        for (String parameter : rawQuery.split("[&;]", -1)) {
            if (parameter.isEmpty()) {
                continue;
            }
            int equal = parameter.indexOf('=');
            String rawName = equal < 0 ? parameter : parameter.substring(0, equal);
            String name = URLDecoder.decode(rawName, StandardCharsets.UTF_8);
            if (!QUERY_NAME.matcher(name).matches() || SIGNATURE.matcher(name).find()) {
                return MASK;
            }
            if (DatasourcePrintableMap.isSensitiveKey(name) || QUERY_CREDENTIAL.matcher(name).find()) {
                parameters.add(rawName + "=" + MASK);
            } else {
                String value = equal < 0 ? ""
                        : URLDecoder.decode(parameter.substring(equal + 1), StandardCharsets.UTF_8);
                // A nested connection string needs its own parser; mask the outer URL instead of leaking it.
                if (value.contains("://") || value.regionMatches(true, 0, "jdbc:", 0, 5)
                        || ENCODED_ESCAPE.matcher(value).find()) {
                    return MASK;
                }
                parameters.add(parameter);
            }
        }
        return String.join("&", parameters);
    }

    private static final class NamePattern {
        private final String source;
        private final Pattern pattern;

        private NamePattern(String source) {
            this.source = source;
            this.pattern = Pattern.compile(source, Pattern.CASE_INSENSITIVE);
        }
    }
}
