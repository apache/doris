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
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
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
    private static final Pattern URL_VALUE = Pattern.compile("(?i)^(?:[a-z][a-z0-9+.-]*://|(?:file|hdfs|viewfs):/)");
    private static final Pattern DRIVER_URL_KEY = Pattern.compile("(?i)(?:^|[._-])driver[._-]url$");
    private static final Pattern AMBIGUOUS_LOCAL_PATH = Pattern.compile("[\\p{Cntrl}?#@;]|://");
    private static final Pattern RESOLVED_FILE_LOCATION = Pattern.compile(
            "^([a-zA-Z][a-zA-Z0-9+.-]*)://([^/?#]*)([^?]*)(\\?.*)?$", Pattern.DOTALL);
    private static final String HOST = "(?:[a-zA-Z0-9_][a-zA-Z0-9._-]*|"
            + "\\[[a-fA-F0-9:.]+(?:%[a-zA-Z0-9_.~-]+)?\\])(?::[0-9]+)?";
    private static final Pattern HOST_PATTERN = Pattern.compile(HOST);
    private static final Set<String> URI_SCHEMES = Set.of("http", "https", "thrift", "hdfs", "viewfs", "file",
            "s3", "s3a", "s3n",
            "oss", "cos", "cosn", "obs", "gs", "abfs", "abfss", "wasb", "wasbs");
    private static final Set<String> AZURE_STORAGE_SCHEMES = Set.of("abfs", "abfss", "wasb", "wasbs");
    // Match AzureUri's container grammar, including its single-character compatibility form.
    private static final Pattern AZURE_CONTAINER_NAME = Pattern.compile("[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?");
    private static final Pattern GCS_BUCKET_NAME = Pattern.compile("[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]");
    private static final Pattern QUERY_NAME = Pattern.compile("[a-zA-Z0-9_.~-]+");
    private static final Pattern QUERY_CREDENTIAL = Pattern.compile(
            "(?i)password|passwd|pwd|secret|token|credential|access[._-]?key|api[._-]?key|"
                    + "account[._-]?key|private[._-]?key|auth[._-]?key|"
                    + "app(?:lication)?[._-]?key|authorization|^auth$|^key$");
    private static final Pattern SIGNATURE = Pattern.compile("(?i)signature|(?:^|[._-])(?:sig|sas)(?:$|[._-])");
    private static final Pattern ENCODED_ESCAPE = Pattern.compile("%[a-fA-F0-9]{2}");
    private static final Pattern LITERAL_PERCENT = Pattern.compile("%(?![a-fA-F0-9]{2})");
    private static final int NATIVE_QUERY_DECODE_PASSES = 3;
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
        // Local driver identifiers have filesystem semantics; ambiguous connection syntax still fails closed.
        if (key instanceof String && DRIVER_URL_KEY.matcher((String) key).find() && address.startsWith("/")) {
            return address.startsWith("//") || AMBIGUOUS_LOCAL_PATH.matcher(address).find() ? MASK : address;
        }
        if ((key instanceof String && ADDRESS_KEY.matcher((String) key).find())
                || URL_VALUE.matcher(address).find()) {
            return redactAddress(address);
        }
        return text;
    }

    private static String redactAddress(String address) {
        return redactAddress(address, null, null);
    }

    static String redactResolvedFileLocation(String location) {
        if (DatasourcePrintableMap.isSensitiveKey("uri")) {
            return MASK;
        }
        Matcher parts = RESOLVED_FILE_LOCATION.matcher(location);
        if (!parts.matches()) {
            return MASK;
        }
        try {
            String path = parts.group(3);
            // A resolved singleton may contain literal glob characters; encode only for URI validation.
            String encodedPath = new URI(null, "placeholder", path, null, null).getRawPath();
            // Selected keys are already decoded: '#' is literal, including in a query-shaped suffix.
            String rawQuery = parts.group(4) == null ? null : parts.group(4).substring(1);
            String encodedQuery = rawQuery == null ? null : new URI(null, null, null, rawQuery, null).getRawQuery();
            String address = parts.group(1) + "://" + parts.group(2) + encodedPath
                    + (encodedQuery == null ? "" : "?" + encodedQuery);
            return redactAddress(address, path, rawQuery);
        } catch (URISyntaxException | IllegalArgumentException e) {
            return MASK;
        }
    }

    private static String redactAddress(String address, String resolvedPath, String resolvedQuery) {
        if (address.isEmpty()) {
            return address;
        }
        try {
            if (!address.contains("://") && isPlainHosts(address)) {
                return address;
            }
            // URI accepts a later URL as path text after the first host; SDKs may instead split the list.
            if (resolvedPath == null && address.indexOf(',') >= 0) {
                return MASK;
            }
            URI uri = new URI(address);
            if (uri.getScheme() == null) {
                return MASK;
            }
            String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            boolean localFile = "file".equals(scheme);
            if (!URI_SCHEMES.contains(scheme)
                    || uri.isOpaque() || uri.getPort() > 65535) {
                return MASK;
            }
            String authority = uri.getRawAuthority();
            if (localFile && authority != null) {
                return MASK;
            }
            if (authority == null) {
                // HDFS and ViewFS can resolve an absolute path through configured nameservices/default mounts.
                if (!(localFile || "hdfs".equals(scheme) || "viewfs".equals(scheme))
                        || !uri.getRawPath().startsWith("/")) {
                    return MASK;
                }
                authority = "";
            } else if ("gs".equals(scheme)) {
                // GCS bucket authorities allow underscores, which java.net.URI does not treat as DNS hosts.
                authority = authority.substring(authority.lastIndexOf('@') + 1);
                if (!isGcsBucket(authority)) {
                    return MASK;
                }
            } else if ("viewfs".equals(scheme)) {
                // ViewFS authorities name configured mount tables, which need not be DNS hostnames.
                authority = authority.substring(authority.lastIndexOf('@') + 1);
                if (!isPlainHosts(authority)) {
                    return MASK;
                }
            } else if (uri.getHost() == null) {
                return MASK;
            }
            // Source metadata already decoded native keys; decoding again corrupts literal '%' and '%HH' names.
            String path = resolvedPath == null ? uri.getPath() : resolvedPath;
            // Ambiguous path syntax remains hidden; another encoding layer applies only to ordinary URI paths.
            if (path != null && (path.contains(";") || (resolvedPath == null && path.contains(","))
                    || path.contains("://") || path.contains(",//")
                    || (resolvedPath == null && ENCODED_ESCAPE.matcher(path).find()))) {
                return MASK;
            }
            String query = redactQuery(resolvedPath == null ? uri.getRawQuery() : resolvedQuery, resolvedPath != null);
            if (MASK.equals(query)) {
                return MASK;
            }
            if (AZURE_STORAGE_SCHEMES.contains(scheme)) {
                // ABFS/WASB place the container in userinfo; validation prevents retaining password-bearing authority.
                String container = uri.getRawUserInfo();
                if (container == null || !AZURE_CONTAINER_NAME.matcher(container).matches()) {
                    return MASK;
                }
            } else {
                authority = authority.substring(authority.lastIndexOf('@') + 1);
            }
            String prefix = uri.getScheme() + ":";
            if (!authority.isEmpty() || uri.getRawSchemeSpecificPart().startsWith("//")) {
                prefix += "//";
            }
            return prefix + authority + (resolvedPath == null ? uri.getRawPath() : resolvedPath)
                    + (query == null ? "" : "?" + query);
        } catch (URISyntaxException | IllegalArgumentException e) {
            // Parser errors may themselves contain credentials. Never echo the value or exception to a log.
            return MASK;
        }
    }

    private static boolean isGcsBucket(String authority) {
        if (!GCS_BUCKET_NAME.matcher(authority).matches()) {
            return false;
        }
        for (String component : authority.split("\\.", -1)) {
            if (component.length() > 63) {
                return false;
            }
        }
        return true;
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

    private static String redactQuery(String rawQuery, boolean nativeQuery) {
        if (rawQuery == null) {
            return null;
        }
        StringBuilder result = new StringBuilder(rawQuery.length());
        int start = 0;
        for (int end = 0; end <= rawQuery.length(); end++) {
            if (end < rawQuery.length() && rawQuery.charAt(end) != '&' && rawQuery.charAt(end) != ';') {
                continue;
            }
            if (end > start) {
                String parameter = redactQueryParameter(rawQuery.substring(start, end), nativeQuery);
                if (MASK.equals(parameter)) {
                    return MASK;
                }
                if (!nativeQuery && result.length() > 0) {
                    result.append('&');
                }
                result.append(parameter);
            }
            // Non-secret delimiters and empty components identify a native key; only ordinary URLs normalize them.
            if (nativeQuery && end < rawQuery.length()) {
                result.append(rawQuery.charAt(end));
            }
            start = end + 1;
        }
        return result.toString();
    }

    private static String redactQueryParameter(String parameter, boolean nativeQuery) {
        int equal = parameter.indexOf('=');
        String rawName = equal < 0 ? parameter : parameter.substring(0, equal);
        String name = URLDecoder.decode(rawName, StandardCharsets.UTF_8);
        if (!QUERY_NAME.matcher(name).matches() || SIGNATURE.matcher(name).find()) {
            return MASK;
        }
        if (DatasourcePrintableMap.isSensitiveKey(name) || QUERY_CREDENTIAL.matcher(name).find()) {
            return rawName + "=" + MASK;
        }
        String value = equal < 0 ? "" : parameter.substring(equal + 1);
        return isUnsafeQueryValue(value, nativeQuery) ? MASK : parameter;
    }

    private static boolean isUnsafeQueryValue(String value, boolean nativeQuery) {
        if (!nativeQuery) {
            String decoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
            return isNestedConnection(decoded) || ENCODED_ESCAPE.matcher(decoded).find();
        }
        // Inspect bounded decoded views without changing native text or letting a literal '%' hide an encoded URL.
        String view = value;
        for (int pass = 0; pass < NATIVE_QUERY_DECODE_PASSES; pass++) {
            // Include form-space views for JDBC detection while emitting the original literal '+' unchanged.
            if (isNestedConnection(view.replace('+', ' '))) {
                return true;
            }
            if (!ENCODED_ESCAPE.matcher(view).find()) {
                return false;
            }
            view = URLDecoder.decode(LITERAL_PERCENT.matcher(view).replaceAll("%25"), StandardCharsets.UTF_8);
        }
        return isNestedConnection(view.replace('+', ' ')) || ENCODED_ESCAPE.matcher(view).find();
    }

    private static boolean isNestedConnection(String value) {
        return value.contains("://") || value.trim().regionMatches(true, 0, "jdbc:", 0, 5);
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
