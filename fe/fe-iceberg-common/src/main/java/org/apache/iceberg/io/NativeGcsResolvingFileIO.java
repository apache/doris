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

package org.apache.iceberg.io;

import org.apache.iceberg.CatalogUtil;

import org.apache.doris.filesystem.gcs.auth.GcsOAuthTokenValidator;
import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ResolvingFileIO} that preserves Hadoop GCS credentials unless SDK credentials override them.
 *
 * <p>Doris configures GCS access as Hadoop properties: native credentials (credential provider type,
 * service-account impersonation) as {@code fs.gs.*}, and HMAC interoperability keys or anonymous access as
 * {@code fs.s3a.*}. Only the Hadoop filesystems read them. Upstream ResolvingFileIO
 * routes {@code gs} to {@code GCSFileIO} whenever iceberg-gcp is on the classpath and falls back to
 * HadoopFileIO only when that class is missing. The plugins bundle iceberg-gcp for the REST catalog's
 * {@code GoogleAuthManager} and an explicitly chosen {@code GCSFileIO}, so this class pins the default
 * routing instead of relying on that class being absent. SDK credentials and their storage options retain
 * the SDK path, including credentials received after FE selection. Other schemes resolve as upstream.
 *
 * <p>It lives in {@code org.apache.iceberg.io} because {@code implFromLocation} is package-private; the
 * override only takes effect when this class and iceberg-core share a class loader, which holds for the
 * FE connector plugin and the BE metadata scanner, both of which bundle iceberg-core themselves. Its class
 * name is part of serialized Iceberg scan tasks, so it must stay loadable on both sides.
 */
public class NativeGcsResolvingFileIO extends ResolvingFileIO {
    private static final long serialVersionUID = 1L;

    private static final String GCS_FILE_IO_IMPL = "org.apache.iceberg.gcp.gcs.GCSFileIO";
    private static final String HADOOP_FILE_IO_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";

    // Clients are bounded by credential prefixes, not file locations, and are rebuilt after serialization.
    private transient Map<String, DelegateFileIO> scopedIos;
    private volatile Boolean hadoopGcsConfigured;
    private boolean closed;

    public static boolean hasSdkGcsProperties(Map<String, String> properties) {
        return properties.keySet().stream().anyMatch(key -> key != null && key.startsWith("gcs."));
    }

    @Override
    synchronized String implFromLocation(String location) {
        String impl = super.implFromLocation(location);
        if (!GCS_FILE_IO_IMPL.equals(impl)) {
            return impl;
        }
        StorageCredential matching = matchingCredential(location);
        Map<String, String> scope = matching == null ? Collections.emptyMap() : matching.config();
        // Validate the effective scope before choosing an identity: a present null token is not an absent token.
        GcsOAuthTokenValidator.validate(properties(), scope);
        Map<String, String> effective = effectiveProperties(scope);
        String impersonation = effective.get("gcs.impersonate.service-account");
        boolean sdkIdentity = effective.containsKey("gcs.oauth2.token")
                || Boolean.parseBoolean(effective.get("gcs.no-auth"))
                || (impersonation != null && !impersonation.isEmpty());
        if (sdkIdentity) {
            return GCS_FILE_IO_IMPL;
        }
        // Project selection is not authentication. SDK-only options must never silently replace HMAC with ADC.
        if (properties().containsKey("gs.access_key") || hasHadoopGcsConfiguration()) {
            for (String key : effective.keySet()) {
                if (key.startsWith("gcs.") && !"gcs.project-id".equals(key)) {
                    throw new IllegalArgumentException("GCS option requires an explicit SDK storage identity: " + key);
                }
            }
            return HADOOP_FILE_IO_IMPL;
        }
        // With no configured Hadoop identity, retain the upstream SDK/ADC default.
        return GCS_FILE_IO_IMPL;
    }

    private boolean hasHadoopGcsConfiguration() {
        if (hadoopGcsConfigured == null) {
            boolean configured = false;
            Configuration conf = getConf();
            if (conf != null) {
                for (Map.Entry<String, String> entry : conf) {
                    if (entry.getKey().startsWith("fs.gs.")) {
                        configured = true;
                        break;
                    }
                }
            }
            hadoopGcsConfigured = configured;
        }
        return hadoopGcsConfigured;
    }

    @Override
    public synchronized void setConf(Configuration conf) {
        super.setConf(conf);
        // Hadoop configurations can contain thousands of defaults; inspect them once per configuration.
        hadoopGcsConfigured = null;
    }

    @Override
    public synchronized void initialize(Map<String, String> properties) {
        super.initialize(properties);
        closed = false;
    }

    @Override
    public synchronized void setCredentials(List<StorageCredential> credentials) {
        boolean changed = !credentials().equals(credentials);
        super.setCredentials(credentials);
        if (changed && scopedIos != null) {
            // Replacement credentials must not reuse a client initialized with the previous effective root map.
            scopedIos.values().forEach(DelegateFileIO::close);
            scopedIos.clear();
        }
    }

    private StorageCredential matchingCredential(String location) {
        StorageCredential matching = null;
        for (StorageCredential credential : credentials()) {
            if (location.startsWith(credential.prefix())
                    && (matching == null || credential.prefix().length() >= matching.prefix().length())) {
                matching = credential;
            }
        }
        return matching;
    }

    private Map<String, String> effectiveProperties(Map<String, String> scope) {
        Map<String, String> effective = new HashMap<>(properties());
        effective.putAll(scope);
        return effective;
    }

    @Override
    synchronized DelegateFileIO io(String location) {
        if (closed) {
            throw new IllegalStateException("FileIO is closed");
        }
        if (!GCS_FILE_IO_IMPL.equals(implFromLocation(location))) {
            return super.io(location);
        }
        StorageCredential matching = matchingCredential(location);
        String prefix = matching == null ? "" : matching.prefix();
        if (scopedIos == null) {
            scopedIos = new HashMap<>();
        }
        return scopedIos.computeIfAbsent(prefix, ignored -> {
            // GCSFileIO eagerly parses globals and every credential, including unrelated/overridden expiries.
            // Give its root client the validated effective map, and retain only this scope for SDK refresh.
            Map<String, String> effective = effectiveProperties(
                    matching == null ? Collections.emptyMap() : matching.config());
            return (DelegateFileIO) CatalogUtil.loadFileIO(GCS_FILE_IO_IMPL, effective, getConf(),
                    matching == null ? Collections.emptyList() : Collections.singletonList(matching));
        });
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                if (scopedIos != null) {
                    scopedIos.values().forEach(DelegateFileIO::close);
                    scopedIos.clear();
                }
            } finally {
                super.close();
            }
        }
    }
}
