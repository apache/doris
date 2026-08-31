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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.lance.namespace.LanceNamespace;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Properties for a Lance directory namespace backed by a filesystem warehouse. */
public class LanceFileSystemMetastoreProperties extends AbstractLanceProperties {
    public static final String WAREHOUSE = "warehouse";
    private static final String OSS_HDFS_MARKER = ".oss-dls.aliyuncs.com";

    @ConnectorProperty(
            names = {WAREHOUSE},
            required = false,
            description = "The local, file, S3, or OSS warehouse containing Lance datasets."
    )
    private String warehouse;

    public LanceFileSystemMetastoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getLanceCatalogType() {
        return LANCE_FILESYSTEM;
    }

    @Override
    public LanceNamespace createNamespace(
            BufferAllocator allocator, Map<String, String> javaStorageOptions) {
        Map<String, String> namespaceProperties = new HashMap<>();
        namespaceProperties.put("root", warehouse);
        javaStorageOptions.forEach(
                (key, value) -> namespaceProperties.put("storage." + key, value));
        return LanceNamespace.connect("dir", namespaceProperties, allocator);
    }

    public String getWarehouse() {
        return warehouse;
    }

    /** A directory namespace opens the warehouse itself, so its options follow that URL. */
    @Override
    public String getNamespaceStorageUri() {
        return warehouse;
    }

    @Override
    protected void validateCatalogProperties() {
        warehouse = origProps.get(WAREHOUSE);
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException(
                    "Missing required property 'warehouse' for Lance filesystem catalog");
        }
        rejectOssHdfs();
        // Validate before normalizing: the rewrite collapses the authority at its first dot, which
        // would strip the very marker that identifies an OSS-HDFS root.
        validateWarehouse(warehouse);
        warehouse = normalizeWarehouse(warehouse);
        for (String key : origProps.keySet()) {
            if (key.startsWith("lance.rest.")) {
                throw new IllegalArgumentException(
                        "Property '" + key + "' is not valid for Lance filesystem catalog");
            }
        }
    }

    private static void validateWarehouse(String warehouse) {
        final URI uri;
        try {
            uri = URI.create(warehouse);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Lance warehouse URI: " + warehouse, e);
        }
        if (uri.getScheme() == null) {
            Path path = Paths.get(warehouse);
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException(
                        "Local Lance warehouse must be an absolute path: " + warehouse);
            }
            return;
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"file".equals(scheme) && !"s3".equals(scheme) && !"oss".equals(scheme)) {
            throw new IllegalArgumentException("Unsupported Lance filesystem warehouse scheme '" + scheme
                    + "'; supported schemes are local/file, s3, and oss");
        }
        // An object-store root names its bucket in the authority. Lance reads that authority as the
        // bucket and fails deep inside the store when it is absent, so reject the no-authority form
        // here where the message can still name the property.
        if (("s3".equals(scheme) || "oss".equals(scheme)) && StringUtils.isBlank(uri.getAuthority())) {
            throw new IllegalArgumentException(
                    "Lance " + scheme + " warehouse must name a bucket, as in " + scheme
                            + "://bucket/path, but was: " + warehouse);
        }
    }

    /**
     * Doris routes an OSS-HDFS configuration to {@code OSSHdfsProperties}, which the Lance OSS
     * provider cannot read - it accepts only {@code OSSProperties} - so the namespace would be
     * handed no endpoint and no credentials and could not open at all.
     *
     * <p>Checks every property rather than only the warehouse: {@code OSSHdfsProperties.guessIsMe}
     * selects on the endpoint, so {@code oss://bucket/path} with an {@code oss.endpoint} ending in
     * the OSS-HDFS suffix routes there just the same, with a clean-looking warehouse.
     */
    private void rejectOssHdfs() {
        for (Map.Entry<String, String> property : origProps.entrySet()) {
            String value = property.getValue();
            if (value != null && value.toLowerCase(Locale.ROOT).contains(OSS_HDFS_MARKER)) {
                throw new IllegalArgumentException(
                        "OSS-HDFS is not supported by the Lance catalog, but '"
                                + property.getKey() + "' names it. Doris reads this form through "
                                + "its HDFS-compatible properties, which carry no Lance OSS "
                                + "storage options.");
            }
        }
    }

    /**
     * Doris accepts an OSS URL that spells out the endpoint in its authority and normalizes it to
     * the bare bucket. Lance takes the authority as the bucket verbatim, so a warehouse left in the
     * qualified form would address {@code bucket.oss-<region>.aliyuncs.com.<endpoint>}. Apply the
     * same normalization Doris applies elsewhere before the root reaches the namespace.
     */
    private static String normalizeWarehouse(String warehouse) {
        if (StringUtils.isBlank(warehouse)) {
            return warehouse;
        }
        URI uri;
        try {
            uri = URI.create(warehouse);
        } catch (IllegalArgumentException e) {
            return warehouse;
        }
        if (uri.getScheme() == null || !"oss".equals(uri.getScheme().toLowerCase(Locale.ROOT))) {
            return warehouse;
        }
        return OSSProperties.rewriteOssBucketIfNecessary(warehouse);
    }
}
