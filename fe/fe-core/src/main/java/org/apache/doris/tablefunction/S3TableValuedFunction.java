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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The Implement of table valued function
 * S3("uri" = "xxx", "access_key" = "xx", "SECRET_KEY" = "qqq", "FORMAT" = "csv").
 * <p>
 * For AWS S3, uri should be:
 * s3://bucket.s3.us-east-1.amazonaws.com/csv/taxi.csv
 * or
 * https://us-east-1.amazonaws.com/bucket/csv/taxi.csv with "use_path_style"="true"
 * or
 * https://bucket.us-east-1.amazonaws.com/csv/taxi.csv with "use_path_style"="false"
 */
public class S3TableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "s3";

    // Remote providers registered by the pinned Lance SDK. File providers also have aliases,
    // so checking only for "file" (or a particular StorageProperties type) is insufficient.
    private static final Set<String> LANCE_REMOTE_SCHEMES = ImmutableSet.of(
            "s3", "s3+ddb", "az", "abfss", "gs", "goosefs", "oss", "cos", "hf", "tos");

    public S3TableValuedFunction(Map<String, String> properties) throws AnalysisException {
        this(properties, null);
    }

    public S3TableValuedFunction(Map<String, String> properties, List<TBrokerFileStatus> files)
            throws AnalysisException {
        // 1. analyze common properties
        Map<String, String> props = super.parseCommonProperties(properties);
        try {
            this.storageProperties = StorageProperties.createPrimary(props);
            this.backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());
            String uri = storageProperties.validateAndGetUri(props);
            filePath = storageProperties.validateAndNormalizeUri(uri);
            this.backendConnectProperties.put(URI_KEY, filePath);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
        // Validate the final URI, including when BrokerProperties passes it through unchanged.
        // A local dataset must not bypass the LOCAL switch, privileges or BE secure directory.
        if (isLanceFormat() && !isRemoteLanceUri(filePath)) {
            throw new AnalysisException("S3 Lance requires remote storage with a valid URI");
        }
        if (FeConstants.runningUnitTest) {
            // Just check
            // Fixme wait to be done  #50320
            // FileSystemFactory.get(storageProperties);
            if (files != null) {
                this.fileStatuses.addAll(files);
            }
            return;
        }
        if (isLanceFormat()) {
            try {
                LanceTableMetadata metadata = LanceMetadataLoader.loadLatestForTvf(
                        filePath, Collections.singletonList(storageProperties));
                setLanceTableMetadata(metadata);
            } catch (Exception e) {
                throw new AnalysisException(
                        "Failed to load Lance metadata from S3 dataset '" + filePath + "': "
                                + e.getMessage(),
                        e);
            }
        } else {
            parseFile(files);
        }
    }


    private static boolean isRemoteLanceUri(String path) {
        try {
            URI uri = new URI(path == null ? "" : path);
            String scheme = uri.getScheme();
            String authority = uri.getRawAuthority();
            if (scheme == null || !LANCE_REMOTE_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))
                    || uri.isOpaque()) {
                return false;
            }
            if (authority == null || authority.isEmpty()) {
                // GooseFS can resolve its remote master from storage options or configuration.
                return "goosefs".equalsIgnoreCase(scheme) && uri.getRawPath() != null
                        && uri.getRawPath().startsWith("/");
            }
            // Lance falls back to a local path if Rust's URL parser rejects the URI. Java's URI
            // accepts registry authorities with invalid ports, which Rust rejects. Validate the
            // host/port separately without disallowing legitimate bucket names with underscores.
            String hostPort = authority.substring(authority.lastIndexOf('@') + 1);
            HostAndPort parsed = HostAndPort.fromString(hostPort).requireBracketsForIPv6();
            if (parsed.hasPort()) {
                // Guava parses "-0" as zero, but Rust rejects signs and falls back to local.
                String port = hostPort.substring(hostPort.lastIndexOf(':') + 1);
                for (int i = 0; i < port.length(); i++) {
                    if (port.charAt(i) < '0' || port.charAt(i) > '9') {
                        return false;
                    }
                }
            }
            String host = parsed.getHost();
            // Rust rejects zones and IPv4 tails with leading zeros, such as ::ffff:127.0.0.01.
            return !host.isEmpty() && (!host.contains(":")
                    || (!host.contains("%") && InetAddresses.isInetAddress(host)));
        } catch (URISyntaxException | IllegalArgumentException e) {
            return false;
        }
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_S3;
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        if (isLanceFormat()) {
            if (columns == null) {
                throw new AnalysisException("S3 Lance metadata was not initialized in FE");
            }
            return columns;
        }
        return super.getTableColumns();
    }

    @Override
    public String getFilePath() {
        // must be "s3://..."
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("S3TvfBroker", processedParams);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "S3TableValuedFunction";
    }
}
