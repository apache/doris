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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class MinioProperties extends AbstractS3CompatibleProperties {
    @Setter
    @Getter
    @ConnectorProperty(names = {"minio.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false, description = "The endpoint of Minio.")
    protected String endpoint = "";
    @Getter
    @Setter
    protected String region = "us-east-1";

    @Getter
    @ConnectorProperty(names = {"minio.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key", "s3.access_key"},
            required = false,
            description = "The access key of Minio.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"minio.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            required = false,
            description = "The secret key of Minio.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"minio.session_token", "s3.session_token", "session_token"},
            required = false,
            description = "The session token of Minio.")
    protected String sessionToken = "";

    /**
     * The maximum number of concurrent connections that can be made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"minio.connection.maximum", "s3.connection.maximum"}, required = false,
            description = "Maximum number of connections.")
    protected String maxConnections = "100";

    /**
     * The timeout (in milliseconds) for requests made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"minio.connection.request.timeout", "s3.connection.request.timeout"}, required = false,
            description = "Request timeout in seconds.")
    protected String requestTimeoutS = "10000";

    /**
     * The timeout (in milliseconds) for establishing a connection to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"minio.connection.timeout", "s3.connection.timeout"}, required = false,
            description = "Connection timeout in seconds.")
    protected String connectionTimeoutS = "10000";

    /**
     * Flag indicating whether to use path-style URLs for the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Setter
    @Getter
    @ConnectorProperty(names = {"minio.use_path_style", "use_path_style", "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";

    @ConnectorProperty(names = {"minio.force_parsing_by_standard_uri", "force_parsing_by_standard_uri"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    @Setter
    @Getter
    protected String forceParsingByStandardUrl = "false";

    private static final Set<String> IDENTIFIERS = ImmutableSet.of("minio.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY",
            "access_key", "s3.access_key", "minio.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT");

    /**
     * Constructor to initialize the object storage properties with the provided type and original properties map.
     *
     * @param origProps the original properties map.
     */
    protected MinioProperties(Map<String, String> origProps) {
        super(Type.MINIO, origProps);
    }

    public static boolean guessIsMe(Map<String, String> origProps) {
        //ugly, but we need to check if the user has set any of the identifiers
        if (AzureProperties.guessIsMe(origProps) || COSProperties.guessIsMe(origProps)
                || OSSProperties.guessIsMe(origProps) || S3Properties.guessIsMe(origProps)) {
            return false;
        }

        return IDENTIFIERS.stream().map(origProps::get).anyMatch(value -> value != null && !value.isEmpty());
    }


    @Override
    protected Set<Pattern> endpointPatterns() {
        return ImmutableSet.of(Pattern.compile("^(?:https?://)?[a-zA-Z0-9.-]+(?::\\d+)?$"));
    }

    protected void setEndpointIfPossible() {
        super.setEndpointIfPossible();
        if (StringUtils.isBlank(getEndpoint())) {
            throw new IllegalArgumentException("Property minio.endpoint is required.");
        }
    }
}
