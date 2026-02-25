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

public class OzoneProperties extends AbstractS3CompatibleProperties {

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.endpoint", "fs.s3a.endpoint"},
            required = false,
            description = "The endpoint of Ozone S3 Gateway.")
    protected String endpoint = "";

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.region", "fs.s3a.endpoint.region"},
            required = false,
            description = "The region of Ozone S3 Gateway.")
    protected String region = "us-east-1";

    @Getter
    @ConnectorProperty(names = {"ozone.access_key", "fs.s3a.access.key"},
            required = false,
            sensitive = true,
            description = "The access key of Ozone S3 Gateway.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"ozone.secret_key", "fs.s3a.secret.key"},
            required = false,
            sensitive = true,
            description = "The secret key of Ozone S3 Gateway.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"ozone.session_token", "fs.s3a.session.token"},
            required = false,
            sensitive = true,
            description = "The session token of Ozone S3 Gateway.")
    protected String sessionToken = "";

    @Getter
    @ConnectorProperty(names = {"ozone.connection.maximum", "fs.s3a.connection.maximum"},
            required = false,
            description = "Maximum number of connections.")
    protected String maxConnections = "100";

    @Getter
    @ConnectorProperty(names = {"ozone.connection.request.timeout", "fs.s3a.connection.request.timeout"},
            required = false,
            description = "Request timeout in seconds.")
    protected String requestTimeoutS = "10000";

    @Getter
    @ConnectorProperty(names = {"ozone.connection.timeout", "fs.s3a.connection.timeout"},
            required = false,
            description = "Connection timeout in seconds.")
    protected String connectionTimeoutS = "10000";

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.use_path_style", "fs.s3a.path.style.access"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "true";

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.force_parsing_by_standard_uri"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    protected String forceParsingByStandardUrl = "false";

    private static final Set<String> IDENTIFIERS = ImmutableSet.of(
            "ozone.endpoint",
            "ozone.access_key",
            "ozone.secret_key");

    protected OzoneProperties(Map<String, String> origProps) {
        super(Type.OZONE, origProps);
    }

    protected static boolean guessIsMe(Map<String, String> origProps) {
        if (origProps == null || origProps.isEmpty()) {
            return false;
        }
        if (IDENTIFIERS.stream().anyMatch(key -> StringUtils.isNotBlank(origProps.get(key)))) {
            return true;
        }
        String endpoint = origProps.get("fs.s3a.endpoint");
        return StringUtils.isNotBlank(endpoint)
                && (StringUtils.containsIgnoreCase(endpoint, "ozone")
                || StringUtils.containsIgnoreCase(endpoint, "s3g"));
    }

    @Override
    protected Set<Pattern> endpointPatterns() {
        return ImmutableSet.of(Pattern.compile("^(?:https?://)?[a-zA-Z0-9.-]+(?::\\d+)?$"));
    }

    @Override
    protected void setEndpointIfPossible() {
        super.setEndpointIfPossible();
        if (StringUtils.isBlank(getEndpoint())) {
            throw new IllegalArgumentException("Property ozone.endpoint is required.");
        }
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("s3", "s3a", "s3n");
    }
}
