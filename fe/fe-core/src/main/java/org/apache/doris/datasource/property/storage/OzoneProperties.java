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
    @ConnectorProperty(names = {"ozone.endpoint", "s3.endpoint"},
            required = false,
            description = "The endpoint of Ozone S3 Gateway.")
    protected String endpoint = "";

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.region", "s3.region"},
            required = false,
            description = "The region of Ozone S3 Gateway.")
    protected String region = "us-east-1";

    @Getter
    @ConnectorProperty(names = {"ozone.access_key", "s3.access_key", "s3.access-key-id"},
            required = false,
            sensitive = true,
            description = "The access key of Ozone S3 Gateway.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"ozone.secret_key", "s3.secret_key", "s3.secret-access-key"},
            required = false,
            sensitive = true,
            description = "The secret key of Ozone S3 Gateway.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"ozone.session_token", "s3.session_token", "s3.session-token"},
            required = false,
            sensitive = true,
            description = "The session token of Ozone S3 Gateway.")
    protected String sessionToken = "";

    @Getter
    @ConnectorProperty(names = {"ozone.connection.maximum", "s3.connection.maximum"},
            required = false,
            description = "Maximum number of connections.")
    protected String maxConnections = "100";

    @Getter
    @ConnectorProperty(names = {"ozone.connection.request.timeout", "s3.connection.request.timeout"},
            required = false,
            description = "Request timeout in seconds.")
    protected String requestTimeoutS = "10000";

    @Getter
    @ConnectorProperty(names = {"ozone.connection.timeout", "s3.connection.timeout"},
            required = false,
            description = "Connection timeout in seconds.")
    protected String connectionTimeoutS = "10000";

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.use_path_style", "use_path_style", "s3.path-style-access"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "true";

    @Setter
    @Getter
    @ConnectorProperty(names = {"ozone.force_parsing_by_standard_uri", "force_parsing_by_standard_uri"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    protected String forceParsingByStandardUrl = "false";

    protected OzoneProperties(Map<String, String> origProps) {
        super(Type.OZONE, origProps);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        hydrateFromOriginalProps();
        super.initNormalizeAndCheckProps();
        hydrateFromOriginalProps();
    }

    private void hydrateFromOriginalProps() {
        endpoint = StringUtils.firstNonBlank(
                endpoint,
                origProps.get("ozone.endpoint"),
                origProps.get("s3.endpoint"));
        region = StringUtils.firstNonBlank(region, origProps.get("ozone.region"), origProps.get("s3.region"));
        accessKey = StringUtils.firstNonBlank(
                accessKey,
                origProps.get("ozone.access_key"),
                origProps.get("s3.access_key"),
                origProps.get("s3.access-key-id"));
        secretKey = StringUtils.firstNonBlank(
                secretKey,
                origProps.get("ozone.secret_key"),
                origProps.get("s3.secret_key"),
                origProps.get("s3.secret-access-key"));
        sessionToken = StringUtils.firstNonBlank(sessionToken, origProps.get("ozone.session_token"),
                origProps.get("s3.session_token"), origProps.get("s3.session-token"));
        usePathStyle = StringUtils.firstNonBlank(usePathStyle, origProps.get("ozone.use_path_style"),
                origProps.get("use_path_style"), origProps.get("s3.path-style-access"));
        forceParsingByStandardUrl = StringUtils.firstNonBlank(forceParsingByStandardUrl,
                origProps.get("ozone.force_parsing_by_standard_uri"),
                origProps.get("force_parsing_by_standard_uri"));
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
