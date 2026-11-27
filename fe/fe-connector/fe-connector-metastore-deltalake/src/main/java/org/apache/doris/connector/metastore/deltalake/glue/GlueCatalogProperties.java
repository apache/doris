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

package org.apache.doris.connector.metastore.deltalake.glue;

import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Credentials and endpoints for the Glue control plane, independent of table storage. */
public final class GlueCatalogProperties extends AbstractMetaStoreProperties {
    public static final String ENDPOINT = "glue.endpoint";
    public static final String AWS_ENDPOINT = "aws.endpoint";
    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
    private static final Pattern REGIONAL_HOST = Pattern.compile(
            "glue(?:-fips)?\\.([a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+)\\."
                    + "(?:amazonaws\\.com(?:\\.cn)?|api\\.aws|api\\.amazonwebservices\\.com\\.cn)\\.?");

    @ConnectorProperty(names = {ENDPOINT, AWS_ENDPOINT, AWS_GLUE_ENDPOINT}, required = false,
            description = "Optional AWS Glue HTTP(S) endpoint override.")
    private String endpoint;

    @ConnectorProperty(names = {"glue.region", "aws.region", "aws.glue.region"}, required = false,
            description = "AWS region for Glue and STS.")
    private String region;

    @ConnectorProperty(names = {"glue.access_key", "aws.glue.access-key"}, required = false, sensitive = true,
            description = "Access key for the Glue credentials.")
    private String accessKey;

    @ConnectorProperty(names = {"glue.secret_key", "aws.glue.secret-key"}, required = false, sensitive = true,
            description = "Secret key for the Glue credentials.")
    private String secretKey;

    @ConnectorProperty(names = {"glue.session_token", "aws.glue.session-token"}, required = false, sensitive = true,
            description = "Session token accompanying temporary Glue credentials.")
    private String sessionToken;

    @ConnectorProperty(names = {"glue.role_arn"}, required = false, description = "IAM role to assume for Glue.")
    private String roleArn;

    @ConnectorProperty(names = {"glue.external_id"}, required = false, description = "External ID for STS AssumeRole.")
    private String externalId;

    @ConnectorProperty(names = {"glue.catalog_id"}, required = false, description = "AWS Glue catalog ID.")
    private String catalogId;

    private GlueCatalogProperties(Map<String, String> raw) {
        super(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(this, raw);
    }

    static GlueCatalogProperties of(Map<String, String> raw) {
        return new GlueCatalogProperties(raw);
    }

    @Override
    public String providerName() {
        return "GLUE";
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    @Override
    public void validate() {
        region();
        if ((accessKey == null) != (secretKey == null)) {
            throw new IllegalArgumentException("glue.access_key and glue.secret_key must be set together");
        }
        if (sessionToken != null && accessKey == null) {
            throw new IllegalArgumentException("glue.session_token requires glue.access_key and glue.secret_key");
        }
        if (externalId != null && roleArn == null) {
            throw new IllegalArgumentException("glue.external_id requires glue.role_arn");
        }
    }

    public URI endpoint() {
        if (endpoint == null) {
            return null;
        }
        try {
            URI uri = new URI(endpoint);
            if (("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                    && uri.getHost() != null && uri.getPort() <= 65535
                    && uri.getRawUserInfo() == null && uri.getRawQuery() == null && uri.getRawFragment() == null) {
                return uri;
            }
        } catch (URISyntaxException ignored) {
            // Avoid including an unparsed URL in the validation error.
        }
        throw new IllegalArgumentException("glue.endpoint must be a valid HTTP(S) endpoint with a host");
    }

    public String region() {
        URI override = endpoint();
        if (region != null) {
            return region;
        }
        if (override != null) {
            Matcher matcher = REGIONAL_HOST.matcher(override.getHost().toLowerCase(Locale.ROOT));
            if (matcher.matches()) {
                return matcher.group(1);
            }
        }
        throw new IllegalArgumentException(
                "glue.region is required unless glue.endpoint names a regional AWS Glue endpoint");
    }

    public String accessKey() {
        return accessKey;
    }

    public String secretKey() {
        return secretKey;
    }

    public String sessionToken() {
        return sessionToken;
    }

    public String roleArn() {
        return roleArn;
    }

    public String externalId() {
        return externalId;
    }

    public String catalogId() {
        return catalogId;
    }
}
