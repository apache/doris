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
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.S3ClientBEProperties;
import org.apache.doris.datasource.property.constants.AzureProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.Map;

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
    public static final String PROP_URI = "uri";

    private static final ImmutableSet<String> DEPRECATED_KEYS =
            ImmutableSet.of("access_key", "secret_key", "session_token", "region",
                    "ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN", "REGION");

    public S3TableValuedFunction(Map<String, String> properties) throws AnalysisException {
        final boolean isAzureTvf = AzureProperties.checkAzureProviderPropertyExist(properties);
        // Azure could run without region
        if (isAzureTvf) {
            properties.put(S3Properties.REGION, "DUMMY-REGION");
        }
        // 1. analyze common properties
        Map<String, String> otherProps = super.parseCommonProperties(properties);

        // 2. analyze uri and other properties
        String uriStr = getOrDefaultAndRemove(otherProps, PROP_URI, null);
        if (Strings.isNullOrEmpty(uriStr)) {
            throw new AnalysisException(String.format("Properties '%s' is required.", PROP_URI));
        }
        forwardCompatibleDeprecatedKeys(otherProps);

        String usePathStyle = getOrDefaultAndRemove(otherProps, PropertyConverter.USE_PATH_STYLE,
                PropertyConverter.USE_PATH_STYLE_DEFAULT_VALUE);
        String forceParsingByStandardUri = getOrDefaultAndRemove(otherProps,
                PropertyConverter.FORCE_PARSING_BY_STANDARD_URI,
                PropertyConverter.FORCE_PARSING_BY_STANDARD_URI_DEFAULT_VALUE);

        S3URI s3uri = getS3Uri(uriStr, Boolean.parseBoolean(usePathStyle.toLowerCase()),
                Boolean.parseBoolean(forceParsingByStandardUri.toLowerCase()));

        // get endpoint first from properties, if not present, get it from s3 uri.
        // If endpoint is missing, exception will be thrown.
        String endpoint = constructEndpoint(otherProps, s3uri);
        if (!otherProps.containsKey(S3Properties.REGION)) {
            String region = s3uri.getRegion().orElseThrow(() ->
                    new AnalysisException(String.format("Properties '%s' is required.", S3Properties.REGION)));
            otherProps.put(S3Properties.REGION, region);
        }
        checkNecessaryS3Properties(otherProps);
        CloudCredentialWithEndpoint credential = new CloudCredentialWithEndpoint(endpoint,
                getOrDefaultAndRemove(otherProps, S3Properties.REGION, ""),
                getOrDefaultAndRemove(otherProps, S3Properties.ACCESS_KEY, ""),
                getOrDefaultAndRemove(otherProps, S3Properties.SECRET_KEY, ""));
        if (otherProps.containsKey(S3Properties.SESSION_TOKEN)) {
            credential.setSessionToken(getOrDefaultAndRemove(otherProps, S3Properties.SESSION_TOKEN, ""));
        }

        locationProperties = S3Properties.credentialToMap(credential);
        locationProperties.put(PropertyConverter.USE_PATH_STYLE, usePathStyle);
        if (isAzureTvf) {
            // For Azure's compatibility, we need bucket to connect to the blob storage's container
            locationProperties.put(S3Properties.BUCKET, s3uri.getBucket());
        }
        locationProperties.putAll(S3ClientBEProperties.getBeFSProperties(locationProperties));
        locationProperties.putAll(otherProps);

        filePath = NAME + S3URI.SCHEME_DELIM + s3uri.getBucket() + S3URI.PATH_DELIM + s3uri.getKey();

        if (FeConstants.runningUnitTest) {
            // Just check
            FileSystemFactory.getS3FileSystem(locationProperties);
        } else {
            parseFile();
        }
    }

    private String constructEndpoint(Map<String, String> properties, S3URI s3uri) throws AnalysisException {
        String endpoint;
        if (!AzureProperties.checkAzureProviderPropertyExist(properties)) {
            // get endpoint first from properties, if not present, get it from s3 uri.
            // If endpoint is missing, exception will be thrown.
            endpoint = getOrDefaultAndRemove(properties, S3Properties.ENDPOINT, s3uri.getEndpoint().orElse(""));
            if (Strings.isNullOrEmpty(endpoint)) {
                throw new AnalysisException(String.format("Properties '%s' is required.", S3Properties.ENDPOINT));
            }
        } else {
            String bucket = s3uri.getBucket();
            String accountName = properties.getOrDefault(S3Properties.ACCESS_KEY, "");
            if (accountName.isEmpty()) {
                throw new AnalysisException(String.format("Properties '%s' is required.", S3Properties.ACCESS_KEY));
            }
            endpoint = String.format(AzureProperties.AZURE_ENDPOINT_TEMPLATE, accountName, bucket);
        }
        return endpoint;
    }

    private void forwardCompatibleDeprecatedKeys(Map<String, String> props) {
        for (String deprecatedKey : DEPRECATED_KEYS) {
            String value = props.remove(deprecatedKey);
            if (!Strings.isNullOrEmpty(value)) {
                props.put("s3." + deprecatedKey.toLowerCase(), value);
            }
        }
    }

    private void checkNecessaryS3Properties(Map<String, String> props) throws AnalysisException {
        if (Strings.isNullOrEmpty(props.get(S3Properties.REGION))) {
            throw new AnalysisException(String.format("Properties '%s' is required.", S3Properties.REGION));
        }
        // do not check ak and sk, because we can read them from system environment.
    }

    private S3URI getS3Uri(String uri, boolean isPathStyle, boolean forceParsingStandardUri) throws AnalysisException {
        try {
            return S3URI.create(uri, isPathStyle, forceParsingStandardUri);
        } catch (UserException e) {
            throw new AnalysisException("parse s3 uri failed, uri = " + uri, e);
        }
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_S3;
    }

    @Override
    public String getFilePath() {
        // must be "s3://..."
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("S3TvfBroker", StorageType.S3, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "S3TableValuedFunction";
    }
}

