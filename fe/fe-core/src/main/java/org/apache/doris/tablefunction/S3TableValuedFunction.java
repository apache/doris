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
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.S3ClientBEProperties;
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

    private String virtualBucket = "";

    public S3TableValuedFunction(Map<String, String> properties) throws AnalysisException {
        // 1. analyze common properties
        Map<String, String> otherProps = super.parseCommonProperties(properties);

        // 2. analyze uri and other properties
        String uriStr = getOrDefaultAndRemove(otherProps, PROP_URI, null);
        if (Strings.isNullOrEmpty(uriStr)) {
            throw new AnalysisException(String.format("Properties '%s' is required.", PROP_URI));
        }
        forwardCompatibleDeprecatedKeys(otherProps);

        String usePathStyle = getOrDefaultAndRemove(otherProps, PropertyConverter.USE_PATH_STYLE, "false");
        boolean forceVirtualHosted = isVirtualHosted(uriStr, Boolean.parseBoolean(usePathStyle));
        S3URI s3uri = getS3Uri(uriStr, forceVirtualHosted);
        String endpoint = forceVirtualHosted
                ? getEndpointAndSetVirtualBucket(s3uri, otherProps) : s3uri.getBucketScheme();
        if (!otherProps.containsKey(S3Properties.REGION)) {
            String region = S3Properties.getRegionOfEndpoint(endpoint);
            otherProps.put(S3Properties.REGION, region);
        }
        checkNecessaryS3Properties(otherProps);
        CloudCredentialWithEndpoint credential = new CloudCredentialWithEndpoint(endpoint,
                otherProps.get(S3Properties.REGION),
                otherProps.get(S3Properties.ACCESS_KEY),
                otherProps.get(S3Properties.SECRET_KEY));
        if (otherProps.containsKey(S3Properties.SESSION_TOKEN)) {
            credential.setSessionToken(otherProps.get(S3Properties.SESSION_TOKEN));
        }

        locationProperties = S3Properties.credentialToMap(credential);
        locationProperties.put(PropertyConverter.USE_PATH_STYLE, usePathStyle);
        locationProperties.putAll(S3ClientBEProperties.getBeFSProperties(locationProperties));

        if (forceVirtualHosted) {
            filePath = NAME + S3URI.SCHEME_DELIM + virtualBucket + S3URI.PATH_DELIM
                + s3uri.getBucket() + S3URI.PATH_DELIM + s3uri.getKey();
        } else {
            filePath = NAME + S3URI.SCHEME_DELIM + s3uri.getKey();
        }

        if (FeConstants.runningUnitTest) {
            // Just check
            FileSystemFactory.getS3FileSystem(locationProperties);
        } else {
            parseFile();
        }
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
        if (Strings.isNullOrEmpty(props.get(S3Properties.ACCESS_KEY))) {
            throw new AnalysisException(String.format("Properties '%s' is required.", S3Properties.ACCESS_KEY));
        }
        if (Strings.isNullOrEmpty(props.get(S3Properties.SECRET_KEY))) {
            throw new AnalysisException(String.format("Properties '%s' is required.", S3Properties.SECRET_KEY));
        }
    }

    private String getEndpointAndSetVirtualBucket(S3URI s3uri, Map<String, String> props)
            throws AnalysisException {
        String[] fields = s3uri.getVirtualBucket().split("\\.", 2);
        virtualBucket = fields[0];
        if (fields.length > 1) {
            // At this point, s3uri.getVirtualBucket() is: virtualBucket.endpoint, Eg:
            //          uri: http://my_bucket.cos.ap-beijing.myqcloud.com/file.txt
            // s3uri.getVirtualBucket() = my_bucket.cos.ap-beijing.myqcloud.com,
            // so we need separate virtualBucket and endpoint.
            return fields[1];
        } else if (props.containsKey(S3Properties.ENDPOINT)) {
            return props.get(S3Properties.ENDPOINT);
        } else {
            throw new AnalysisException("can not parse endpoint, please check uri.");
        }
    }

    private boolean isVirtualHosted(String uri, boolean usePathStyle) {
        if (uri.toLowerCase().startsWith("s3")) {
            // s3 protocol, default virtual-hosted style
            return true;
        } else {
            // not s3 protocol, forceVirtualHosted is determined by USE_PATH_STYLE.
            return !usePathStyle;
        }
    }

    private S3URI getS3Uri(String uri, boolean forceVirtualHosted) throws AnalysisException {
        try {
            return S3URI.create(uri, forceVirtualHosted);
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

