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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
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
    public static final String S3_URI = "uri";

    private static final ImmutableSet<String> DEPRECATED_KEYS =
            ImmutableSet.of("access_key", "secret_key", "session_token", "region");

    private static final ImmutableSet<String> OPTIONAL_KEYS =
            ImmutableSet.of(S3Properties.SESSION_TOKEN, PropertyConverter.USE_PATH_STYLE, S3Properties.REGION,
                    PATH_PARTITION_KEYS);

    private static final ImmutableSet<String> LOCATION_PROPERTIES = ImmutableSet.<String>builder()
            .add(S3_URI)
            .add(S3Properties.ENDPOINT)
            .addAll(DEPRECATED_KEYS)
            .addAll(S3Properties.TVF_REQUIRED_FIELDS)
            .addAll(OPTIONAL_KEYS)
            .build();

    private final S3URI s3uri;
    private final boolean forceVirtualHosted;
    private String virtualBucket = "";

    public S3TableValuedFunction(Map<String, String> params) throws AnalysisException {

        Map<String, String> fileParams = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey();
            String lowerKey = key.toLowerCase();
            if (!LOCATION_PROPERTIES.contains(lowerKey) && !FILE_FORMAT_PROPERTIES.contains(lowerKey)) {
                throw new AnalysisException("Invalid property: " + key);
            }
            if (DEPRECATED_KEYS.contains(lowerKey)) {
                lowerKey = S3Properties.S3_PREFIX + lowerKey;
            }
            fileParams.put(lowerKey, entry.getValue());
        }

        if (!fileParams.containsKey(S3_URI)) {
            throw new AnalysisException("Missing required property: " + S3_URI);
        }

        forceVirtualHosted = isVirtualHosted(fileParams);
        s3uri = getS3Uri(fileParams);
        final String endpoint = forceVirtualHosted
                ? getEndpointAndSetVirtualBucket(params)
                : s3uri.getBucketScheme();
        if (!fileParams.containsKey(S3Properties.REGION)) {
            String region = S3Properties.getRegionOfEndpoint(endpoint);
            fileParams.put(S3Properties.REGION, region);
        }
        CloudCredentialWithEndpoint credential = new CloudCredentialWithEndpoint(endpoint,
                fileParams.get(S3Properties.REGION),
                fileParams.get(S3Properties.ACCESS_KEY),
                fileParams.get(S3Properties.SECRET_KEY));
        if (fileParams.containsKey(S3Properties.SESSION_TOKEN)) {
            credential.setSessionToken(fileParams.get(S3Properties.SESSION_TOKEN));
        }

        // set S3 location properties
        // these five properties is necessary, no one can be lost.
        locationProperties = S3Properties.credentialToMap(credential);
        String usePathStyle = fileParams.getOrDefault(PropertyConverter.USE_PATH_STYLE, "false");
        locationProperties.put(PropertyConverter.USE_PATH_STYLE, usePathStyle);
        this.locationProperties.putAll(S3ClientBEProperties.getBeFSProperties(this.locationProperties));

        super.parseProperties(fileParams);

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

    private String getEndpointAndSetVirtualBucket(Map<String, String> params) throws AnalysisException {
        Preconditions.checkState(forceVirtualHosted, "only invoked when force virtual hosted.");
        String[] fileds = s3uri.getVirtualBucket().split("\\.", 2);
        virtualBucket = fileds[0];
        if (fileds.length > 1) {
            // At this point, s3uri.getVirtualBucket() is: virtualBucket.endpoint, Eg:
            //          uri: http://my_bucket.cos.ap-beijing.myqcloud.com/file.txt
            // s3uri.getVirtualBucket() = my_bucket.cos.ap-beijing.myqcloud.com,
            // so we need separate virtualBucket and endpoint.
            return fileds[1];
        } else if (params.containsKey(S3Properties.ENDPOINT)) {
            return params.get(S3Properties.ENDPOINT);
        } else {
            throw new AnalysisException("can not parse endpoint, please check uri.");
        }
    }

    private boolean isVirtualHosted(Map<String, String> validParams) {
        String originUri = validParams.getOrDefault(S3_URI, "");
        if (originUri.toLowerCase().startsWith("s3")) {
            // s3 protocol, default virtual-hosted style
            return true;
        } else {
            // not s3 protocol, forceVirtualHosted is determined by USE_PATH_STYLE.
            return !Boolean.parseBoolean(validParams.get(PropertyConverter.USE_PATH_STYLE));
        }
    }

    private S3URI getS3Uri(Map<String, String> validParams) throws AnalysisException {
        try {
            return S3URI.create(validParams.get(S3_URI), forceVirtualHosted);
        } catch (UserException e) {
            throw new AnalysisException("parse s3 uri failed, uri = " + validParams.get(S3_URI), e);
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
