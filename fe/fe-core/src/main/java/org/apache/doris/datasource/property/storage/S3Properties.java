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
import org.apache.doris.datasource.property.metastore.AWSGlueProperties;
import org.apache.doris.datasource.property.metastore.AliyunDLFProperties;

import com.google.common.collect.Lists;
import org.apache.paimon.options.Options;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3Properties extends AbstractObjectStorageProperties {


    @ConnectorProperty(names = {"s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of S3.")
    protected String s3Endpoint = "";

    @ConnectorProperty(names = {"s3.region", "AWS_REGION", "region", "REGION"},
            required = false,
            description = "The region of S3.")
    protected String s3Region = "us-east-1";

    @ConnectorProperty(names = {"s3.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key"},
            description = "The access key of S3.")
    protected String s3AccessKey = "";

    @ConnectorProperty(names = {"s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            description = "The secret key of S3.")
    protected String s3SecretKey = "";


    @ConnectorProperty(names = {"s3.connection.maximum",
            "AWS_MAX_CONNECTIONS"},
            required = false,
            description = "The maximum number of connections to S3.")
    protected String s3ConnectionMaximum = "50";

    @ConnectorProperty(names = {"s3.connection.request.timeout",
            "AWS_REQUEST_TIMEOUT_MS"},
            required = false,
            description = "The request timeout of S3 in milliseconds,")
    protected String s3ConnectionRequestTimeoutS = "3000";

    @ConnectorProperty(names = {"s3.connection.timeout",
            "AWS_CONNECTION_TIMEOUT_MS"},
            required = false,
            description = "The connection timeout of S3 in milliseconds,")
    protected String s3ConnectionTimeoutS = "1000";

    @ConnectorProperty(names = {"s3.sts_endpoint"},
            supported = false,
            required = false,
            description = "The sts endpoint of S3.")
    protected String s3StsEndpoint = "";

    @ConnectorProperty(names = {"s3.sts_region"},
            supported = false,
            required = false,
            description = "The sts region of S3.")
    protected String s3StsRegion = "";

    @ConnectorProperty(names = {"s3.iam_role"},
            supported = false,
            required = false,
            description = "The iam role of S3.")
    protected String s3IAMRole = "";

    @ConnectorProperty(names = {"s3.external_id"},
            supported = false,
            required = false,
            description = "The external id of S3.")
    protected String s3ExternalId = "";

    public S3Properties(Map<String, String> origProps) {
        super(Type.S3, origProps);
    }


    /**
     * Guess if the storage properties is for this storage type.
     * Subclass should override this method to provide the correct implementation.
     *
     * @return
     */
    public static boolean guessIsMe(Map<String, String> origProps) {
        List<Field> fields = getIdentifyFields();
        return StorageProperties.checkIdentifierKey(origProps, fields);
    }

    private static List<Field> getIdentifyFields() {
        List<Field> fields = Lists.newArrayList();
        try {
            //todo AliyunDlfProperties should in OSS storage type.
            fields.add(S3Properties.class.getDeclaredField("s3AccessKey"));
            fields.add(AliyunDLFProperties.class.getDeclaredField("dlfAccessKey"));
            fields.add(AWSGlueProperties.class.getDeclaredField("glueAccessKey"));
            return fields;
        } catch (NoSuchFieldException e) {
            // should not happen
            throw new RuntimeException("Failed to get field: " + e.getMessage(), e);
        }
    }

    public void toPaimonOSSFileIOProperties(Options options) {
        options.set("fs.oss.endpoint", s3Endpoint);
        options.set("fs.oss.accessKeyId", s3AccessKey);
        options.set("fs.oss.accessKeySecret", s3SecretKey);
    }

    public void toPaimonS3FileIOProperties(Options options) {
        options.set("s3.endpoint", s3Endpoint);
        options.set("s3.access-key", s3AccessKey);
        options.set("s3.secret-key", s3SecretKey);
    }

    public void toIcebergS3FileIOProperties(Map<String, String> catalogProps) {
        // See S3FileIOProperties.java
        catalogProps.put("s3.endpoint", s3Endpoint);
        catalogProps.put("s3.access-key-id", s3AccessKey);
        catalogProps.put("s3.secret-access-key", s3SecretKey);
        catalogProps.put("client.region", s3Region);
        catalogProps.put("s3.path-style-access", usePathStyle);
    }

    @Override
    public void toNativeS3Configuration(Map<String, String> config) {
        Map<String, String> awsS3Properties = generateAWSS3Properties(s3Endpoint, s3Region, s3AccessKey, s3SecretKey,
                s3ConnectionMaximum, s3ConnectionRequestTimeoutS, s3ConnectionTimeoutS, String.valueOf(usePathStyle));
        config.putAll(awsS3Properties);
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        Map<String, String> config = new HashMap<>();
        toNativeS3Configuration(config);
        return config;
    }

    @Override
    public String getEndpoint() {
        return s3Endpoint;
    }

    @Override
    public String getRegion() {
        return s3Region;
    }

    @Override
    public String getAccessKey() {
        return s3AccessKey;
    }

    @Override
    public String getSecretKey() {
        return s3SecretKey;
    }

    @Override
    public void setEndpoint(String endpoint) {
        this.s3Endpoint = endpoint;
    }
}
