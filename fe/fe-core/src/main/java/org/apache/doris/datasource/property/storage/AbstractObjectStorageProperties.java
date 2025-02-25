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

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for object storage system properties. This class provides common configuration
 * settings for object storage systems and supports conversion of these properties into configuration
 * maps for different protocols, such as AWS S3. All object storage systems should extend this class
 * to inherit the common configuration properties and methods.
 * <p>
 * The properties include connection settings (e.g., timeouts and maximum connections) and a flag to
 * determine if path-style URLs should be used for the storage system.
 */
public abstract class AbstractObjectStorageProperties extends StorageProperties implements ObjectStorageProperties {

    /**
     * The maximum number of concurrent connections that can be made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"maxConnections"}, required = false, description = "Maximum number of connections.")
    protected int maxConnections = 100;

    /**
     * The timeout (in milliseconds) for requests made to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"requestTimeoutS"}, required = false, description = "Request timeout in seconds.")
    protected int requestTimeoutS = 10000;

    /**
     * The timeout (in milliseconds) for establishing a connection to the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Getter
    @ConnectorProperty(names = {"connectionTimeoutS"}, required = false, description = "Connection timeout in seconds.")
    protected int connectionTimeoutS = 10000;

    /**
     * Flag indicating whether to use path-style URLs for the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Setter
    @Getter
    @ConnectorProperty(names = {"usePathStyle"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected boolean usePathStyle = false;

    /**
     * Constructor to initialize the object storage properties with the provided type and original properties map.
     *
     * @param type      the type of object storage system.
     * @param origProps the original properties map.
     */
    protected AbstractObjectStorageProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }

    /**
     * Generates a map of configuration properties for AWS S3 based on the provided values.
     * This map includes various AWS-specific settings like endpoint, region, access keys, and timeouts.
     *
     * @param endpoint           the AWS endpoint URL.
     * @param region             the AWS region.
     * @param accessKey          the AWS access key.
     * @param secretKey          the AWS secret key.
     * @param maxConnections     the maximum number of connections.
     * @param requestTimeoutS    the request timeout in milliseconds.
     * @param connectionTimeoutS the connection timeout in milliseconds.
     * @param usePathStyle       flag indicating if path-style URLs should be used.
     * @return a map containing AWS S3-specific configuration properties.
     */
    protected Map<String, String> generateAWSS3Properties(String endpoint, String region, String accessKey,
                                                          String secretKey, String maxConnections,
                                                          String requestTimeoutS,
                                                          String connectionTimeoutS, String usePathStyle) {
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("AWS_ENDPOINT", endpoint);
        s3Props.put("AWS_REGION", region);
        s3Props.put("AWS_ACCESS_KEY", accessKey);
        s3Props.put("AWS_SECRET_KEY", secretKey);
        s3Props.put("AWS_MAX_CONNECTIONS", maxConnections);
        s3Props.put("AWS_REQUEST_TIMEOUT_MS", requestTimeoutS);
        s3Props.put("AWS_CONNECTION_TIMEOUT_MS", connectionTimeoutS);
        s3Props.put("use_path_style", usePathStyle);
        return s3Props;
    }

    /**
     * Generates a map of configuration properties for AWS S3 using the default values from this class.
     * This method automatically uses the values defined in the class for connection settings and path-style URL flag.
     *
     * @param endpoint  the AWS endpoint URL.
     * @param region    the AWS region.
     * @param accessKey the AWS access key.
     * @param secretKey the AWS secret key.
     * @return a map containing AWS S3-specific configuration properties.
     */
    protected Map<String, String> generateAWSS3Properties(String endpoint, String region,
                                                          String accessKey, String secretKey) {
        return generateAWSS3Properties(endpoint, region, accessKey, secretKey,
                String.valueOf(getMaxConnections()), String.valueOf(getRequestTimeoutS()),
                String.valueOf(getConnectionTimeoutS()), String.valueOf(isUsePathStyle()));
    }
}
