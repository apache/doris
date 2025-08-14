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
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

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

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        // Check if credentials are provided properly - either both or neither
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            return;
        }
        // Allow anonymous access if both access_key and secret_key are empty
        if (StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)) {
            return;
        }
        // If only one is provided, it's an error
        throw new StoragePropertiesException(
                        "Please set access_key and secret_key or omit both for anonymous access to public bucket.");
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
