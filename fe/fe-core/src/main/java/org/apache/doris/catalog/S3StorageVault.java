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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * S3 storage vault
 * <p>
 * Syntax:
 * CREATE STORAGE VAULT "remote_s3"
 * PROPERTIES
 * (
 * "type" = "s3",
 * "AWS_ENDPOINT" = "bj",
 * "AWS_REGION" = "bj",
 * "AWS_ROOT_PATH" = "/path/to/root",
 * "AWS_ACCESS_KEY" = "bbb",
 * "AWS_SECRET_KEY" = "aaaa",
 * "AWS_MAX_CONNECTION" = "50",
 * "AWS_REQUEST_TIMEOUT_MS" = "3000",
 * "AWS_CONNECTION_TIMEOUT_MS" = "1000"
 * );
 * <p>
 * For AWS S3, BE need following properties:
 * 1. AWS_ACCESS_KEY: ak
 * 2. AWS_SECRET_KEY: sk
 * 3. AWS_ENDPOINT: s3.us-east-1.amazonaws.com
 * 4. AWS_REGION: us-east-1
 * And file path: s3://bucket_name/csv/taxi.csv
 */
public class S3StorageVault extends StorageVault {
    // Reuse all the code from S3Resource
    private Resource resource;
    public static final String S3_ROOT_PATH = "s3.root.path";

    public static class PropertyKey {
        public static final String ACCESS_KEY = "s3.access_key";
        public static final String SECRET_KEY = "s3.secret_key";
        public static final String USE_PATH_STYLE = "use_path_style";
        public static final String ROOT_PATH = S3_ROOT_PATH;
        public static final String PROVIDER = "provider";
        public static final String REGION = "s3.region";
        public static final String ENDPOINT = "s3.endpoint";
        public static final String BUCKET = "s3.bucket";
        public static final String ROLE_ARN = "s3.role_arn";
        public static final String EXTERNAL_ID = "s3.external_id";
        public static final String CREDENTIALS_PROVIDER_TYPE = "s3.credentials_provider_type";
    }

    public static final HashSet<String> ALLOW_ALTER_PROPERTIES = new HashSet<>(Arrays.asList(
            StorageVault.PropertyKey.VAULT_NAME,
            StorageVault.PropertyKey.TYPE,
            PropertyKey.ACCESS_KEY,
            PropertyKey.SECRET_KEY,
            PropertyKey.USE_PATH_STYLE,
            PropertyKey.ROLE_ARN,
            PropertyKey.EXTERNAL_ID,
            PropertyKey.CREDENTIALS_PROVIDER_TYPE
    ));

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public S3StorageVault(String name, boolean ifNotExists,
            boolean setAsDefault, CreateResourceCommand command) throws DdlException {
        super(name, StorageVault.StorageVaultType.S3, ifNotExists, setAsDefault);
        resource = Resource.fromCommand(command);
    }

    @Override
    public void modifyProperties(ImmutableMap<String, String> properties) throws DdlException {
        resource.setProperties(properties);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return resource.getCopiedProperties();
    }

    @Override
    public void checkCreationProperties(Map<String, String> properties) throws UserException {
        super.checkCreationProperties(properties);
        Preconditions.checkArgument(
                properties.get(PropertyKey.ROOT_PATH) != null, "Missing property " + PropertyKey.ROOT_PATH);
        Preconditions.checkArgument(
                !properties.get(PropertyKey.ROOT_PATH).isEmpty(),
                "Property " + PropertyKey.ROOT_PATH + " cannot be empty");

    }

}
