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

package org.apache.doris.datasource.property.common;

import org.apache.doris.datasource.iceberg.IcebergS3FileIOAwsClientFactory;
import org.apache.doris.datasource.property.storage.S3Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.Map;

/**
 * Shared util for putting Iceberg AWS assume-role properties into a map.
 * Used by Iceberg REST (glue/s3tables signing), S3 Tables catalog, and S3 FileIO.
 */
public final class IcebergAwsAssumeRoleProperties {

    private IcebergAwsAssumeRoleProperties() {}

    /**
     * Puts assume-role related Iceberg client properties into the target map when roleArn is present.
     * No-op if roleArn is blank.
     */
    public static void putAssumeRoleProperties(Map<String, String> target, S3Properties s3Properties) {
        if (StringUtils.isBlank(s3Properties.getS3IAMRole())) {
            return;
        }
        target.putIfAbsent(S3FileIOProperties.CLIENT_FACTORY,
                IcebergS3FileIOAwsClientFactory.class.getName());
        target.put("aws.region", s3Properties.getRegion());
        target.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, s3Properties.getRegion());
        target.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, s3Properties.getS3IAMRole());
        if (StringUtils.isNotBlank(s3Properties.getS3ExternalId())) {
            target.put(AwsProperties.CLIENT_ASSUME_ROLE_EXTERNAL_ID, s3Properties.getS3ExternalId());
        }
    }
}
