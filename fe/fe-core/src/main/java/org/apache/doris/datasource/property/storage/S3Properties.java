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

import org.apache.paimon.options.Options;

import java.util.Map;

public class S3Properties extends StorageProperties {

    @ConnectorProperty(names = {"s3.endpoint",
            "oss.endpoint", "cos.endpoint", "obs.endpoint", "gcs.endpoint"},
            description = "The endpoint of S3.")
    protected String s3Endpoint = "";

    @ConnectorProperty(names = {"s3.region",
            "oss.region", "cos.region", "obs.region", "gcs.region"},
            description = "The region of S3.")
    protected String s3Region = "";

    @ConnectorProperty(names = {"s3.access_key",
            "oss.access_key", "cos.access_key", "obs.access_key", "gcs.access_key"},
            description = "The access key of S3.")
    protected String s3AccessKey = "";

    @ConnectorProperty(names = {"s3.secret_key",
            "oss.secret_key", "cos.secret_key", "obs.secret_key", "gcs.secret_key"},
            description = "The secret key of S3.")
    protected String s3SecretKey = "";

    @ConnectorProperty(names = {"s3.connection.maximum"},
            description = "The maximum number of connections to S3.")
    protected String s3ConnectionMaximum = "";

    @ConnectorProperty(names = {"s3.connection.request.timeout"},
            description = "The request timeout of S3 in second,")
    protected String s3ConnectionRequestTimeoutS = "";

    @ConnectorProperty(names = {"s3.connection.timeout"},
            description = "The connection timeout of S3 in second,")
    protected String s3ConnectionTimeoutS = "";

    @ConnectorProperty(names = {"s3.sts_endpoint"},
            description = "The sts endpoint of S3.")
    protected String s3StsEndpoint = "";

    @ConnectorProperty(names = {"s3.sts_region"},
            description = "The sts region of S3.")
    protected String s3StsRegion = "";

    @ConnectorProperty(names = {"s3.iam_role"},
            description = "The iam role of S3.")
    protected String s3IAMRole = "";

    @ConnectorProperty(names = {"s3.external_id"},
            description = "The external id of S3.")
    protected String s3ExternalId = "";

    public S3Properties(Map<String, String> origProps) {
        super(Type.S3, origProps);
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
}
