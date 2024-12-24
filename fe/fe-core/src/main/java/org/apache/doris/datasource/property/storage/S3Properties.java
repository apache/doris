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

public class S3Properties extends StorageProperties {

    @ConnectorProperty(name = "s3.endpoint",
            alternativeNames = {"oss.endpoint", "cos.endpoint", "obs.endpoint", "gcs.endpoint"},
            description = "The endpoint of S3.")
    protected String s3Endpoint = "";

    @ConnectorProperty(name = "s3.region",
            alternativeNames = {"oss.region", "cos.region", "obs.region", "gcs.region"},
            description = "The region of S3.")
    protected String s3Region = "";

    @ConnectorProperty(name = "s3.access_key",
            alternativeNames = {"oss.access_key", "cos.access_key", "obs.access_key", "gcs.access_key"},
            description = "The access key of S3.")
    protected String s3AccessKey = "";

    @ConnectorProperty(name = "s3.secret_key",
            alternativeNames = {"oss.secret_key", "cos.secret_key", "obs.secret_key", "gcs.secret_key"},
            description = "The secret key of S3.")
    protected String s3SecretKey = "";

    @ConnectorProperty(name = "s3.connection.maximum",
            description = "The maximum number of connections to S3.")
    protected String s3ConnectionMaximum = "";

    @ConnectorProperty(name = "s3.connection.request.timeout",
            description = "The request timeout of S3 in second,")
    protected String s3ConnectionRequestTimeoutS = "";

    @ConnectorProperty(name = "s3.connection.timeout",
            description = "The connection timeout of S3 in second,")
    protected String s3ConnectionTimeoutS = "";

    @ConnectorProperty(name = "s3.sts_endpoint",
            description = "The sts endpoint of S3.")
    protected String s3StsEndpoint = "";

    @ConnectorProperty(name = "s3.sts_region",
            description = "The sts region of S3.")
    protected String s3StsRegion = "";

    @ConnectorProperty(name = "s3.iam_role",
            description = "The iam role of S3.")
    protected String s3IAMRole = "";

    @ConnectorProperty(name = "s3.external_id",
            description = "The external id of S3.")
    protected String s3ExternalId = "";
}
