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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.ConnectorProperty;

public class AWSGlueProperties extends MetastoreProperties {

    @ConnectorProperty(name = "glue.endpoint",
            description = "The endpoint of the AWS Glue.")
    private String glueEndpoint = "";

    @ConnectorProperty(name = "glue.access_key",
            description = "The access key of the AWS Glue.")
    private String glueAccessKey = "";

    @ConnectorProperty(name = "glue.secret_key",
            description = "The secret key of the AWS Glue.")
    private String glueSecretKey = "";

    @ConnectorProperty(name = "glue.catalog_id",
            description = "The catalog id of the AWS Glue.")
    private String glueCatalogId = "";

    @ConnectorProperty(name = "glue.iam_role",
            description = "The IAM role the AWS Glue.")
    private String glueIAMRole = "";

    @ConnectorProperty(name = "glue.external_id",
            description = "The external id of the AWS Glue.")
    private String glueExternalId = "";
}
