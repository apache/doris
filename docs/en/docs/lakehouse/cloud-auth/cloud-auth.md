---
{
    "title": "Cloud Service Authentication",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Overview

When accessing a service on the cloud, we need to provide the credentials needed to access the service so that the service can be authenticated by IAM of cloud vendors.

## AWS

Now Doris support two types of authentication to access AWS service.

### Catalog Credentials

The Catalog supports filling in basic Credentials properties, such as:
1. For S3: `s3.endpoint`，`s3.access_key`，`s3.secret_key`。
2. For Glue: `glue.endpoint`，`glue.access_key`，`glue.secret_key`。

When access Glue though Iceberg Catalog, we can access tables on Glue by filling in the following properties:

```sql
CREATE CATALOG glue PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type" = "glue",
    "glue.endpoint" = "https://glue.us-east-1.amazonaws.com",
    "glue.access_key" = "ak",
    "glue.secret_key" = "sk"
);
```

### System Credentials

For applications running on AWS resources, such as EC2 instances, this approach enhances security by avoiding hardcoded credentials.

If we create the Catalog but not fill any Credentials in properties, the `DefaultAWSCredentialsProviderChain` will be used to read in the system environment variables or instance profile.

For details about how to configure environment variables and system properties, see: [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) .
- The configurable environment variables are: `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`、`AWS_ROLE_ARN`、`AWS_WEB_IDENTITY_TOKEN_FILE` and so on.
- In addition, you can also use [aws configure](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) to configure Credentials, the Credentials file will be written to the `~/.aws` directory.
