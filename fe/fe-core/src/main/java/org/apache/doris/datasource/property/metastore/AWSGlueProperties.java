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
import org.apache.doris.datasource.property.PropertyUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class AWSGlueProperties extends MetastoreProperties {

    @ConnectorProperty(names = {"glue.endpoint", "aws.region"},
            description = "The endpoint of the AWS Glue.")
    private String glueEndpoint = "";

    @ConnectorProperty(names = {"glue.access_key",
            "aws.glue.access-key", "client.credentials-provider.glue.access_key"},
            description = "The access key of the AWS Glue.")
    private String glueAccessKey = "";

    @ConnectorProperty(names = {"glue.secret_key",
            "aws.glue.secret-key", "client.credentials-provider.glue.secret_key"},
            description = "The secret key of the AWS Glue.")
    private String glueSecretKey = "";

    @ConnectorProperty(names = {"glue.catalog_id"},
            description = "The catalog id of the AWS Glue.",
            supported = false)
    private String glueCatalogId = "";

    @ConnectorProperty(names = {"glue.iam_role"},
            description = "The IAM role the AWS Glue.",
            supported = false)
    private String glueIAMRole = "";

    @ConnectorProperty(names = {"glue.external_id"},
            description = "The external id of the AWS Glue.",
            supported = false)
    private String glueExternalId = "";

    // ===== following is converted properties ======
    public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
    public static final String CLIENT_CREDENTIALS_PROVIDER_AK = "client.credentials-provider.glue.access_key";
    public static final String CLIENT_CREDENTIALS_PROVIDER_SK = "client.credentials-provider.glue.secret_key";

    public AWSGlueProperties(Map<String, String> origProps) {
        super(Type.GLUE);
        normalizedProps(origProps);
    }

    private void normalizedProps(Map<String, String> origProps) {
        // 1. set fields from original properties
        List<Field> supportedProps = PropertyUtils.getConnectorProperties(this.getClass());
        for (Field field : supportedProps) {
            field.setAccessible(true);
            ConnectorProperty anno = field.getAnnotation(ConnectorProperty.class);
            String[] names = anno.names();
            for (String name : names) {
                if (origProps.containsKey(name)) {
                    try {
                        field.set(this, origProps.get(name));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Failed to set property " + name + ", " + e.getMessage(), e);
                    }
                    break;
                }
            }
        }
        // 2. check properties

    }
}
