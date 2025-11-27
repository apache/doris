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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.metastore.AWSGlueMetaStoreBaseProperties;
import org.apache.doris.datasource.property.metastore.AbstractIcebergProperties;

import org.apache.commons.lang3.StringUtils;


public class IcebergGlueMetaStoreConnectivityTester extends AbstractIcebergConnectivityTester {
    private final AWSGlueMetaStoreBaseConnectivityTester glueTester;

    public IcebergGlueMetaStoreConnectivityTester(AbstractIcebergProperties properties,
            AWSGlueMetaStoreBaseProperties awsGlueMetaStoreBaseProperties) {
        super(properties);
        this.glueTester = new AWSGlueMetaStoreBaseConnectivityTester(awsGlueMetaStoreBaseProperties);
    }

    @Override
    public String getTestType() {
        return "Iceberg Glue";
    }

    @Override
    public String getErrorHint() {
        return "Please check AWS Glue credentials (access_key and secret_key or IAM role), "
                + "region, warehouse location, and endpoint";
    }

    @Override
    public void testConnection() throws Exception {
        glueTester.testConnection();
    }

    @Override
    public String getTestLocation() {
        String warehouse = properties.getWarehouse();
        if (StringUtils.isBlank(warehouse)) {
            return null;
        }

        String location = validateLocation(warehouse);
        if (location == null) {
            throw new IllegalArgumentException(
                    "Iceberg Glue warehouse location must be in S3 format (s3:// or s3a://), but got: " + warehouse);
        }
        return location;
    }
}
