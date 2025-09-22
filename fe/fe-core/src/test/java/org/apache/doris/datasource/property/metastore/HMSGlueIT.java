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

import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.hive.ThriftHMSCachedClient;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/*
 * This is for local development and testing only. Use actual environment settings in production
 */
public class HMSGlueIT {

    public static void main(String[] args) throws UserException {
        Map<String, String> baseProps = ImmutableMap.of(
                "type", "hms",
                "hive.metastore.type", "glue",
                "glue.role_arn", "arn:aws:iam::1001:role/doris-glue-role",
                "glue.external_id", "1001",
                "glue.endpoint", "https://glue.us-east-1.amazonaws.com"
        );
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "");
        System.setProperty("aws.secretKey", "");
        HMSGlueMetaStoreProperties properties = (HMSGlueMetaStoreProperties) MetastoreProperties.create(baseProps);
        ThriftHMSCachedClient client = new ThriftHMSCachedClient(properties.hiveConf, 1, new ExecutionAuthenticator() {
        });
        client.getTable("default", "test_hive_table");
    }
}
