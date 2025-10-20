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
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.SupportsNamespaces;

import java.util.List;
import java.util.Map;

/*
 * This is for local development and testing only. Use actual environment settings in production
 */
public class IcebergGlueIT {
    public static void main(String[] args) throws UserException {

        Map<String, String> baseProps = ImmutableMap.of(
                "type", "iceberg",
                "iceberg.catalog.type", "glue",
                "glue.role_arn", "arn:aws:iam::12345:role/kristen",
                "glue.external_id", "1001",
                "glue.endpoint", "https://glue.us-east-1.amazonaws.com"
        );
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "acc");
        System.setProperty("aws.secretAccessKey", "heyhey");
        IcebergGlueMetaStoreProperties properties = (IcebergGlueMetaStoreProperties) MetastoreProperties.create(baseProps);
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(baseProps);
        SupportsNamespaces catalog = (SupportsNamespaces) properties.initializeCatalog("iceberg_glue_test", storagePropertiesList);
        catalog.listNamespaces().forEach(System.out::println);

    }
}
