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

public class AliyunDLFProperties extends MetastoreProperties {

    @ConnectorProperty(name = "dlf.endpoint",
            description = "The endpoint of the Aliyun DLF.")
    private String dlfEndpoint = "";

    @ConnectorProperty(name = "dlf.region",
            description = "The region of the Aliyun DLF.")
    private String dlfRegion = "";

    @ConnectorProperty(name = "dlf.uid",
            description = "The uid of the Aliyun DLF.")
    private String dlfUid = "";

    @ConnectorProperty(name = "dlf.access_key",
            description = "The access key of the Aliyun DLF.")
    private String dlfAccessKey = "";

    @ConnectorProperty(name = "dlf.secret_key",
            description = "The secret key of the Aliyun DLF.")
    private String dlfSecretKey = "";

    @ConnectorProperty(name = "dlf.catalog_id",
            description = "The catalog id of the Aliyun DLF.")
    private String dlfCatalogId = "";
}
