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

import java.util.Map;

public class IcebergRestProperties extends MetastoreProperties {

    @ConnectorProperty(names = {"iceberg.rest.uri"},
            description = "The uri of the iceberg rest catalog service.")
    private String icebergRestUri = "";

    @ConnectorProperty(names = {"iceberg.rest.security.type"},
            required = false,
            supported = false,
            description = "The security type of the iceberg rest catalog service.")
    private String icebergRestSecurityType = "none";

    @ConnectorProperty(names = {"iceberg.rest.prefix"},
            required = false,
            supported = false,
            description = "The prefix of the iceberg rest catalog service.")
    private String icebergRestPrefix = "";

    public IcebergRestProperties(Map<String, String> origProps) {
        super(Type.ICEBERG_REST, origProps);
    }

    @Override
    protected void checkRequiredProperties() {
    }

    public void toIcebergRestCatalogProperties(Map<String, String> catalogProps) {
        // See CatalogUtil.java
        catalogProps.put("type", "rest");
        // See CatalogProperties.java
        catalogProps.put("uri", icebergRestUri);
    }
}
