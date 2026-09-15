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

package org.apache.doris.job.common;

/**
 * The source databases a streaming (CDC) job can read. Every one of them is reached, for metadata
 * discovery on the FE, through the connector plugin named by {@link #connectorType()}; the BE-side CDC
 * client is chosen separately by the streaming framework.
 */
public enum DataSourceType {
    MYSQL,
    POSTGRES,
    OCEANBASE;

    /** The connector plugin type ({@code ConnectorProvider.getType()}) that serves this source's metadata. */
    public String connectorType() {
        return "jdbc";
    }
}
