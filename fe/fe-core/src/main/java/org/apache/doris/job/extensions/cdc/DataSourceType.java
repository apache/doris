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

package org.apache.doris.job.extensions.cdc;

import java.util.Arrays;

public enum DataSourceType {

    MYSQL("mysql", "com.mysql.cj.jdbc.Driver", "mysql-connector-java-8.0.25.jar");

    private String type;
    private String driverClass;
    private String driverUrl;

    DataSourceType(String type, String driverClass, String driverUrl) {
        this.type = type;
        this.driverClass = driverClass;
        this.driverUrl = driverUrl;
    }

    public String getType() {
        return type;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    public static DataSourceType getByType(String type) {
        return Arrays.stream(DataSourceType.values())
                .filter(config -> config.getType().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupport datasource " + type));
    }
}
