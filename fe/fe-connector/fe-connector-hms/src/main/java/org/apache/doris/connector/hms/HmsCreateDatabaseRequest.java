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

package org.apache.doris.connector.hms;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Write-side spec describing a database to create in the metastore.
 *
 * <p>The write-side counterpart of the read DTO {@link HmsDatabaseInfo}. It is
 * SPI-clean (only JDK types) and mirrors fe-core's {@code HiveDatabaseMetadata}
 * without depending on any fe-core class. A connector plugin assembles it from
 * a CREATE DATABASE request and hands it to {@link HmsClient#createDatabase}.</p>
 */
public final class HmsCreateDatabaseRequest {

    private final String dbName;
    private final String locationUri;
    private final String comment;
    private final Map<String, String> properties;

    public HmsCreateDatabaseRequest(String dbName, String locationUri,
            String comment, Map<String, String> properties) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.locationUri = locationUri;
        this.comment = comment;
        this.properties = properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(properties);
    }

    public String getDbName() {
        return dbName;
    }

    /** Optional database location URI; {@code null} when the metastore should pick a default. */
    public String getLocationUri() {
        return locationUri;
    }

    /** Database comment; never {@code null} (empty when unset). */
    public String getComment() {
        return comment == null ? "" : comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "HmsCreateDatabaseRequest{" + dbName + "}";
    }
}
