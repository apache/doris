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

package org.apache.doris.connector.metastore;

/**
 * Neutral connection facts for a JDBC catalog metastore backend (e.g. paimon jdbc catalog).
 * The driver URL is resolved against the engine's jdbc-drivers directory during parsing.
 */
public interface JdbcMetaStoreProperties extends MetaStoreProperties {

    /** The JDBC connection URI. */
    String getUri();

    /** The JDBC user, or empty when not configured. */
    String getUser();

    /** The JDBC password, or empty when not configured. */
    String getPassword();

    /** The resolved driver jar URL, or empty when the engine-provided driver is used. */
    String getDriverUrl();

    /** The JDBC driver class name, or empty when not configured. */
    String getDriverClass();
}
