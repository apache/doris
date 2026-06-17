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

import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.KerberosAuthSpec;

import java.util.Map;
import java.util.Optional;

/**
 * Neutral connection facts for a Hive Metastore (HMS) backend. The concrete {@code HiveConf} is
 * assembled by the connector (which has the hive classes); this contract only carries neutral keys.
 */
public interface HmsMetaStoreProperties extends MetaStoreProperties {

    /** The metastore thrift URI ({@code hive.metastore.uris}). */
    String getUri();

    /** Whether the metastore connection is {@code SIMPLE} or {@code KERBEROS} authenticated. */
    AuthType getAuthType();

    /**
     * Neutral {@code hive.*} / {@code hadoop.security.*} / SASL overrides to be layered onto the
     * connector's {@code HiveConf}. Includes the HMS service principal when configured.
     */
    Map<String, String> toHiveConfOverrides();

    /**
     * The client Kerberos login facts (principal/keytab), present only for a Kerberos-secured
     * metastore. The real {@code UGI.doAs} is still performed FE-side via
     * {@code ConnectorContext.executeAuthenticated}; this only carries the facts.
     */
    Optional<KerberosAuthSpec> kerberos();
}
