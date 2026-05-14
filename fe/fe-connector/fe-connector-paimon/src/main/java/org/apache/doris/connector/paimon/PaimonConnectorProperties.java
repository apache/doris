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

package org.apache.doris.connector.paimon;

/**
 * Property key constants for Paimon connector configuration.
 */
public final class PaimonConnectorProperties {

    /** Paimon catalog backend type: filesystem, hms, dlf, rest, jdbc. */
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";

    /** Warehouse location for the Paimon catalog. */
    public static final String WAREHOUSE = "warehouse";

    /** Whether to map Paimon BINARY/VARBINARY to Doris VARBINARY instead of STRING. */
    public static final String ENABLE_MAPPING_BINARY_AS_VARBINARY = "enable_mapping_binary_as_varbinary";

    /** Whether to map Paimon TIMESTAMP_WITH_LOCAL_TIME_ZONE to TIMESTAMPTZ. */
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable_mapping_timestamp_tz";

    /** Default catalog type when not specified. */
    public static final String DEFAULT_CATALOG_TYPE = "filesystem";

    private PaimonConnectorProperties() {
    }
}
