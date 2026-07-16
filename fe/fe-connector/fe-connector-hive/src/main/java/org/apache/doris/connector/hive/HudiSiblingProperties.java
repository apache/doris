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

package org.apache.doris.connector.hive;

import java.util.HashMap;
import java.util.Map;

/**
 * Synthesizes the catalog-property map for the embedded Hudi <em>sibling</em> connector that a flipped HMS
 * gateway delegates its hudi-on-HMS tables to. Mirrors {@link IcebergSiblingProperties} so the two sibling
 * paths read identically at the {@code getOrCreate*Sibling} seams.
 *
 * <p>The sibling is built once per gateway catalog (not per table) via
 * {@code ConnectorContext.createSiblingConnector("hudi", synthesize(catalogProps))}, sharing the gateway's
 * context (metastore auth + storage). Unlike the iceberg sibling there is <b>no flavor key to inject</b>: hudi
 * has no {@code iceberg.catalog.type} analogue — {@code HudiConnector.createClient} reads
 * {@code hive.metastore.uris}/{@code uri} plus the raw {@code hadoop.*}/{@code fs.*}/{@code dfs.*}/{@code hive.*}/
 * {@code s3.*} storage + kerberos passthrough straight from this map. So synthesis is a plain verbatim copy of
 * the gateway catalog's whole property map. Carrying the whole map (rather than a hand-picked subset) is the
 * robust choice: it cannot silently drop a connectivity key (the {@code uri} short form, an HDFS-HA
 * {@code dfs.*} set, an S3 endpoint override, a kerberos variant, ...); the connector ignores keys it does not
 * recognize.
 *
 * <p>A defensive copy is returned so the gateway's own (unmodifiable, shared) property map is never aliased into
 * the sibling connector.
 */
final class HudiSiblingProperties {

    private HudiSiblingProperties() {
    }

    /**
     * Returns a NEW property map = the gateway catalog's properties verbatim (a defensive copy). The input is
     * never mutated. No flavor key is injected — a hudi-on-HMS sibling needs none.
     */
    static Map<String, String> synthesize(Map<String, String> gatewayCatalogProperties) {
        return new HashMap<>(gatewayCatalogProperties);
    }
}
