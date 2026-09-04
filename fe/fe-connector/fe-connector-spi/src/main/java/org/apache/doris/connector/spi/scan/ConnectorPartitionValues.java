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

package org.apache.doris.connector.spi.scan;

/**
 * Doris-canonical constants for partition NAMES.
 *
 * <p>{@link #NULL_PARTITION_NAME} is how "this partition column's value is a genuine SQL NULL" is spelled
 * inside a partition NAME. A connector whose source has its own spelling (paimon's
 * {@code partition.default-name}, for instance) normalizes to this one when it renders a partition name.</p>
 *
 * <p><b>The literal is frozen byte for byte.</b> It is the historical hive value, and a partition name is a
 * persisted, user-visible identity: it lands in view and materialized-view definitions, in the
 * {@code partition_values()} table-function output, and in the {@code columns_from_path} bytes BE parses.
 * Only the Java symbol may be renamed; changing the string breaks already-persisted objects.</p>
 *
 * <p><b>This constant does not replace the structured null flag</b> on {@code ConnectorPartitionInfo}, and the
 * flag does not replace this constant — see {@code PluginDrivenMvccExternalTable#toListPartitionItem}. The flag
 * exists so FE can build a TYPED {@code NullLiteral} (parsing this string as an INT or DATE partition value
 * would throw and silently drop the partition, making a partitioned table look unpartitioned); the name exists
 * for partition identity and for BE's path parsing. Whether a value IS null must be declared by the connector
 * through the flag: two connectors render this identical string with different NULL semantics, so fe-core
 * never decides nullness by comparing against it.</p>
 *
 * <p>There is deliberately no shared "normalize a partition value" helper here. The rule for turning a value
 * into a null flag is per-connector — hudi's directory-name rule also treats a literal {@code \N} as NULL,
 * which would corrupt real data for a connector whose partition values are typed — so each connector derives
 * its own flags (see {@code HudiScanRange}, {@code HiveScanRange}, {@code PaimonScanRange},
 * {@code IcebergScanRange}).</p>
 */
public final class ConnectorPartitionValues {

    public static final String NULL_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__";

    private ConnectorPartitionValues() {
    }
}
