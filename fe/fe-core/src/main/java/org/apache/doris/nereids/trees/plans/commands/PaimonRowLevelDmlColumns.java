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

package org.apache.doris.nereids.trees.plans.commands;

/**
 * The synthetic row-id column name paimon row-level DELETE binds against.
 *
 * <p>Deliberately a plain FE-side constant rather than a reference into the paimon connector: fe-core must
 * not take a compile-time dependency on a connector module (the SPI iron law). The connector declares the
 * column of the SAME NAME through {@code ConnectorWritePlanProvider.getSyntheticWriteColumns()}; this
 * constant is the FE half of that contract, mirroring how {@code Column.ICEBERG_ROWID_COL} pairs with the
 * iceberg connector's declaration.
 *
 * <p><b>Keep in sync</b> with {@code PaimonWritePlanProvider.DORIS_PAIMON_ROWID_COL}. A mismatch does not
 * fail to compile — it fails at bind time with an unresolved slot, so the two are covered by a test that
 * asserts the literal.
 */
public final class PaimonRowLevelDmlColumns {

    /** Synthetic row-id column carrying a row's physical address (data file + ordinal within it). */
    public static final String ROWID_COL = "__DORIS_PAIMON_ROWID_COL__";

    private PaimonRowLevelDmlColumns() {
    }
}
