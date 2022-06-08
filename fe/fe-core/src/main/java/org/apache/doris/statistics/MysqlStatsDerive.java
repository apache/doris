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

package org.apache.doris.statistics;

/**
 * Derive MysqlScanNode statistics.
 */
public class MysqlStatsDerive extends BaseStatsDerive {

    // Current ODBC_SCAN_NODE also uses this derivation method
    @Override
    public StatsDeriveResult deriveStats() {
        return new StatsDeriveResult(deriveRowCount(), deriveColumnToDataSize(), deriveColumnToNdv());
    }

    @Override
    protected long deriveRowCount() {
        // this is just to avoid mysql scan node's rowCount being -1. So that we can calculate the join cost
        // normally.
        // We assume that the data volume of all mysql tables is very small, so set rowCount directly to 1.
        rowCount = rowCount == -1 ? 1 : rowCount;
        return rowCount;
    }
}
