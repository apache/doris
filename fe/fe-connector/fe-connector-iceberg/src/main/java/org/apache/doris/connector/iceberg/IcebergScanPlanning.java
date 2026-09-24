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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.Table;

/** Checks native table capabilities before snapshot wrappers replace them with BaseTable defaults. */
final class IcebergScanPlanning {
    private IcebergScanPlanning() {}

    static void rejectServerSideScanPlanning(Table table, String tableName) {
        if (table instanceof SupportsDistributedScanPlanning
                && !((SupportsDistributedScanPlanning) table).allowDistributedPlanning()) {
            // RESTTable overrides this to false; plain BaseTable inherits true. Doris cannot consume
            // server tasks/residuals or scan-scoped credentials yet, so reject before local manifest I/O.
            throw new DorisConnectorException("Iceberg server-side scan planning is not supported for table "
                    + tableName + "; configure the REST catalog to use client-side scan planning");
        }
    }
}
