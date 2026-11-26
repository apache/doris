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

package org.apache.doris.iceberg;

import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;

import java.util.Map;

/** Builds {@link IcebergSysTableJniScanner}. BE reaches it as {@code (iceberg, sys-table)}. */
public class IcebergSysTableScannerFactory implements JniScannerFactory {

    /**
     * Not {@code reader}, the name every other connector's scan factory carries, because this one
     * does not read iceberg tables: their data files go through BE's native parquet/orc readers.
     * It serves the metadata tables ({@code $snapshots}, {@code $files}, ...), whose rows only
     * iceberg's own Java library can produce - see the table in BE's jni_plugin_registry.h.
     */
    public static final String NAME = "sys-table";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public JniScanner create(int batchSize, Map<String, String> params) {
        return new IcebergSysTableJniScanner(batchSize, params);
    }
}
