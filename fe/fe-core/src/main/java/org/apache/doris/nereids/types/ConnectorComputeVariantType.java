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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Type;

/** Execution-only Variant marker retained while connector scan slots pass through Nereids. */
public final class ConnectorComputeVariantType extends VariantType {

    public static final ConnectorComputeVariantType INSTANCE = new ConnectorComputeVariantType();

    private ConnectorComputeVariantType() {
        super(0);
    }

    @Override
    public DataType conversion() {
        // Conversion is the durable-schema boundary used by CTAS and MTMV; the execution marker
        // must not enter catalog metadata because persistence only registers the ordinary Variant type.
        return super.conversion();
    }

    @Override
    public Type toCatalogDataType() {
        // Recreate the catalog marker so tuple translation cannot silently fall back to the
        // storage-configured Variant encoding while external readers still produce V2.
        return new org.apache.doris.datasource.connector.converter.ConnectorComputeVariantType();
    }
}
